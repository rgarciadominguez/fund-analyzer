"""revision.py — Publicación en dos tiempos de un análisis (Rafa 30-sep-2026).

Rafa: "en caso de error grave, intenta solucionarlo dentro del análisis y, si no ha podido, lo deja sin
publicar y se muestra claramente en el portal explicando el porqué; las dudas se indican en una pestaña
dentro del análisis para validarlas conmigo antes de publicar de forma definitiva".

Estados (se envían al portal, POST admin/fondos/revision; la pantalla de análisis los muestra):
  · no_publicado          — errores graves que el bat no pudo corregir. Sigue visible la versión anterior.
  · pendiente_validacion  — análisis con dudas: publicado SOLO como borrador (/fund-{ISIN}-borrador del
                            Worker) con la pestaña "Novedades y revisión" interactiva. Sigue visible la
                            versión anterior hasta que Rafa pulsa "Validar y publicar".
  · corrigiendo           — Rafa marcó alguna duda como incorrecta: se relanza el análisis de ESE fondo con
                            sus comentarios (solo las secciones afectadas) y vuelve a pasar por aquí.
  · publicado             — versión definitiva (Worker + Supabase + portal).
Los veredictos y comentarios de Rafa van además al agente de aprendizaje (tools.aprendizaje).

El borrador habla con el portal por postMessage (el dashboard va en un iframe del Worker dentro del portal):
  borrador → portal: {hf:'revision', isin, accion:'estado'|'veredicto'|'publicar', duda, veredicto, comentario}
  portal → borrador: {hf:'revision-estado', veredictos:{id:{v,c}}} y {hf:'revision-ok', texto}

CLI:  borrador ISIN · no-publicado ISIN "motivo" · publicar ISIN · publicado ISIN · estado ISIN
"""
from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
WORKER = os.environ.get("FUND_WORKER_BASE", "https://fund-analyzer.rafagdominguez96.workers.dev")
BRANCH = "v2-cowork"


def _log(m: str) -> None:
    print(f"[REVISION] {m}", flush=True)


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def _portal():
    c = json.load(open(os.path.expanduser("~/.horizonte-portal.json"), encoding="utf-8"))
    base = c["base_url"].rstrip("/")
    auth = "Basic " + base64.b64encode(f"{c['usuario']}:{c['app_password']}".encode()).decode()
    return base, auth


def _api(method: str, path: str, body: dict | None = None) -> dict:
    import urllib.request
    base, auth = _portal()
    req = urllib.request.Request(base + "/wp-json/horizonte/v1/" + path, method=method,
                                 data=json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None,
                                 headers={"Authorization": auth, "Content-Type": "application/json; charset=utf-8"})
    return json.loads(urllib.request.urlopen(req, timeout=40).read() or b"{}")


def _git(*args, timeout=180):
    p = subprocess.run(["git", *args], cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout, env=dict(os.environ, GIT_TERMINAL_PROMPT="0"))
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def estado_portal(isin: str, estado: str, motivo: str = "", dudas: list | None = None, borrador_url: str = "") -> None:
    try:
        r = _api("POST", "admin/fondos/revision", {
            "isin": isin, "estado": estado, "motivo": motivo[:2000],
            "dudas": [{"id": d.get("id"), "titulo": d.get("titulo"), "detalle": d.get("detalle")} for d in (dudas or [])],
            "borrador_url": borrador_url, "fecha": datetime.now().isoformat(timespec="seconds")})
        if not r.get("ok"):
            _log(f"[WARN] el portal no aceptó el estado: {str(r)[:160]}")
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] estado al portal no enviado: {str(e)[:120]}")
    fd = ROOT / "data" / "funds" / isin
    if fd.exists():
        (fd / "revision.json").write_text(json.dumps({"isin": isin, "estado": estado, "motivo": motivo,
                                                      "n_dudas": len(dudas or []), "borrador_url": borrador_url,
                                                      "fecha": datetime.now().isoformat(timespec="seconds")},
                                                     ensure_ascii=False, indent=2), encoding="utf-8")


def _restaurar_publicado(isin: str) -> None:
    """Deja dashboard/fund-{ISIN}.html como la versión PUBLICADA (la del último commit), para que el borrador
    o el análisis roto no se cuelen en la próxima publicación de otro fondo."""
    rel = f"dashboard/fund-{isin}.html"
    rc, out, _ = _git("show", f"HEAD:{rel}")
    if rc == 0 and out:
        _git("checkout", "HEAD", "--", rel)


# ── borrador ────────────────────────────────────────────────────────────────────────────────────
_SCRIPT = r"""
<div id="hf-borrador-bar" style="position:sticky;top:0;z-index:60;background:#fdf3e1;border-bottom:1px solid #e8cf9e;padding:10px 16px;display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between;font:14px/1.4 system-ui,sans-serif;color:#3a2a08">
  <div><b>Borrador pendiente de tu validación.</b> <span style="color:#6b5424">Lo que ven los demás sigue siendo la versión anterior.</span></div>
  <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap"><span id="hf-cnt" style="font-size:13px;color:#6b5424"></span>
  <button id="hf-pub" style="background:#2e7d4f;color:#fff;border:0;border-radius:6px;padding:7px 14px;font:600 13px system-ui,sans-serif;cursor:pointer">Validar y publicar</button></div>
  <div id="hf-msg" style="width:100%;font-size:13px;display:none"></div>
</div>
<script>(function(){
  var ISIN=__ISIN__, PORTAL=__PORTAL__, V={};
  var enPortal=(window.parent&&window.parent!==window);
  var cards=[].slice.call(document.querySelectorAll('.hf-duda'));
  var bar=document.getElementById('hf-borrador-bar'); document.body.insertBefore(bar, document.body.firstChild);
  function send(accion,extra){ if(!enPortal) return; var m={hf:'revision',isin:ISIN,accion:accion}; for(var k in (extra||{})) m[k]=extra[k];
    try{ window.parent.postMessage(m, PORTAL); }catch(e){} }
  function msg(t){ var e=document.getElementById('hf-msg'); e.style.display='block'; e.textContent=t; }
  function pinta(){ var n=0,mal=0; cards.forEach(function(c){ var id=c.getAttribute('data-duda'), v=V[id]||{};
      c.querySelectorAll('button[data-v]').forEach(function(b){ var on=b.getAttribute('data-v')===v.v;
        b.style.background=on?(v.v==='ok'?'#e8f4ec':'#fbeaea'):'#fff'; b.style.borderColor=on?(v.v==='ok'?'#2e7d4f':'#b3372c'):'#d8dde6'; });
      var ta=c.querySelector('textarea'); if(ta && document.activeElement!==ta && v.c!=null) ta.value=v.c;
      if(v.v) n++; if(v.v==='mal') mal++; });
    document.getElementById('hf-cnt').textContent=n+' de '+cards.length+' dudas revisadas';
    document.getElementById('hf-pub').textContent= mal ? ('Corregir '+mal+' y volver a revisar') : 'Validar y publicar'; }
  cards.forEach(function(c){ var id=c.getAttribute('data-duda');
    var w=document.createElement('div'); w.style.cssText='margin-top:8px;display:grid;gap:6px';
    w.innerHTML='<div style="display:flex;gap:8px;flex-wrap:wrap"><button data-v="ok" style="border:1px solid #d8dde6;border-radius:6px;padding:5px 12px;cursor:pointer;font:600 12.5px system-ui">Correcto</button>'
      +'<button data-v="mal" style="border:1px solid #d8dde6;border-radius:6px;padding:5px 12px;cursor:pointer;font:600 12.5px system-ui">Incorrecto</button></div>'
      +'<textarea rows="2" placeholder="Comentario (opcional): qué es lo correcto o qué debe tener en cuenta el sistema" style="width:100%;box-sizing:border-box;border:1px solid #d8dde6;border-radius:6px;padding:6px;font:13px system-ui"></textarea>';
    c.appendChild(w);
    w.querySelectorAll('button[data-v]').forEach(function(b){ b.onclick=function(){ V[id]=V[id]||{}; V[id].v=b.getAttribute('data-v'); pinta();
      send('veredicto',{duda:id,veredicto:V[id].v,comentario:(w.querySelector('textarea').value||'')}); }; });
    w.querySelector('textarea').onblur=function(){ V[id]=V[id]||{}; V[id].c=this.value; send('veredicto',{duda:id,veredicto:V[id].v||'',comentario:this.value}); };
  });
  document.getElementById('hf-pub').onclick=function(){
    if(!enPortal){ msg('Abre este borrador desde el portal (pantalla de análisis) para validarlo.'); return; }
    send('publicar',{}); this.disabled=true; msg('Enviado. El servidor lo procesa en menos de un minuto; el estado se ve en la pantalla de análisis del portal.'); };
  window.addEventListener('message',function(e){ if(e.origin!==PORTAL||!e.data) return;
    if(e.data.hf==='revision-estado'){ V=e.data.veredictos||{}; pinta(); }
    if(e.data.hf==='revision-ok' && e.data.texto){ msg(e.data.texto); } });
  try{ var t=document.querySelector("button[onclick^='goTab(10']"); if(t) t.click(); }catch(e){}
  if(!enPortal) msg('Estás viendo el borrador fuera del portal: para validar las dudas, ábrelo desde la pantalla de análisis.');
  pinta(); send('estado',{});
})();</script>
"""


def _html_borrador(isin: str, html: str) -> str:
    base, _ = _portal()
    o = urlparse(base)
    script = (_SCRIPT.replace("__ISIN__", json.dumps(isin))
              .replace("__PORTAL__", json.dumps(f"{o.scheme}://{o.netloc}")))
    html = re.sub(r"<!-- hf-build:[0-9a-f]{12} -->\n?", "", html)
    return html.replace("</body>", script + "\n</body>", 1) if "</body>" in html else html + script


def _version_y_meta(isin: str, tipo: str | None, regenerar: bool = True) -> None:
    """Registra la versión publicada (tools.versiones), regenera el dashboard con su cabecera y envía al
    portal la fecha/tipo/historial (push_meta usa Supabase si responde y, si no, el análisis local)."""
    try:
        from tools.versiones import registrar
        v = registrar(isin, tipo)
        _log(f"{isin}: versión registrada → {v.get('etiqueta')} {v.get('fecha')}")
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] versión no registrada: {str(e)[:100]}")
    if regenerar:
        subprocess.run([sys.executable, str(DASH / "generate_dashboard.py"), isin], cwd=str(ROOT),
                       capture_output=True, text=True, timeout=300)
    try:
        from tools.portal_analyze_worker import push_meta
        push_meta(isin)
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] meta al portal: {str(e)[:100]}")


def publicar_borrador(isin: str, wait: int = 420, tipo: str | None = None) -> bool:
    isin = isin.upper()
    if tipo:
        (ROOT / "data" / "funds" / isin / "_version_tipo.txt").write_text(tipo, encoding="utf-8")
    gate = _load(ROOT / "data" / "funds" / isin / "quality_gate.json", {}) or {}
    live = DASH / f"fund-{isin}.html"
    if not live.exists():
        _log("no hay dashboard generado")
        return False
    borr = DASH / f"fund-{isin}-borrador.html"
    borr.write_text(_html_borrador(isin, live.read_text(encoding="utf-8")), encoding="utf-8")
    from tools.publish_dashboard import stamp_html, stamp_of
    sello = stamp_html(borr)
    _restaurar_publicado(isin)                       # lo público sigue siendo la versión anterior
    rel = f"dashboard/fund-{isin}-borrador.html"
    _git("add", "--", rel)
    rc, out, _ = _git("diff", "--cached", "--name-only")
    if out.strip():
        _git("commit", "-q", "-m", f"auto: borrador {isin} pendiente de validar (build {sello})")
    from tools.git_autopush import push
    if push(BRANCH) != 0:
        _log("[WARN] push del borrador no realizado; el guardián lo reintenta")
    url = f"{WORKER}/fund-{isin}-borrador"
    import httpx
    t0 = time.time()
    while True:
        try:
            r = httpx.get(url, params={"_": int(time.time())}, headers={"Cache-Control": "no-cache"}, timeout=60)
            if r.status_code == 200 and stamp_of(r.text) == sello:
                break
        except Exception:
            pass
        if time.time() - t0 > wait:
            _log(f"[WARN] el Worker aún no sirve el borrador ({sello})")
            break
        time.sleep(30)
    estado_portal(isin, "pendiente_validacion", "", gate.get("dudas") or [], url)
    _log(f"{isin}: borrador publicado en {url} con {len(gate.get('dudas') or [])} dudas")
    return True


def no_publicado(isin: str, motivo: str = "") -> None:
    isin = isin.upper()
    gate = _load(ROOT / "data" / "funds" / isin / "quality_gate.json", {}) or {}
    motivo = motivo or "; ".join(gate.get("graves") or []) or "error grave de calidad"
    _restaurar_publicado(isin)
    estado_portal(isin, "no_publicado", motivo)
    _log(f"{isin}: NO publicado — {motivo}")


def _veredictos(isin: str) -> dict:
    try:
        r = _api("GET", f"admin/fondos/revision?isin={isin}")
        filas = r.get("filas") or []
        return (filas[0].get("veredictos") or {}) if filas else {}
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude leer los veredictos del portal: {str(e)[:100]}")
        return {}


def publicar(isin: str) -> bool:
    """Rafa pulsó 'Validar y publicar'. Si marcó dudas como incorrectas → corrige ESE fondo y vuelve a revisión.
    Si no → publica la versión definitiva. En ambos casos, lo que dijo va al agente de aprendizaje."""
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    gate = _load(fd / "quality_gate.json", {}) or {}
    dudas = {d.get("id"): d for d in gate.get("dudas") or []}
    ver = _veredictos(isin)
    nombre = (_load(fd / "output.json", {}) or {}).get("nombre") or isin
    items = [{"duda": dudas.get(k, {}).get("titulo", k), "detalle": dudas.get(k, {}).get("detalle", ""),
              "veredicto": v.get("v"), "comentario": v.get("c") or ""} for k, v in ver.items() if v.get("v") or v.get("c")]
    try:
        from tools.aprendizaje import add as _aprender
        if items:
            _aprender(isin, "validacion", {"nombre": nombre, "items": items})
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] aprendizaje: {str(e)[:100]}")
    malas = [it for it in items if it["veredicto"] == "mal"]
    if malas:
        from tools.feedback_store import append_feedback
        texto = "Correcciones de Rafa al validar el análisis:\n" + "\n".join(
            f"- {it['duda']}: {it['comentario'] or 'lo que dice el análisis es incorrecto'}" for it in malas)
        append_feedback(isin, texto, structured_items=[{"target_section": None, "action": "revisar",
                                                        "value": it["comentario"], "rationale": it["duda"]} for it in malas],
                        fund_name=nombre)
        try:
            import urllib.request
            req = urllib.request.Request("http://127.0.0.1:5000/api/analyze-batch", method="POST",
                                         data=json.dumps({"isins": [isin], "cold_start": False, "apply_feedback": True}).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=30).read()
        except Exception as e:  # noqa: BLE001
            _log(f"[WARN] no pude encolar la corrección: {str(e)[:100]}")
        estado_portal(isin, "corrigiendo", f"{len(malas)} duda(s) marcadas como incorrectas: se corrige este análisis y vuelve a revisión")
        return True
    # Publicación definitiva: versión (tipo guardado al crear el borrador) + dashboard con su cabecera
    _tp = fd / "_version_tipo.txt"
    _version_y_meta(isin, _tp.read_text(encoding="utf-8").strip() if _tp.exists() else None, regenerar=True)
    _tp.unlink(missing_ok=True)
    rc = subprocess.call([sys.executable, "-m", "tools.publish_dashboard", "--isin", isin, "--wait", "420"], cwd=str(ROOT))
    subprocess.call([sys.executable, "-m", "tools.sync_to_supabase", isin], cwd=str(ROOT))
    b = DASH / f"fund-{isin}-borrador.html"
    if b.exists():
        _git("rm", "-q", "--", f"dashboard/fund-{isin}-borrador.html")
        _git("commit", "-q", "-m", f"auto: {isin} validado y publicado (borrador retirado)")
        from tools.git_autopush import push
        push(BRANCH)
    estado_portal(isin, "publicado", "validado por Rafa" if items else "")
    _log(f"{isin}: publicado (rc publish={rc})")
    return rc == 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(2)
    acc, isin = sys.argv[1], sys.argv[2].upper()
    if acc == "borrador":
        sys.exit(0 if publicar_borrador(isin, tipo=sys.argv[3] if len(sys.argv) > 3 else None) else 1)
    if acc == "no-publicado":
        no_publicado(isin, sys.argv[3] if len(sys.argv) > 3 else ""); sys.exit(0)
    if acc == "publicar":
        sys.exit(0 if publicar(isin) else 1)
    if acc == "publicado":
        # antes de publish_dashboard: versión + dashboard regenerado con su cabecera + meta al portal
        _version_y_meta(isin, sys.argv[3] if len(sys.argv) > 3 else None, regenerar=True)
        estado_portal(isin, "publicado"); sys.exit(0)
    if acc == "estado":
        print(json.dumps(_load(ROOT / "data" / "funds" / isin / "revision.json", {}), ensure_ascii=False, indent=2)); sys.exit(0)
    print(__doc__); sys.exit(2)
