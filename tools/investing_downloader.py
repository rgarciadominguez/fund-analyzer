"""investing_downloader.py - Descarga histórica de ÍNDICES desde Investing.com.

Resuelve el problema de los índices propietarios (MSCI/S&P Gross, Nasdaq TR, oro,
bonos) que Morningstar NO cubre y que hoy están congelados en data/benchmarks/ como
CSV exportados a mano. Usa el endpoint financialdata de Investing vía cloudscraper
(resuelve el challenge de Cloudflare sin token TVC manual ni navegador).

Estrategia probada (2026-06-16):
  - Endpoint: api.investing.com/api/financialdata/historical/{pairId}
              header 'domain-id: www'. Tope 5000 filas/petición -> se trocea por
              ventanas de fecha hacia atrás y se fusiona.
  - Resolución nombre -> pairId: api.investing.com/api/search/v2/search
  - Conversión a EUR: índices Total-Return USD (SPXTR, XNDX...) se dividen por la
    serie EUR/USD (pair_id=1) día a día, reproduciendo el método de los CSV "euros".

Registro: data/benchmarks/index_registry.json (fuente de verdad de pair_id/csv/fx).

CLI:
  python -m tools.investing_downloader --pilot            # 3 índices -> *.auto.csv (NO toca originales)
  python -m tools.investing_downloader --key "MSCI World EUR" --since 2025-12-01
  python -m tools.investing_downloader --resolve "Russell 2000"   # buscar pair_id
  python -m tools.investing_downloader --add "Russell 2000"       # resolver + añadir al registry (interactivo no: usa --pair-id)

Salida: CSV en formato Investing español (mismas columnas que los originales) para
que tools.benchmarks_to_funddash.py los suba a Supabase como hoy.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "data" / "benchmarks" / "Benchmark"
REGISTRY_PATH = ROOT / "data" / "benchmarks" / "index_registry.json"

API_HIST = "https://api.investing.com/api/financialdata/historical/{pid}"
API_SEARCH = "https://api.investing.com/api/search/v2/search?q={q}"
HDR = {"domain-id": "www", "Accept": "application/json", "Referer": "https://www.investing.com/"}
MAX_ROWS = 5000           # tope del endpoint por petición
CHUNK_DAYS = 365 * 12     # ~12 años por ventana (<5000 días hábiles, margen de sobra)


# ───────────────────────── transporte (cloudscraper + fallback) ─────────────────────────
_SCRAPER = None


def _scraper():
    global _SCRAPER
    if _SCRAPER is None:
        import cloudscraper
        _SCRAPER = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    return _SCRAPER


def _get_json(url: str, tries: int = 3) -> dict:
    last = None
    for i in range(tries):
        try:
            r = _scraper().get(url, headers=HDR, timeout=45)
            if r.status_code == 200 and r.text and r.text.lstrip().startswith(("{", "[")):
                return json.loads(r.text)
            last = f"HTTP {r.status_code}: {r.text[:80]}"
        except Exception as e:  # noqa: BLE001
            last = repr(e)[:120]
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"fallo tras {tries} intentos: {last}")


# ───────────────────────── descarga histórica ─────────────────────────
def _parse_data(rows: list) -> dict[date, dict]:
    """{date: {close, open, max, min, vol, chg}} a partir del JSON de Investing."""
    out: dict[date, dict] = {}
    for x in rows or []:
        try:
            ts = int(x["rowDateRaw"])
            d = datetime.fromtimestamp(ts, timezone.utc).date()
            close = float(x["last_closeRaw"])
        except (KeyError, ValueError, TypeError):
            continue
        if close <= 0:
            continue

        def _f(k):
            try:
                return float(x.get(k))
            except (TypeError, ValueError):
                return None

        out[d] = {
            "close": close, "open": _f("last_openRaw"), "max": _f("last_maxRaw"),
            "min": _f("last_minRaw"), "vol": _f("volumeRaw"), "chg": _f("change_precentRaw"),
        }
    return out


def fetch_history(pair_id: int, since: date | None = None) -> dict[date, dict]:
    """Histórico completo (o desde `since`) troceado por ventanas y fusionado."""
    end = date.today()
    floor = since or date(1985, 1, 1)
    merged: dict[date, dict] = {}
    guard = 0
    while end > floor and guard < 60:
        guard += 1
        start = max(floor, end - timedelta(days=CHUNK_DAYS))
        url = API_HIST.format(pid=pair_id) + f"?start-date={start:%Y-%m-%d}&end-date={end:%Y-%m-%d}&time-frame=Daily&add-missing-rows=false"
        chunk = _parse_data(_get_json(url).get("data", []))
        if not chunk:
            break
        merged.update(chunk)
        earliest = min(chunk)
        if since:  # incremental: una ventana basta si ya cubre 'since'
            if earliest <= floor:
                break
        if earliest <= start + timedelta(days=2):
            # el instrumento empieza aquí o hay más historia: seguimos hacia atrás
            end = earliest - timedelta(days=1)
            if len(chunk) < MAX_ROWS - 5:  # ventana no saturada -> ya está el inicio
                break
        else:
            break
        time.sleep(0.4)
    return merged


# ───────────────────────── conversión a EUR (índices USD-TR) ─────────────────────────
def to_eur(idx: dict[date, dict], fx: dict[date, dict]) -> dict[date, dict]:
    """EUR = USD / (USD por EUR). Alinea por fecha; rellena FX con el último valor previo."""
    fx_dates = sorted(fx)
    out: dict[date, dict] = {}
    j, last_fx = 0, None
    for d in sorted(idx):
        while j < len(fx_dates) and fx_dates[j] <= d:
            last_fx = fx[fx_dates[j]]["close"]
            j += 1
        if not last_fx:
            continue
        row = idx[d]
        conv = {k: (row[k] / last_fx if (k in ("close", "open", "max", "min") and row[k]) else row[k]) for k in row}
        out[d] = conv
    # recomputar %var sobre el close EUR
    ds = sorted(out)
    for i, d in enumerate(ds):
        if i == 0:
            out[d]["chg"] = None
        else:
            prev = out[ds[i - 1]]["close"]
            out[d]["chg"] = (out[d]["close"] / prev - 1) * 100 if prev else None
    return out


# ───────────────────────── formato CSV Investing (español) ─────────────────────────
def _es_num(v, dec=2) -> str:
    if v is None:
        return ""
    s = f"{v:,.{dec}f}"               # 17,963.62
    return s.replace(",", "§").replace(".", ",").replace("§", ".")  # -> 17.963,62


def splice_preserve(new_rows: dict[date, dict], csv_path: Path) -> dict[date, dict]:
    """Conserva el histórico ANTIGUO del CSV que Investing no alcanza (p.ej. oro 2000-2014)
    y empalma la serie nueva rescalada para que el nivel sea continuo (sin salto falso en
    base-100). Si el CSV no tiene datos previos al inicio de la serie nueva -> replace puro."""
    from tools.benchmark_data import load_series
    old = dict(load_series(str(csv_path)))           # {date: close}
    if not old or not new_rows:
        return new_rows
    boundary = min(new_rows)
    old_before = {d: c for d, c in old.items() if d < boundary and c}
    if not old_before:
        return new_rows                               # Investing cubre todo -> replace
    last_old = max(old_before)
    base_new = new_rows[boundary]["close"]
    factor = (old_before[last_old] / base_new) if base_new else 1.0
    merged: dict[date, dict] = {}
    for d, c in old_before.items():                   # antiguo verbatim (solo se usa el cierre aguas abajo)
        merged[d] = {"close": c, "open": c, "max": c, "min": c, "vol": None, "chg": None}
    for d, r in new_rows.items():                     # nuevo rescalado a la escala antigua
        merged[d] = {k: (r[k] * factor if k in ("close", "open", "max", "min") and r.get(k) else r[k]) for k in r}
    ds = sorted(merged)
    for i, d in enumerate(ds):
        prev = merged[ds[i - 1]]["close"] if i else None
        merged[d]["chg"] = ((merged[d]["close"] / prev - 1) * 100) if prev else None
    return merged


def write_csv(path: Path, rows: dict[date, dict]):
    """Formato idéntico a los exports de Investing: descendente, comillas, ; cabecera."""
    ds = sorted(rows, reverse=True)
    lines = ['"Fecha","Último","Apertura","Máximo","Mínimo","Vol.","% var."']
    for d in ds:
        r = rows[d]
        chg = f'{r["chg"]:+.2f}%'.replace(".", ",") if r.get("chg") is not None else ""
        cells = [f"{d:%d.%m.%Y}", _es_num(r["close"]), _es_num(r.get("open")),
                 _es_num(r.get("max")), _es_num(r.get("min")), "", chg]
        lines.append(",".join(f'"{c}"' for c in cells))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("﻿" + "\n".join(lines) + "\n", encoding="utf-8")


# ───────────────────────── registry + alto nivel ─────────────────────────
def load_registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def build_index(entry: dict, since: date | None = None) -> dict[date, dict]:
    idx = fetch_history(int(entry["pair_id"]), since=since)
    if not idx:
        return {}
    if entry.get("fx_pair_id"):
        fx = fetch_history(int(entry["fx_pair_id"]), since=since)
        idx = to_eur(idx, fx)
    return idx


# ───────────────────────── resolución nombre -> pair_id ─────────────────────────
def resolve(query: str) -> list[dict]:
    import urllib.parse
    j = _get_json(API_SEARCH.format(q=urllib.parse.quote(query)))
    out = []
    for it in j.get("quotes", []):
        out.append({"pair_id": it.get("pairId") or it.get("id"),
                    "symbol": it.get("symbol"), "exchange": it.get("exchange")})
    return out


def add_index(key: str, pair_id: int, fx: int | None = None, symbol: str | None = None,
              csv_name: str | None = None, subdir: str = "Renta Variable",
              upload: bool = False) -> dict:
    """Añade un índice al registry, descarga su histórico y (opc.) lo sube a Supabase.
    Reutilizable desde CLI y desde el server. Devuelve dict con el resultado."""
    csv_name = csv_name or key
    csv_rel = f"{subdir}/{csv_name}.csv"
    data = load_registry()
    if key in data["indices"]:
        return {"ok": False, "error": f"la clave '{key}' ya existe en el registry"}
    entry = {"pair_id": int(pair_id), "symbol": symbol, "csv": csv_rel,
             "fx_pair_id": (int(fx) if fx else None),
             "divisa": "Euro" if fx else "USD", "_added": "manual"}
    rows = build_index(entry, since=None)
    if not rows:
        return {"ok": False, "error": "sin datos — revisa el pair_id"}
    out_path = BENCH / csv_rel
    write_csv(out_path, rows)
    data["indices"][key] = entry
    REGISTRY_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    ds = sorted(rows)
    result = {"ok": True, "key": key, "pair_id": int(pair_id), "fx": (int(fx) if fx else None),
              "csv": csv_rel, "rows": len(rows), "from": ds[0].isoformat(), "to": ds[-1].isoformat(),
              "last": rows[ds[-1]]["close"]}
    if upload:
        try:
            from tools.benchmarks_to_funddash import derive_meta, build_rows, _post
            meta = derive_meta(out_path, False)
            sb_rows = build_rows(out_path)
            _post({"isin": meta["isin"], "meta": meta, "rows": sb_rows})
            result["isin"] = meta["isin"]
            result["uploaded"] = True
        except Exception as e:  # noqa: BLE001
            result["uploaded"] = False
            result["upload_error"] = str(e)[:120]
    return result


# ───────────────────────── auto-mapeo CSV -> pair_id por huella de retornos ─────────────────────────
import re as _re

_FX_CACHE: dict[int, dict] = {}


def _clean_query(stem: str) -> str:
    s = _re.sub(r"^\d{4}\s*-\s*", "", stem)
    s = _re.sub(r"\.auto$", "", s)
    s = _re.sub(r"Datos hist[oó]ricos del TR\s*", "", s, flags=_re.I)
    s = _re.sub(r"_convertido_EUR_desde_\d+", "", s, flags=_re.I)
    s = _re.sub(r"\s*-\s*(euros|dolares)$", "", s, flags=_re.I)
    s = _re.sub(r"\s*\(\d+\)", "", s)
    s = _re.sub(r"\s*\((Net|Price version|Gross)\)", "", s, flags=_re.I)
    return _re.sub(r"\s+", " ", s).strip(" -")


def _recent_returns(series: list[tuple[date, float]], n: int = 12) -> dict[date, float]:
    """Retornos diarios de los últimos n puntos: {fecha: ret%}."""
    s = sorted(series)[-n - 1:]
    out = {}
    for i in range(1, len(s)):
        if s[i - 1][1]:
            out[s[i][0]] = (s[i][1] / s[i - 1][1] - 1) * 100
    return out


def _fx_recent(pid: int, start: date, end: date) -> dict[date, float]:
    if pid not in _FX_CACHE:
        url = API_HIST.format(pid=pid) + f"?start-date={start:%Y-%m-%d}&end-date={end:%Y-%m-%d}&time-frame=Daily&add-missing-rows=false"
        _FX_CACHE[pid] = {d: r["close"] for d, r in _parse_data(_get_json(url).get("data", [])).items()}
    return _FX_CACHE[pid]


def _score_candidate(pid: int, ref_rets: dict[date, float], window: tuple[date, date], fx: int | None) -> float | None:
    """Error medio (pp) de los retornos del candidato vs ref_rets, opcional ÷FX. None si no solapa."""
    url = API_HIST.format(pid=pid) + f"?start-date={window[0]:%Y-%m-%d}&end-date={window[1]:%Y-%m-%d}&time-frame=Daily&add-missing-rows=false"
    try:
        raw = _parse_data(_get_json(url).get("data", []))
    except Exception:
        return None
    if not raw:
        return None
    closes = {d: r["close"] for d, r in raw.items()}
    if fx:
        fxs = _fx_recent(fx, window[0] - timedelta(days=5), window[1])
        fxd = sorted(fxs)
        conv = {}
        for d in sorted(closes):
            prev = [x for x in fxd if x <= d]
            if prev:
                conv[d] = closes[d] / fxs[prev[-1]]
        closes = conv
    ds = sorted(closes)
    cand = {ds[i]: (closes[ds[i]] / closes[ds[i - 1]] - 1) * 100 for i in range(1, len(ds)) if closes[ds[i - 1]]}
    common = set(cand) & set(ref_rets)
    if len(common) < 4:
        return None
    return sum(abs(cand[d] - ref_rets[d]) for d in common) / len(common)


def auto_map(csv_paths: list[Path], tol: float = 0.05) -> list[dict]:
    out = []
    for p in csv_paths:
        serie = load_series_local(p)
        if len(serie) < 20:
            out.append({"csv": p, "status": "sin_datos"})
            continue
        ref = _recent_returns(serie)
        if not ref:
            out.append({"csv": p, "status": "sin_retornos"})
            continue
        window = (min(ref) - timedelta(days=3), max(ref))
        q = _clean_query(p.stem)
        try:
            cands = resolve(q)
        except Exception as e:  # noqa: BLE001
            out.append({"csv": p, "status": f"search_err:{repr(e)[:40]}"})
            continue
        best = None
        for c in cands[:10]:
            pid = c["pair_id"]
            if not pid:
                continue
            for fx in (None, 1):
                err = _score_candidate(pid, ref, window, fx)
                if err is not None and (best is None or err < best["err"]):
                    best = {"pair_id": pid, "symbol": c["symbol"], "exchange": c["exchange"], "fx": fx, "err": err}
            time.sleep(0.3)
        if best and best["err"] <= tol:
            out.append({"csv": p, "status": "OK", **best})
        elif best:
            out.append({"csv": p, "status": "DUDOSO", **best})
        else:
            out.append({"csv": p, "status": "no_match", "query": q})
        print(f"  [{out[-1]['status']:9}] {p.name[:46]:46} "
              + (f"pid={best['pair_id']} {best['symbol']} fx={best['fx']} err={best['err']:.3f}pp" if best else f"q='{q}'"))
    return out


def load_series_local(path: Path):
    from tools.benchmark_data import load_series
    return load_series(str(path))


# ───────────────────────── CLI ─────────────────────────
def _cmp_overlap(auto: dict[date, dict], original_csv: Path) -> str:
    """Compara fidelidad de retornos en el solape contra el CSV original."""
    from tools.benchmark_data import load_series
    orig = dict(load_series(str(original_csv)))
    common = sorted(set(auto) & set(orig))
    if len(common) < 5:
        return f"solape={len(common)} (insuficiente para comparar)"
    # retornos diarios
    errs = []
    for i in range(1, len(common)):
        a0, a1 = auto[common[i - 1]]["close"], auto[common[i]]["close"]
        o0, o1 = orig[common[i - 1]], orig[common[i]]
        if a0 and o0:
            errs.append(abs((a1 / a0 - 1) - (o1 / o0 - 1)) * 100)
    mae = sum(errs) / len(errs) if errs else 0
    return f"solape={len(common)}d  error medio retorno diario={mae:.3f}pp  rango={common[0]}→{common[-1]}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pilot", action="store_true", help="descarga TODO el registry a *.auto.csv (no toca originales; valida fidelidad)")
    ap.add_argument("--all", action="store_true", help="descarga TODO el registry y SOBREESCRIBE los CSV originales (replace)")
    ap.add_argument("--key", help="descarga una entrada del registry por nombre")
    ap.add_argument("--since", help="solo desde esta fecha YYYY-MM-DD (incremental)")
    ap.add_argument("--out", help="ruta de salida (con --key)")
    ap.add_argument("--resolve", metavar="QUERY", help="busca pair_id por nombre")
    ap.add_argument("--auto-map", action="store_true", help="casa los CSV EUR de Benchmark/ con su pair_id por huella de retornos")
    ap.add_argument("--add", metavar="KEY", help="añade un índice nuevo al registry (requiere --pair-id; opc. --fx, --csv-name, --subdir) y lo descarga")
    ap.add_argument("--pair-id", type=int, help="pair_id de Investing (con --add)")
    ap.add_argument("--fx", type=int, default=None, help="pair_id del FX para convertir a EUR (1 = EUR/USD; con --add)")
    ap.add_argument("--symbol", default=None, help="símbolo Investing (con --add, informativo)")
    ap.add_argument("--csv-name", default=None, help="nombre del CSV sin extensión (con --add; por defecto = KEY)")
    ap.add_argument("--subdir", default="Renta Variable", help="subcarpeta dentro de Benchmark/ (con --add)")
    args = ap.parse_args(argv)

    since = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else None

    if args.resolve:
        for r in resolve(args.resolve):
            print(f"  pair_id={r['pair_id']:<10} {str(r['symbol']):<16} {r['exchange']}")
        return 0

    if args.add:
        if not args.pair_id:
            print("Falta --pair-id. Búscalo con: --resolve \"<nombre>\"")
            return 1
        r = add_index(args.add, args.pair_id, fx=args.fx, symbol=args.symbol,
                      csv_name=args.csv_name, subdir=args.subdir, upload=False)
        if not r.get("ok"):
            print(f"ERROR: {r.get('error')}")
            return 1
        print(f"AÑADIDO '{r['key']}': pair_id={r['pair_id']} fx={r['fx']}")
        print(f"  filas={r['rows']} rango={r['from']}→{r['to']}")
        print(f"  CSV  -> {BENCH.relative_to(ROOT)}/{r['csv']}")
        print(f"  registry actualizado. Reflejar en dashboard: python -m tools.benchmarks_to_funddash --benchmark-only")
        return 0

    if args.auto_map:
        csvs = [p for p in sorted((BENCH).rglob("*.csv"))
                if ".auto" not in p.name and ("euro" in p.name.lower() or p.parent.name == "Bonos Gubernamentales")]
        print(f"Auto-mapeando {len(csvs)} CSV EUR...\n")
        res = auto_map(csvs)
        out = ROOT / "data" / "benchmarks" / "_automap_proposal.json"
        out.write_text(json.dumps([{**r, "csv": str(r["csv"].relative_to(BENCH))} for r in res], ensure_ascii=False, indent=2), encoding="utf-8")
        ok = sum(1 for r in res if r["status"] == "OK")
        print(f"\nOK={ok}  dudoso={sum(1 for r in res if r['status']=='DUDOSO')}  fallo={len(res)-ok-sum(1 for r in res if r['status']=='DUDOSO')}")
        print(f"-> {out.relative_to(ROOT)}")
        return 0

    reg = load_registry()["indices"]

    if args.pilot or args.all:
        replace = args.all
        if replace:                                   # backup de seguridad antes de sobreescribir
            import shutil
            bdir = ROOT / "backups" / "benchmarks_pre_investing"
            for key, entry in reg.items():
                src = BENCH / entry["csv"].replace("\\", "/")
                if src.exists():
                    dst = bdir / entry["csv"].replace("\\", "/")
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    if not dst.exists():
                        shutil.copy2(src, dst)
            print(f"(backup originales -> {bdir.relative_to(ROOT)})\n")
        ok = fail = 0
        for key, entry in reg.items():
            entry = {**entry, "csv": entry["csv"].replace("\\", "/")}
            orig = BENCH / entry["csv"]
            try:
                rows = build_index(entry, since=since)
            except Exception as e:  # noqa: BLE001
                print(f"  ERR  {key[:40]:40} {repr(e)[:60]}"); fail += 1; continue
            if not rows:
                print(f"  VACÍO {key[:40]:40}"); fail += 1; continue
            ds0 = sorted(rows)[0]
            if replace:
                rows = splice_preserve(rows, orig)    # conserva histórico antiguo (oro 2000-2014)
            out = orig if replace else orig.with_suffix(".auto.csv")
            write_csv(out, rows)
            ds = sorted(rows)
            tag = f" (splice<{ds0})" if (replace and ds[0] < ds0) else ""
            fid = "" if replace else f" | {_cmp_overlap(rows, orig)}"
            print(f"  OK   {key[:40]:40} filas={len(rows):>5} {ds[0]}→{ds[-1]}{tag}{fid}")
            ok += 1
        print(f"\n{'REPLACE' if replace else 'VALIDACIÓN'}: OK={ok} fallo={fail}")
        return 0

    if args.key:
        entry = reg.get(args.key)
        if not entry:
            print(f"clave no encontrada: {args.key}. Disponibles: {list(reg)}")
            return 1
        rows = build_index(entry, since=since)
        out = Path(args.out) if args.out else (BENCH / entry["csv"]).with_suffix(".auto.csv")
        write_csv(out, rows)
        ds = sorted(rows)
        print(f"{args.key}: filas={len(rows)} rango={ds[0]}→{ds[-1]} -> {out}")
        return 0

    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
