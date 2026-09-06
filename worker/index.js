/**
 * fund-analyzer Worker — Contrato FONDO vs CLASE (CLAUDE.md §0.9) en el borde.
 *
 * El portal embebe en un iframe `/fund-{ISIN}` con el ISIN de la CLASE pulsada. Servir
 * `./dashboard/` como assets estáticos SIN routing hacía que:
 *   - una clase sin su HTML → 404 (iframe vacío),
 *   - una clase con HTML viejo → análisis stale (clases hermanas divergentes).
 *
 * Este Worker (modo avanzado sobre static assets) ROUTEA cualquier clase de un grupo
 * multi-clase al HTML del PRIMARIO (el último análisis bueno del fondo) e INYECTA
 * `window.__FUND_CLASS_CTX__` con la clase pedida + la lista de clases del grupo, para que
 * el dashboard pinte la cabecera de ESA clase + el selector "ver otra clase". Un análisis por
 * grupo; todas las clases resuelven a él, se pulse la que se pulse (portal/BDD/fund-analyzer).
 *
 * Resiliencia: si el mapa no carga o el ISIN no es de un grupo enrutable, se cae al
 * comportamiento normal (`env.ASSETS.fetch`) → cero regresión para los fondos ya servidos.
 *
 * El mapa lo genera `tools/build_class_map.py` → `dashboard/_class_map.json`.
 */

let MAP = null; // cache por-isolate

async function loadMap(env) {
  if (MAP) return MAP;
  try {
    const res = await env.ASSETS.fetch(new URL("https://x/_class_map.json"));
    if (res.status === 200) {
      MAP = await res.json();
    } else {
      MAP = { aliases: {}, groups: {} };
    }
  } catch (_e) {
    MAP = { aliases: {}, groups: {} };
  }
  MAP.aliases = MAP.aliases || {};
  MAP.groups = MAP.groups || {};
  return MAP;
}

// primario del ISIN: él mismo si es primario de grupo, su primario si es alias, o null
function resolvePrimary(map, isin) {
  if (map.groups[isin]) return isin;
  if (map.aliases[isin]) return map.aliases[isin];
  return null;
}

// Consumidor del ctx: pinta en la cabecera un selector de CLASE + aviso de divisa. Idempotente
// (guard .fa-class-bar) y auto-suficiente, así funciona sobre CUALQUIER dashboard de primario ya
// generado sin re-generarlo. Los gráficos cuant. siguen siendo los del primario → aviso al cambiar.
const CONSUMER = `<script>(function(){
  var ctx=window.__FUND_CLASS_CTX__;
  if(!ctx||!ctx.classes||ctx.classes.length<2)return;
  if(document.querySelector('.fa-class-bar'))return;
  var C=ctx.classes,U=function(s){return (s||'').toUpperCase();};
  var prim=C.filter(function(c){return c.es_primario;})[0]||C[0];
  var cur=C.filter(function(c){return U(c.isin)===U(ctx.requested);})[0]||prim;
  function ccy(c){return (c.divisa||'')+(c.hedge?' hedged':'');}
  function label(c){var n=(c.nombre_clase&&c.nombre_clase.length>1)?c.nombre_clase:c.isin;
    var e=[c.divisa?ccy(c):null,c.anios?(c.anios+'a'):null].filter(Boolean).join(' \\u00b7 ');
    return n+(e?(' \\u2014 '+e):'')+(c.es_primario?'  \\u2605':'');}
  var bar=document.createElement('div');bar.className='fa-class-bar';
  bar.style.cssText='margin-top:8px;display:flex;flex-wrap:wrap;align-items:center;gap:8px;font-size:11.5px;color:rgba(255,255,255,.85);';
  var lab=document.createElement('span');lab.textContent='Clase mostrada:';lab.style.opacity='.7';
  var sel=document.createElement('select');sel.setAttribute('aria-label','Seleccionar clase');
  sel.style.cssText='background:rgba(255,255,255,.10);color:#fff;border:1px solid rgba(255,255,255,.24);border-radius:6px;padding:3px 8px;font-size:11.5px;max-width:100%;';
  C.forEach(function(c){var o=document.createElement('option');o.value=U(c.isin);o.textContent=label(c);o.style.color='#111';o.style.background='#fff';if(o.value===U(cur.isin))o.selected=true;sel.appendChild(o);});
  var warn=document.createElement('div');warn.style.cssText='flex-basis:100%;font-size:11px;color:#ffd27a;';
  function uw(c){if(U(c.isin)!==U(prim.isin)&&ccy(c)!==ccy(prim)){warn.textContent='\\u26a0 Los gr\\u00e1ficos cuantitativos son de la clase primaria '+ccy(prim)+' ('+prim.isin+'). Esta clase es '+ccy(c)+' y sus m\\u00e9tricas pueden diferir.';warn.style.display='';}else{warn.textContent='';warn.style.display='none';}}
  uw(cur);
  sel.addEventListener('change',function(){var iv=sel.value,c=C.filter(function(x){return U(x.isin)===iv;})[0]||cur;uw(c);try{history.replaceState(null,'','/fund-'+iv+location.search);}catch(e){}});
  bar.appendChild(lab);bar.appendChild(sel);bar.appendChild(warn);
  function mount(){(document.querySelector('.lh-left')||document.body).appendChild(bar);}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount);else mount();
})();</script>`;

function injectCtx(html, ctx) {
  const tag =
    "<script>window.__FUND_CLASS_CTX__=" +
    JSON.stringify(ctx).replace(/</g, "\\u003c") +
    ";</script>" +
    CONSUMER;
  if (html.includes("</head>")) return html.replace("</head>", tag + "</head>");
  return tag + html;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/fund-([A-Za-z0-9]{12})\/?$/);
    if (!m) return env.ASSETS.fetch(request);

    const isin = m[1].toUpperCase();
    const map = await loadMap(env);
    const primary = resolvePrimary(map, isin);
    if (!primary) return env.ASSETS.fetch(request); // no es grupo enrutable → normal

    // Trae el HTML del PRIMARIO (aunque se haya pedido un alias con HTML propio stale)
    const primReq = new Request(new URL(`/fund-${primary}`, url), request);
    const res = await env.ASSETS.fetch(primReq);
    if (res.status !== 200) return env.ASSETS.fetch(request); // primario sin fichero → normal

    const grp = map.groups[primary] || { primary, classes: [] };
    const ctx = {
      requested: isin,
      primary,
      nombre: grp.nombre || "",
      classes: grp.classes || [],
    };
    const html = injectCtx(await res.text(), ctx);
    const headers = new Headers(res.headers);
    headers.set("content-type", "text/html; charset=utf-8");
    headers.set("cache-control", "public, max-age=300");
    return new Response(html, { status: 200, headers });
  },
};
