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
  function ccy(c){return (c.divisa||'')+(c.hedge?' cubierta':'');}
  function pct(v){return (v===null||v===undefined||v==='')?null:(''+(+v).toFixed(2)).replace('.',',')+'%';}
  function fees(c){var g=pct(c.comision),e=(c.exito===0||c.exito==='0')?null:pct(c.exito);
    return g?('gesti\\u00f3n '+g+(e?(' + '+e+' \\u00e9xito'):'')):null;}
  // Etiqueta para IDENTIFICAR la clase de un vistazo: divisa (+cubierta) · c\\u00f3digo · ISIN · comisiones
  function label(c){var n=c.codigo||((c.nombre_clase&&c.nombre_clase.length>1)?c.nombre_clase:'');
    return [c.divisa?ccy(c):null,n||null,c.isin,c.reparto==='Dist'?'reparto':null,fees(c)].filter(Boolean).join(' \\u00b7 ')+(c.es_primario?'  \\u2605 analizada':'');}
  // Frase llana de la clase elegida (debajo del selector)
  function describe(c){var p=[];
    p.push('Clase '+(c.codigo||c.isin)+' ('+c.isin+'): en '+(c.divisa||'?')+(c.hedge?' con la divisa cubierta (el tipo de cambio frente a la divisa del fondo no te afecta, a cambio de un coste de cobertura)':(c.divisa&&c.divisa!=='EUR'?' \\u2014 un inversor en euros asume el riesgo de cambio':'')));
    p.push(c.reparto==='Dist'?'reparte rentas':'acumula (no reparte)');
    var f=fees(c);if(f)p.push(f);
    if(c.minimo)p.push('m\\u00ednimo '+c.minimo);
    return p.join(' \\u00b7 ')+'.';}
  var ORD={EUR:0,USD:1};
  C=C.slice().sort(function(a,b){return (b.es_primario?1:0)-(a.es_primario?1:0)||((ORD[a.divisa]===undefined?2:ORD[a.divisa])-(ORD[b.divisa]===undefined?2:ORD[b.divisa]))||(''+(a.divisa||'')).localeCompare(''+(b.divisa||''))||((b.hedge?1:0)-(a.hedge?1:0))||(''+(a.codigo||a.nombre_clase)).localeCompare(''+(b.codigo||b.nombre_clase));});
  var bar=document.createElement('div');bar.className='fa-class-bar';
  bar.style.cssText='margin-top:8px;display:flex;flex-wrap:wrap;align-items:center;gap:8px;font-size:11.5px;color:rgba(255,255,255,.85);';
  var lab=document.createElement('span');lab.textContent='Clase mostrada:';lab.style.opacity='.7';
  var sel=document.createElement('select');sel.setAttribute('aria-label','Seleccionar clase');
  sel.style.cssText='background:rgba(255,255,255,.10);color:#fff;border:1px solid rgba(255,255,255,.24);border-radius:6px;padding:3px 8px;font-size:11.5px;max-width:100%;';
  C.forEach(function(c){var o=document.createElement('option');o.value=U(c.isin);o.textContent=label(c);o.style.color='#111';o.style.background='#fff';if(o.value===U(cur.isin))o.selected=true;sel.appendChild(o);});
  var warn=document.createElement('div');warn.style.cssText='flex-basis:100%;font-size:11px;color:rgba(255,255,255,.78);line-height:1.45;';
  // Los gr\\u00e1ficos cuantitativos S\\u00cd cambian a la clase elegida (window.switchClass). Solo si el
  // dashboard es antiguo y no lo soporta se avisa de que siguen siendo los de la clase analizada.
  function uw(c){var t=describe(c);
    if(U(c.isin)!==U(prim.isin)){t+=window.switchClass?' Los gr\\u00e1ficos de la pesta\\u00f1a Evoluci\\u00f3n muestran ahora ESTA clase; el an\\u00e1lisis escrito es com\\u00fan al fondo.':' \\u26a0 Los gr\\u00e1ficos cuantitativos siguen siendo los de la clase analizada ('+prim.isin+').';}
    warn.textContent=t;warn.style.display='';}
  uw(cur);
  sel.addEventListener('change',function(){var iv=sel.value,c=C.filter(function(x){return U(x.isin)===iv;})[0]||cur;uw(c);try{history.replaceState(null,'','/fund-'+iv+location.search);}catch(e){}try{if(window.switchClass)window.switchClass(iv);}catch(e){}});
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
