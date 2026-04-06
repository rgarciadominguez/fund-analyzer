"""
Fund Analyzer — Streamlit Dashboard v4  (dark/light executive theme)
"""
import json
import re
import urllib.parse
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime

import ui_components
from ui_components import (
    section_header, narrative_block, timeline_item,
    dual_timeline_item, consistency_period, stat_row, empty_state,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fund Analyzer",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state ─────────────────────────────────────────────────────────────
if "selected_isin" not in st.session_state:
    st.session_state.selected_isin = None
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

dark_mode = st.session_state.dark_mode

# ── Palettes ──────────────────────────────────────────────────────────────────
DARK = {
    "BG":"#0d0f14","BG2":"#13161e","BG3":"#1a1d28","BORDER":"#1e2130",
    "TEXT":"#e8e8e8","TEXT2":"#bbb","TEXT3":"#666",
    "ACCENT":"#4fc3f7",
    "GREEN":"#4caf50","GREEN_CHART":"#10b981",
    "RED":"#ef5350","RED_CHART":"#ef4444",
    "YELLOW":"#f59e0b","YELLOW_CHART":"#f59e0b",
    "PURPLE":"#a78bfa",
    # CSS-specific
    "TAB_BG":"#13161e","TAB_INACTIVE":"#555",
    "METRIC_BG":"#13161e","METRIC_BORDER":"#1e2130",
    "METRIC_LABEL":"#555","METRIC_VALUE":"#e8e8e8",
    "EXP_BG":"#13161e","EXP_BORDER":"#1e2130","EXP_TEXT":"#888",
    "SBOX_BG":"#0d0f14","SBOX_BORDER":"#1e2130","SBOX_TEXT":"#999",
    "INPUT_BG":"#13161e","INPUT_TEXT":"#e8e8e8",
    "TAG_BG":"#1a1d28","TAG_TEXT":"#888",
    "GRID":"#1e2130",
}
LIGHT = {
    "BG":"#f5f7fa","BG2":"#ffffff","BG3":"#fafafa","BORDER":"#e5e5e5",
    "TEXT":"#1a1a1a","TEXT2":"#444444","TEXT3":"#888888",
    "ACCENT":"#1e40af",
    "GREEN":"#2e7d32","GREEN_CHART":"#10b981",
    "RED":"#c62828","RED_CHART":"#ef4444",
    "YELLOW":"#b45309","YELLOW_CHART":"#f59e0b",
    "PURPLE":"#7c3aed",
    # CSS-specific
    "TAB_BG":"#ffffff","TAB_INACTIVE":"#888",
    "METRIC_BG":"#ffffff","METRIC_BORDER":"#e5e5e5",
    "METRIC_LABEL":"#888","METRIC_VALUE":"#1a1a1a",
    "EXP_BG":"#fafafa","EXP_BORDER":"#e5e5e5","EXP_TEXT":"#555",
    "SBOX_BG":"#f5f7fa","SBOX_BORDER":"#e5e5e5","SBOX_TEXT":"#444",
    "INPUT_BG":"#ffffff","INPUT_TEXT":"#1a1a1a",
    "TAG_BG":"#f0f0f0","TAG_TEXT":"#555",
    "GRID":"#e5e5e5",
}

P = DARK if dark_mode else LIGHT

# Convenience vars
BG=P["BG"]; BG2=P["BG2"]; BG3=P["BG3"]; BORDER=P["BORDER"]
TEXT=P["TEXT"]; TEXT2=P["TEXT2"]; TEXT3=P["TEXT3"]
ACCENT=P["ACCENT"]
GREEN=P["GREEN"]; GREEN_CHART=P["GREEN_CHART"]
RED=P["RED"]; RED_CHART=P["RED_CHART"]
YELLOW=P["YELLOW"]; YELLOW_CHART=P["YELLOW_CHART"]
PURPLE=P["PURPLE"]

# Propagate theme to component library
ui_components.set_theme(
    accent=ACCENT, bg2=BG2, border=BORDER,
    text=TEXT, text2=TEXT2, text3=TEXT3,
    green=GREEN, red=RED, yellow=YELLOW,
    sbox_bg=P["SBOX_BG"],
)

# ── Global CSS (dynamic, palette-driven) ──────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Sora:wght@300;400;600;700&display=swap');

/* ── Reset & base ── */
.block-container {{ padding-top:1.2rem !important; padding-bottom:1rem !important; max-width:1140px !important; }}
.element-container {{ margin-bottom:0.35rem !important; }}
div[data-testid="stVerticalBlock"] > div {{ gap:0.35rem !important; }}

html, body, [class*="css"] {{
  font-family:'Sora', sans-serif !important;
  font-size:13.5px;
  color:{TEXT};
}}

.stApp {{ background-color:{BG} !important; }}
section[data-testid="stSidebar"] {{ display:none; }}
header[data-testid="stHeader"] {{ display:none; }}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {{
  background:{P["TAB_BG"]};
  border-radius:0;
  border-bottom:1px solid {BORDER};
  gap:0; padding:0;
}}
.stTabs [data-baseweb="tab"] {{
  font-family:'DM Mono', monospace !important;
  font-size:10.5px;
  letter-spacing:0.09em;
  text-transform:uppercase;
  padding:10px 18px;
  border-bottom:2px solid transparent;
  color:{P["TAB_INACTIVE"]};
  background:transparent !important;
}}
.stTabs [aria-selected="true"] {{
  border-bottom:2px solid {ACCENT} !important;
  color:{ACCENT} !important;
  background:transparent !important;
}}
.stTabs [data-baseweb="tab-panel"] {{ padding-top:18px; }}

/* ── Metrics ── */
[data-testid="stMetric"] {{
  background:{P["METRIC_BG"]};
  border:1px solid {P["METRIC_BORDER"]};
  border-left:3px solid {ACCENT};
  padding:14px 18px !important;
  border-radius:4px;
}}
[data-testid="stMetricLabel"] {{
  font-family:'DM Mono', monospace !important;
  font-size:9.5px !important;
  letter-spacing:0.12em;
  text-transform:uppercase;
  color:{P["METRIC_LABEL"]} !important;
}}
[data-testid="stMetricValue"] {{
  font-family:'DM Mono', monospace !important;
  font-size:20px !important;
  font-weight:500 !important;
  color:{P["METRIC_VALUE"]} !important;
}}
[data-testid="stMetricDelta"] {{ font-size:10px !important; }}

/* ── Expanders ── */
details {{
  background:{P["EXP_BG"]} !important;
  border:1px solid {P["EXP_BORDER"]} !important;
  border-radius:4px !important;
  margin-bottom:4px !important;
}}
details summary {{
  font-family:'DM Mono', monospace !important;
  font-size:11px !important;
  padding:10px 16px !important;
  color:{P["EXP_TEXT"]} !important;
  letter-spacing:0.04em;
}}
details summary:hover {{ color:{ACCENT} !important; }}

/* ── Scrollable box ── */
.sbox {{
  background:{P["SBOX_BG"]};
  border:1px solid {P["SBOX_BORDER"]};
  border-radius:4px;
  padding:12px 14px;
  font-size:12.5px;
  color:{P["SBOX_TEXT"]};
  line-height:1.75;
  max-height:220px;
  overflow-y:auto;
}}

/* ── Misc ── */
.stAlert {{ display:none !important; }}
hr {{ border:none; border-top:1px solid {BORDER}; margin:1.2rem 0; }}
.modebar {{ display:none !important; }}

/* ── Form controls ── */
.stSelectbox > div > div, .stTextInput > div > div > input {{
  background:{P["INPUT_BG"]} !important;
  color:{P["INPUT_TEXT"]} !important;
  border-color:{BORDER} !important;
  font-family:'DM Mono', monospace !important;
  font-size:12px !important;
}}

/* ── Position bar ── */
.pos-bar-bg {{ background:{BORDER}; border-radius:2px; height:3px; margin-top:4px; }}
.pos-bar-fill {{ background:{ACCENT}; border-radius:2px; height:3px; }}

/* ── Tag/badge ── */
.tag {{
  display:inline-block;
  background:{P["TAG_BG"]};
  border:1px solid {BORDER};
  border-radius:3px;
  padding:2px 8px;
  font-family:'DM Mono', monospace;
  font-size:10px;
  letter-spacing:0.06em;
  color:{P["TAG_TEXT"]};
  margin-right:4px;
}}
.tag-accent {{ border-color:{ACCENT}; color:{ACCENT}; }}
.tag-green  {{ border-color:{GREEN}; color:{GREEN}; }}
.tag-red    {{ border-color:{RED}; color:{RED}; }}
.tag-amber  {{ border-color:{YELLOW}; color:{YELLOW}; }}

/* ── Toggle button ── */
button[kind="secondary"] {{
  font-family:'DM Mono', monospace !important;
  font-size:10px !important;
  letter-spacing:0.08em !important;
  border-radius:3px !important;
  padding:4px 12px !important;
  border-color:{BORDER} !important;
  background:{BG2} !important;
  color:{TEXT3} !important;
}}
button[kind="secondary"]:hover {{ color:{ACCENT} !important; border-color:{ACCENT} !important; }}

/* ── Links ── */
a {{ color:{ACCENT} !important; text-decoration:none !important; }}
a:hover {{ text-decoration:underline !important; }}
</style>
""", unsafe_allow_html=True)

# ── Data loaders ──────────────────────────────────────────────────────────────

def load_output(isin: str) -> dict:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def load_json(isin: str, filename: str) -> dict | list:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / filename
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _fund_has_data(data: dict) -> bool:
    if not data.get("nombre"):
        return False
    kpis  = data.get("kpis") or {}
    cuant = data.get("cuantitativo") or {}
    cual  = data.get("cualitativo") or {}
    has_aum    = bool(kpis.get("aum_actual_meur"))
    has_mix    = bool(cuant.get("mix_activos_historico"))
    has_pos    = bool((data.get("posiciones") or {}).get("actuales"))
    has_gestores = any(g.get("nombre") for g in (cual.get("gestores") or []))
    return has_aum or has_mix or has_pos or has_gestores


def discover_funds() -> list[dict]:
    base = Path(__file__).parent.parent / "data" / "funds"
    if not base.exists():
        return []
    result = []
    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        op = d / "output.json"
        if not op.exists():
            continue
        try:
            data = json.loads(op.read_text(encoding="utf-8"))
            if not _fund_has_data(data):
                continue
            result.append({"isin": d.name, "nombre": data.get("nombre", d.name)})
        except Exception:
            pass
    return sorted(result, key=lambda x: x["nombre"])

# ── Formatters ────────────────────────────────────────────────────────────────

def es(v, dec=2, suffix="") -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
        s = f"{v:,.{dec}f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return s + suffix
    except Exception:
        return str(v)


def pct(v) -> str:
    return es(v, 2, "%")


def meur(v) -> str:
    return es(v, 3, " M€")


def normalize_year(periodo: str) -> str:
    m = re.match(r"^(20\d{2})", str(periodo))
    return m.group(1) if m else str(periodo)


def manager_slug(name: str) -> str:
    s = name.lower().strip()
    for a, b in [("á","a"),("à","a"),("ä","a"),("é","e"),("è","e"),("ë","e"),
                 ("í","i"),("ì","i"),("ï","i"),("ó","o"),("ò","o"),("ö","o"),
                 ("ú","u"),("ù","u"),("ü","u"),("ñ","n")]:
        s = s.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

# ── Chart helpers ─────────────────────────────────────────────────────────────

def chart_layout(height=280, legend=True) -> dict:
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT3, size=10, family="DM Mono, monospace"),
        height=height,
        margin=dict(l=0, r=0, t=10, b=40),
        showlegend=legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=TEXT3),
        ),
        xaxis=dict(showgrid=False, tickfont=dict(color=TEXT3, size=10),
                   linecolor=BORDER, tickangle=-30),
        yaxis=dict(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                   tickfont=dict(color=TEXT3, size=10)),
    )

# ── Country inference ─────────────────────────────────────────────────────────

ISIN_COUNTRY_MAP = {
    "ES":"España","DE":"Alemania","FR":"Francia","IT":"Italia",
    "NL":"Países Bajos","GB":"Reino Unido","US":"EE.UU.","JP":"Japón",
    "CH":"Suiza","AU":"Australia","CA":"Canadá","SE":"Suecia",
    "NO":"Noruega","DK":"Dinamarca","FI":"Finlandia","PT":"Portugal",
    "BE":"Bélgica","AT":"Austria","IE":"Irlanda","MX":"México",
    "BR":"Brasil","CN":"China","IN":"India","KR":"Corea del Sur",
    "TW":"Taiwan","HK":"Hong Kong","SG":"Singapur","ZA":"Sudáfrica",
    "PL":"Polonia","CZ":"Rep. Checa","HU":"Hungría","RO":"Rumanía",
    "TR":"Turquía","RU":"Rusia","AE":"EAU","SA":"Arabia Saudí",
    "LU":"Luxemburgo","XS":"Internacional","XF":"Internacional",
    "GR":"Grecia","SK":"Eslovaquia","SI":"Eslovenia","HR":"Croacia",
    "CL":"Chile","CO":"Colombia","PE":"Perú","AR":"Argentina",
    "IL":"Israel","TH":"Tailandia","ID":"Indonesia","MY":"Malasia",
    "PH":"Filipinas","VN":"Vietnam","EG":"Egipto","MA":"Marruecos",
    "NG":"Nigeria","KE":"Kenia",
}

NAME_COUNTRY_MAP = {
    "SPAIN":"España","ESPAÑA":"España","REINO DE ESPAÑA":"España",
    "GERMANY":"Alemania","BUNDESREPUBLIK":"Alemania","DEUTSCHLAND":"Alemania",
    "FRANCE":"Francia","REPUBLIQUE FRANCAISE":"Francia","FRENCH":"Francia",
    "ITALY":"Italia","ITALIA":"Italia","REPUBBLICA ITALIANA":"Italia","ITALIAN":"Italia",
    "NETHERLANDS":"Países Bajos","NEDERLAND":"Países Bajos","DUTCH":"Países Bajos",
    "UNITED KINGDOM":"Reino Unido","UK GILT":"Reino Unido","GILT":"Reino Unido",
    "BRITISH":"Reino Unido","ENGLAND":"Reino Unido",
    "UNITED STATES":"EE.UU.","US TREASURY":"EE.UU.","U.S. TREASURY":"EE.UU.",
    "AMERICAN":"EE.UU.","USA":"EE.UU.",
    "JAPAN":"Japón","JAPANESE":"Japón","NIPPON":"Japón",
    "SWITZERLAND":"Suiza","SWISS":"Suiza","EIDGENOSSENSCHAFT":"Suiza",
    "CANADA":"Canadá","CANADIAN":"Canadá",
    "AUSTRALIA":"Australia","AUSTRALIAN":"Australia",
    "SWEDEN":"Suecia","SVENSKA":"Suecia","SWEDISH":"Suecia",
    "NORWAY":"Noruega","NORGES":"Noruega","NORWEGIAN":"Noruega",
    "DENMARK":"Dinamarca","DANISH":"Dinamarca",
    "FINLAND":"Finlandia","FINNISH":"Finlandia",
    "PORTUGAL":"Portugal","PORTUGUESE":"Portugal",
    "BELGIUM":"Bélgica","BELGIQUE":"Bélgica","BELGIAN":"Bélgica",
    "AUSTRIA":"Austria","AUSTRIAN":"Austria",
    "IRELAND":"Irlanda","IRISH":"Irlanda",
    "MEXICO":"México","MEXICAN":"México",
    "BRAZIL":"Brasil","BRASIL":"Brasil","BRAZILIAN":"Brasil",
    "CHINA":"China","CHINESE":"China","PEOPLES REPUBLIC":"China",
    "INDIA":"India","INDIAN":"India",
    "KOREA":"Corea del Sur","REPUBLIC OF KOREA":"Corea del Sur","KOREAN":"Corea del Sur",
    "TAIWAN":"Taiwan","HONG KONG":"Hong Kong","SINGAPORE":"Singapur",
    "SOUTH AFRICA":"Sudáfrica","POLAND":"Polonia","POLSKA":"Polonia","POLISH":"Polonia",
    "CZECH":"Rep. Checa","HUNGARY":"Hungría","HUNGARIAN":"Hungría",
    "TURKEY":"Turquía","TURKIYE":"Turquía","TURKISH":"Turquía",
    "RUSSIA":"Rusia","RUSSIAN":"Rusia",
    "GREECE":"Grecia","GREEK":"Grecia","HELLENIC":"Grecia",
    "CHILE":"Chile","CHILEAN":"Chile","COLOMBIA":"Colombia","COLOMBIAN":"Colombia",
    "PERU":"Perú","PERUVIAN":"Perú","ARGENTINA":"Argentina","ARGENTINE":"Argentina",
    "ISRAEL":"Israel","ISRAELI":"Israel",
}

DIVISA_REGION = {
    "EUR":"Eurozona","USD":"EE.UU.","GBP":"Reino Unido","JPY":"Japón",
    "CHF":"Suiza","SEK":"Suecia","NOK":"Noruega","DKK":"Dinamarca",
    "AUD":"Australia","CAD":"Canadá","HKD":"Hong Kong","SGD":"Singapur",
    "CNY":"China","CNH":"China","BRL":"Brasil","MXN":"México","INR":"India",
    "KRW":"Corea del Sur","TWD":"Taiwan","ZAR":"Sudáfrica","PLN":"Polonia",
    "CZK":"Rep. Checa","HUF":"Hungría","TRY":"Turquía","RUB":"Rusia",
}

GEO_COLORS = [
    "#4fc3f7","#10b981","#f59e0b","#ef4444","#a78bfa",
    "#06b6d4","#f97316","#84cc16","#ec4899","#6b7280",
]

TIPO_COLOR = {
    "REPO":YELLOW_CHART,"BONO":ACCENT,"IIC":GREEN_CHART,
    "PARTICIPACIONES":GREEN_CHART,"PAGARE":PURPLE,
    "OBLIGACION":"#60a5fa","RENTA FIJA":ACCENT,"ETC":"#34d399",
}
MIX_COLORS = {
    "renta_fija_pct":ACCENT,"rv_pct":GREEN_CHART,
    "iic_pct":PURPLE,"liquidez_pct":YELLOW_CHART,"depositos_pct":"#6b7280",
}
MIX_LABELS = {
    "renta_fija_pct":"Renta Fija","rv_pct":"Renta Variable",
    "iic_pct":"IIC / ETF","liquidez_pct":"Liquidez","depositos_pct":"Depósitos",
}


def infer_country(pos: dict) -> str:
    ticker = str(pos.get("ticker","") or "").upper().strip()
    nombre = str(pos.get("nombre","") or "").upper()
    tipo   = str(pos.get("tipo","") or "").upper()
    if len(ticker) >= 2 and ticker[:2].isalpha():
        cc = ticker[:2]
        if cc in ISIN_COUNTRY_MAP:
            c = ISIN_COUNTRY_MAP[cc]
            if c != "Luxemburgo":
                return c
    for kw, country in NAME_COUNTRY_MAP.items():
        if kw in nombre:
            return country
    if tipo in ("IIC","PARTICIPACIONES","ETC","ETF"):
        div = str(pos.get("divisa","") or "")
        return DIVISA_REGION.get(div, "Internacional")
    div = str(pos.get("divisa","") or "")
    if div in DIVISA_REGION:
        return DIVISA_REGION[div]
    return "Otros"

# ── TOP BAR ───────────────────────────────────────────────────────────────────
funds = discover_funds()

bar_logo, bar_sel, bar_toggle = st.columns([2, 9, 1])

with bar_logo:
    st.markdown(
        f'<div style="font-family:\'DM Mono\',monospace; font-size:13px; '
        f'font-weight:500; color:{ACCENT}; letter-spacing:0.14em; '
        f'text-transform:uppercase; padding-top:8px;">◈ Fund Analyzer</div>',
        unsafe_allow_html=True)

with bar_sel:
    nombres = [f["nombre"] for f in funds]
    isins   = [f["isin"]   for f in funds]
    default_idx = 0
    if st.session_state.selected_isin and st.session_state.selected_isin in isins:
        default_idx = isins.index(st.session_state.selected_isin)
    sel_nombre = st.selectbox(
        "Fondo", options=nombres, index=default_idx, label_visibility="collapsed",
    )
    sel_idx = nombres.index(sel_nombre)
    st.session_state.selected_isin = isins[sel_idx]

with bar_toggle:
    toggle_label = "☀ Light" if dark_mode else "◐ Dark"
    if st.button(toggle_label, key="theme_toggle", use_container_width=True):
        st.session_state.dark_mode = not dark_mode
        st.rerun()

st.markdown(f"<div style='height:1px;background:{BORDER};margin:6px 0 14px 0'></div>",
            unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
d         = load_output(st.session_state.selected_isin)
kpis      = d.get("kpis", {})
cual      = d.get("cualitativo", {})
cuant     = d.get("cuantitativo", {})
pos_data  = d.get("posiciones", {})
consist   = d.get("analisis_consistencia", {})
fuentes   = d.get("fuentes", {})

# Periods: ascending for display (oldest first)
periodos_asc = sorted(consist.get("periodos", []),
                      key=lambda p: str(p.get("periodo", "")))
# For quick access to most-recent, keep desc version too
periodos_desc = list(reversed(periodos_asc))

letters_d    = load_json(st.session_state.selected_isin, "letters_data.json")
lecturas_d   = load_json(st.session_state.selected_isin, "lecturas.json")
analisis_ext = load_json(st.session_state.selected_isin, "analisis_externos.json")
meta_report  = load_json(st.session_state.selected_isin, "meta_report.json")

# ── FUND HEADER ───────────────────────────────────────────────────────────────
clasificacion = kpis.get("clasificacion") or "—"
perfil        = kpis.get("perfil_riesgo") or "—"
fecha_reg     = kpis.get("fecha_registro") or "—"
gestora_name  = d.get("gestora") or "—"
depositario   = kpis.get("depositario") or "—"
divisa_f      = kpis.get("divisa") or "EUR"

tags = " ".join(
    f'<span class="tag">{t}</span>'
    for t in [clasificacion, f"Riesgo {perfil}/7", divisa_f, f"Dep. {depositario}"]
    if t and t != "—"
)

st.markdown(f"""
<div style="background:linear-gradient(135deg,#0a0c12,#111520);
            border:1px solid {BORDER}; border-radius:6px;
            padding:20px 26px; margin-bottom:14px;">
  <div style="display:flex; justify-content:space-between; align-items:flex-start;">
    <div>
      <div style="font-family:'Sora',sans-serif; font-size:19px; font-weight:700;
                  color:#f0f0f0; letter-spacing:-0.01em;">
        {d.get('nombre', sel_nombre)}
      </div>
      <div style="font-family:'DM Mono',monospace; font-size:10px;
                  letter-spacing:0.08em; color:{TEXT3}; margin-top:6px;
                  text-transform:uppercase;">{gestora_name}</div>
    </div>
    <div style="text-align:right;">
      <span style="font-family:'DM Mono',monospace; font-size:12px;
                   font-weight:500; color:{ACCENT};
                   background:#0d0f14; border:1px solid {BORDER};
                   border-radius:3px; padding:4px 10px;">
        {st.session_state.selected_isin}
      </span>
      <div style="font-family:'DM Mono',monospace; font-size:9px;
                  color:{TEXT3}; margin-top:6px; letter-spacing:0.06em;">
        REG {fecha_reg}
      </div>
    </div>
  </div>
  <div style="margin-top:14px;">{tags}</div>
</div>
""", unsafe_allow_html=True)

# ── KPI ROW ───────────────────────────────────────────────────────────────────
aum      = kpis.get("aum_actual_meur")
part     = kpis.get("num_participes")
part_ant = kpis.get("num_participes_anterior")
ter      = kpis.get("ter_pct")
gestion  = kpis.get("coste_gestion_pct")
deposito = kpis.get("coste_deposito_pct")
vol      = kpis.get("volatilidad_pct")

part_delta = None
if part and part_ant and part_ant != 0:
    part_delta = (part - part_ant) / part_ant * 100

k1, k2, k3, k4, k5, k6 = st.columns(6)
kpi_cols = [
    (k1, "AUM",        meur(aum),            None),
    (k2, "Partícipes", es(part, 0),          f"{'+' if (part_delta or 0)>0 else ''}{es(part_delta,1)}%" if part_delta else None),
    (k3, "TER",        pct(ter),             f"Gest {pct(gestion)}  Dep {pct(deposito)}"),
    (k4, "Volatilidad",pct(vol),             None),
    (k5, "Riesgo",     f"{perfil} / 7",      None),
    (k6, "Posiciones", str(len(pos_data.get("actuales",[]))), "activos en cartera"),
]
for col, label, value, delta in kpi_cols:
    with col:
        st.metric(label=label, value=value, delta=delta)

st.markdown(f"<div style='height:4px'></div>", unsafe_allow_html=True)

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Resumen",
    "Historia & Gestores",
    "Evolutivo",
    "Cartera",
    "Consistencia",
    "Lecturas",
    "Análisis ext.",
    "Archivos",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RESUMEN
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    estrategia = cual.get("estrategia") or ""
    filosofia  = cual.get("filosofia_inversion") or ""
    proceso    = cual.get("proceso_seleccion") or ""

    section_header("Estrategia ejecutiva")
    if estrategia:
        narrative_block(estrategia, "SÍNTESIS")
    else:
        empty_state("Sin datos de estrategia — ejecuta el pipeline con API key configurada.")

    if filosofia and filosofia != estrategia:
        section_header("Filosofía de inversión")
        narrative_block(filosofia, "FILOSOFÍA")

    if proceso:
        section_header("Proceso de selección")
        narrative_block(proceso, "SELECCIÓN")

    # Tipo de activos
    tipo_activos = cual.get("tipo_activos", "")
    if tipo_activos:
        section_header("Universo de inversión")
        narrative_block(tipo_activos, "UNIVERSO")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — HISTORIA & GESTORES
# ══════════════════════════════════════════════════════════════════════════════
with tab2:

    # ── Historia del fondo — timeline ascending ───────────────────────────────
    historia = cual.get("historia_fondo", "")
    if historia:
        section_header("Historia del fondo")
        paragraphs = [p.strip() for p in re.split(r"\n{2,}|\n(?=\d+\.)", historia)
                      if len(p.strip()) > 30]
        if paragraphs:
            for para in paragraphs:
                year_m = re.search(r"\b(20\d{2}|19\d{2})\b", para)
                yr = year_m.group(1) if year_m else ""
                timeline_item(yr, para)
        else:
            timeline_item("", historia)

    # ── Hechos relevantes — ascending ────────────────────────────────────────
    hechos_relevantes = cual.get("hechos_relevantes", [])
    hechos_con_contenido = [h for h in hechos_relevantes if h.get("epigrafe") or h.get("detalle")]
    if hechos_con_contenido:
        section_header("Hechos relevantes", accent_color=YELLOW_CHART)
        for hr in sorted(hechos_con_contenido, key=lambda x: x.get("periodo", "")):
            periodo_hr  = hr.get("periodo", "")
            epigrafe_hr = hr.get("epigrafe", "")
            detalle_hr  = hr.get("detalle", "")
            label = f"<strong>{epigrafe_hr}</strong> — " if epigrafe_hr else ""
            timeline_item(periodo_hr, label + detalle_hr, color=YELLOW_CHART)

    # ── Equipo gestor ─────────────────────────────────────────────────────────
    gestores = cual.get("gestores", [])
    if gestores:
        section_header("Equipo gestor")
        for g in gestores:
            nombre_g = g.get("nombre") or ""
            if not nombre_g:
                continue
            cargo_g  = g.get("cargo") or ""
            back_g   = g.get("background") or ""
            anio_g   = g.get("anio_incorporacion") or ""
            slug     = manager_slug(nombre_g)
            q_enc    = urllib.parse.quote(f'"{nombre_g}" gestor fondo')

            links = " ".join([
                f'<a href="https://citywire.com/selector/manager/profile/{slug}" target="_blank" class="tag tag-accent">Citywire</a>',
                f'<a href="https://www.finect.com/user/{slug}" target="_blank" class="tag tag-accent">Finect</a>',
                f'<a href="https://www.google.com/search?q={q_enc}" target="_blank" class="tag">Google</a>',
            ])

            desde = f"<span style='color:{TEXT3}; font-size:10px;'>  ·  desde {anio_g}</span>" if anio_g else ""
            bg_html = f"<div style='font-size:12.5px; color:{TEXT2}; line-height:1.7; margin:8px 0;'>{back_g}</div>" if back_g else ""
            st.markdown(f"""
            <div style="background:{BG2}; border:1px solid {BORDER}; border-left:3px solid {ACCENT};
                        border-radius:4px; padding:14px 18px; margin-bottom:8px;">
              <div style="font-size:14px; font-weight:600; color:{TEXT};">{nombre_g}</div>
              <div style="font-family:'DM Mono',monospace; font-size:9.5px; letter-spacing:0.08em;
                          text-transform:uppercase; color:{TEXT3}; margin-top:3px;">
                {cargo_g}{desde}
              </div>
              {bg_html}
              <div style="margin-top:10px;">{links}</div>
            </div>""", unsafe_allow_html=True)

    # ── Visión gestores — dual timeline ascending ─────────────────────────────
    if periodos_asc:
        section_header("Visión de los gestores — año a año",
                       subtitle=f"{periodos_asc[0].get('periodo','')[:4]} → {periodos_asc[-1].get('periodo','')[:4]}")

        # Column headers
        h_l, h_yr, h_r = st.columns([1, 0.4, 1])
        with h_l:
            st.markdown(f"<div style='text-align:right; font-family:\"DM Mono\",monospace;"
                        f" font-size:9px; letter-spacing:0.10em; text-transform:uppercase;"
                        f" color:{TEXT3};'>Tesis / visión</div>", unsafe_allow_html=True)
        with h_r:
            st.markdown(f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                        f" letter-spacing:0.10em; text-transform:uppercase;"
                        f" color:{TEXT3};'>Decisiones tomadas</div>", unsafe_allow_html=True)

        for pdata in periodos_asc[-10:]:
            yr_label   = pdata.get("periodo", "—")
            tesis      = (pdata.get("tesis_gestora", "") or "")[:400]
            decisiones = (pdata.get("decisiones_tomadas", "") or "")[:400]
            dual_timeline_item(yr_label, tesis, decisiones)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EVOLUTIVO
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    # ── AUM ──────────────────────────────────────────────────────────────────
    section_header("Evolución del patrimonio (AUM)")
    serie_aum = [s for s in cuant.get("serie_aum", [])
                 if s.get("valor_meur") and len(str(s["periodo"])) <= 7]

    if serie_aum:
        serie_aum_s = sorted(serie_aum, key=lambda x: str(x["periodo"]))
        year_map: dict[str, dict] = {}
        for s in serie_aum_s:
            yr = normalize_year(str(s["periodo"]))
            if yr not in year_map or (s.get("valor_meur") or 0) > (year_map[yr].get("valor_meur") or 0):
                year_map[yr] = s
        deduped = sorted(year_map.values(), key=lambda x: normalize_year(str(x["periodo"])))
        labels = [normalize_year(str(s["periodo"])) for s in deduped]
        values = [s["valor_meur"] for s in deduped]
        vls    = [s.get("vl") if (s.get("vl") and 1 < (s.get("vl") or 0) < 100000
                                  and (s.get("vl") or 0) < 2010) else None
                  for s in deduped]
        vl_x = [labels[i] for i, v in enumerate(vls) if v]
        vl_y = [v for v in vls if v]
        has_vl = bool(vl_y)
        rows = 2 if has_vl else 1
        row_heights = [0.6, 0.4] if has_vl else [1.0]
        fig = make_subplots(rows=rows, cols=1, shared_xaxes=True, shared_yaxes=False,
                            row_heights=row_heights, vertical_spacing=0.08)
        fig.add_trace(go.Bar(
            x=labels, y=values, name="AUM (M€)",
            marker_color=ACCENT,
            text=[f"{v:.1f}" for v in values],
            textposition="outside",
            textfont=dict(size=10, color=TEXT3, family="DM Mono, monospace"),
            hovertemplate="<b>%{x}</b><br>AUM: %{y:.2f} M€<extra></extra>",
        ), row=1, col=1)
        if has_vl:
            fig.add_trace(go.Scatter(
                x=vl_x, y=vl_y, mode="lines+markers", name="VL",
                line=dict(color=GREEN_CHART, width=2),
                marker=dict(size=6, color=GREEN_CHART),
                hovertemplate="<b>%{x}</b><br>VL: %{y:.4f}<extra></extra>",
            ), row=2, col=1)
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=TEXT3, size=10, family="DM Mono, monospace"),
            height=320 if has_vl else 280,
            margin=dict(l=0, r=0, t=10, b=40), showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=TEXT3)),
        )
        fig.update_xaxes(showgrid=False, tickfont=dict(color=TEXT3, size=10),
                         linecolor=BORDER, tickangle=-30, type="category")
        fig.update_yaxes(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                         tickfont=dict(color=TEXT3, size=10), row=1, col=1)
        if has_vl:
            fig.update_yaxes(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                             tickfont=dict(color=TEXT3, size=10), row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state("Sin datos de AUM histórico disponibles.")

    col_part, col_ter = st.columns(2)

    # ── Partícipes ────────────────────────────────────────────────────────────
    with col_part:
        section_header("Evolución de partícipes")
        serie_p = cuant.get("serie_participes", [])
        part_points = [{"periodo": s["periodo"], "valor": s["valor"]}
                       for s in serie_p if s.get("valor")]
        yr_map_p: dict[str, float] = {}
        for p in part_points:
            yr = normalize_year(str(p["periodo"]))
            if yr not in yr_map_p or p["valor"] > yr_map_p[yr]:
                yr_map_p[yr] = p["valor"]
        part_sorted = sorted(yr_map_p.items())
        if part_sorted:
            fig_p = go.Figure(go.Bar(
                x=[x[0] for x in part_sorted],
                y=[x[1] for x in part_sorted],
                marker_color=ACCENT,
                text=[f"{int(x[1])}" for x in part_sorted],
                textposition="outside",
                textfont=dict(size=10, color=TEXT3, family="DM Mono, monospace"),
                width=[0.5] * len(part_sorted),
                hovertemplate="<b>%{x}</b><br>Partícipes: %{y:.0f}<extra></extra>",
            ))
            fig_p.update_layout(**chart_layout(260, legend=False))
            fig_p.update_xaxes(type="category")
            st.plotly_chart(fig_p, use_container_width=True)
        else:
            empty_state("Sin datos de partícipes históricos.")

    # ── TER ───────────────────────────────────────────────────────────────────
    with col_ter:
        section_header("Evolución TER y comisión de gestión")
        serie_ter = cuant.get("serie_ter", [])
        ter_points = [t for t in serie_ter if t.get("ter_pct") or t.get("coste_gestion_pct")]
        if ter and not any(normalize_year(str(t.get("periodo",""))) ==
                           normalize_year(str(datetime.now().year)) for t in ter_points):
            ter_points.append({"periodo": str(datetime.now().year),
                               "ter_pct": ter, "coste_gestion_pct": gestion})
        yr_map_ter: dict[str, dict] = {}
        for t in ter_points:
            yr = normalize_year(str(t.get("periodo", "")))
            if yr not in yr_map_ter:
                yr_map_ter[yr] = t
        ter_sorted = sorted(yr_map_ter.items())

        serie_cls = cuant.get("serie_comisiones_por_clase", [])
        yr_map_cls: dict[str, dict] = {}
        for e in serie_cls:
            yr = normalize_year(str(e.get("periodo", "")))
            if yr not in yr_map_cls:
                yr_map_cls[yr] = e.get("clases", {})
        cls_sorted = sorted(yr_map_cls.items())

        CLASE_COLORS = [YELLOW_CHART, ACCENT, GREEN_CHART, RED_CHART, PURPLE]

        if ter_sorted or cls_sorted:
            fig_ter = go.Figure()
            if cls_sorted:
                all_clases = sorted({cls for _, clases in cls_sorted for cls in clases})
                for i, cls in enumerate(all_clases):
                    xlabels_cls = [yr for yr, _ in cls_sorted]
                    yvals_cls = [clases.get(cls, 0) for _, clases in cls_sorted]
                    fig_ter.add_trace(go.Bar(
                        x=xlabels_cls, y=yvals_cls,
                        name=f"Clase {cls}",
                        marker_color=CLASE_COLORS[i % len(CLASE_COLORS)],
                        text=[f"{v:.2f}%" if v else "" for v in yvals_cls],
                        textposition="outside",
                        textfont=dict(size=10, color=TEXT3),
                        width=[0.35] * len(xlabels_cls),
                        hovertemplate=f"Clase {cls}: %{{y:.2f}}%<extra></extra>",
                    ))
                fig_ter.update_layout(barmode="group", **chart_layout(260))
            elif ter_sorted:
                xlabels  = [x[0] for x in ter_sorted]
                gest_y   = [x[1].get("coste_gestion_pct") or 0 for x in ter_sorted]
                dep_y    = [x[1].get("coste_deposito_pct") or 0 for x in ter_sorted]
                ter_y_raw = [x[1].get("ter_pct") or 0 for x in ter_sorted]
                dep_computed = [round(max(t - g, 0), 3) for t, g in zip(ter_y_raw, gest_y)]
                dep_final = [d if d > 0 else dep_y[i] for i, d in enumerate(dep_computed)]
                if any(v > 0 for v in gest_y):
                    fig_ter.add_trace(go.Bar(
                        x=xlabels, y=gest_y, name="Gestión %",
                        marker_color=ACCENT,
                        text=[f"{v:.2f}%" if v else "" for v in gest_y],
                        textposition="outside",
                        textfont=dict(size=10, color=TEXT3),
                        width=[0.5] * len(xlabels),
                        hovertemplate="Gestión: %{y:.3f}%<extra></extra>",
                    ))
                if any(v > 0 for v in dep_final):
                    fig_ter.add_trace(go.Bar(
                        x=xlabels, y=dep_final, name="Depósito / Otros %",
                        marker_color="#334155",
                        width=[0.5] * len(xlabels),
                        hovertemplate="Depósito: %{y:.3f}%<extra></extra>",
                    ))
                fig_ter.update_layout(barmode="stack", **chart_layout(260))
            fig_ter.update_xaxes(type="category")
            st.plotly_chart(fig_ter, use_container_width=True)
        else:
            if ter:
                st.metric("TER actual", pct(ter))
                st.metric("Comisión gestión", pct(gestion))
            else:
                empty_state("Sin datos de TER histórico.")

    # ── Mix activos ───────────────────────────────────────────────────────────
    section_header("Evolución por tipo de activo")
    mix_hist = cuant.get("mix_activos_historico", [])
    if mix_hist:
        yr_map_mix: dict[str, dict] = {}
        for m in mix_hist:
            yr = normalize_year(str(m.get("periodo", "")))
            if yr not in yr_map_mix:
                yr_map_mix[yr] = m
        mix_s = sorted(yr_map_mix.items())
        xlabels = [x[0] for x in mix_s]
        mix_rows = [x[1] for x in mix_s]
        mix_keys = list(MIX_LABELS.keys())
        norm_rows = []
        for m in mix_rows:
            total = sum(m.get(k, 0) or 0 for k in mix_keys)
            if total > 0:
                norm_rows.append({k: round((m.get(k, 0) or 0) / total * 100, 1) for k in mix_keys})
            else:
                norm_rows.append({k: 0 for k in mix_keys})
        fig_mix = go.Figure()
        for key, label in MIX_LABELS.items():
            vals = [r[key] for r in norm_rows]
            if any(v > 0 for v in vals):
                fig_mix.add_trace(go.Bar(
                    x=xlabels, y=vals, name=label,
                    marker_color=MIX_COLORS[key],
                    hovertemplate=f"<b>{label}</b>: %{{y:.1f}}%<extra></extra>",
                    text=[f"{v:.0f}%" if v >= 5 else "" for v in vals],
                    textposition="inside",
                ))
        ly_mix = chart_layout(260)
        ly_mix["yaxis"] = dict(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                               tickfont=dict(color=TEXT3, size=10),
                               range=[0, 100], ticksuffix="%")
        fig_mix.update_layout(barmode="stack", **ly_mix)
        st.plotly_chart(fig_mix, use_container_width=True)

    # ── Geografía ─────────────────────────────────────────────────────────────
    posiciones_actuales = pos_data.get("actuales", [])
    historicas_pos = pos_data.get("historicas", [])

    geo_series: list[tuple[str, dict]] = []
    for h in sorted(historicas_pos, key=lambda x: str(x.get("periodo", ""))):
        yr = normalize_year(str(h.get("periodo", "")))
        cw: dict[str, float] = {}
        for p in h.get("top10", []):
            c = infer_country(p)
            cw[c] = cw.get(c, 0) + (p.get("peso_pct") or 0)
        if cw:
            geo_series.append((yr, cw))
    if not geo_series and posiciones_actuales:
        cw: dict[str, float] = {}
        for p in posiciones_actuales:
            c = infer_country(p)
            cw[c] = cw.get(c, 0) + (p.get("peso_pct") or 0)
        if cw:
            geo_series.append(("Actual", cw))

    if geo_series:
        section_header("Distribución geográfica por país del emisor")
        all_countries: dict[str, float] = {}
        for _, cw in geo_series:
            for c, w in cw.items():
                all_countries[c] = all_countries.get(c, 0) + w
        top_countries = [c for c, _ in sorted(all_countries.items(), key=lambda x: x[1], reverse=True)[:8]]
        geo_norm = []
        for yr, cw in geo_series:
            total_w = sum(cw.values())
            geo_norm.append((yr, {c: round(w / total_w * 100, 1) for c, w in cw.items()} if total_w else cw))
        xlabels_geo = [yr for yr, _ in geo_norm]
        fig_geo = go.Figure()
        for i, country in enumerate(top_countries):
            vals = [cw.get(country, 0) for _, cw in geo_norm]
            fig_geo.add_trace(go.Bar(
                x=xlabels_geo, y=vals, name=country,
                marker_color=GEO_COLORS[i % len(GEO_COLORS)],
                text=[f"{v:.0f}%" if v >= 5 else "" for v in vals],
                textposition="inside",
                hovertemplate=f"<b>{country}</b>: %{{y:.1f}}%<extra></extra>",
            ))
        otros_vals = [sum(v for c, v in cw.items() if c not in top_countries) for _, cw in geo_norm]
        if any(v > 0 for v in otros_vals):
            fig_geo.add_trace(go.Bar(x=xlabels_geo, y=otros_vals, name="Otros",
                                     marker_color="#334155",
                                     text=[f"{v:.0f}%" if v >= 5 else "" for v in otros_vals],
                                     textposition="inside",
                                     hovertemplate="<b>Otros</b>: %{y:.1f}%<extra></extra>"))
        ly_geo = chart_layout(280)
        ly_geo["yaxis"] = dict(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                               tickfont=dict(color=TEXT3, size=10),
                               range=[0, 100], ticksuffix="%")
        fig_geo.update_layout(barmode="stack", **ly_geo)
        st.markdown(
            f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
            f" color:{TEXT3}; letter-spacing:0.06em; margin-bottom:4px;'>"
            f"PAÍS INFERIDO POR PREFIJO ISIN / NOMBRE EMISOR / DIVISA</div>",
            unsafe_allow_html=True)
        st.plotly_chart(fig_geo, use_container_width=True)

    # ── Divisa ────────────────────────────────────────────────────────────────
    divisa_series: list[tuple[str, dict]] = []
    for h in sorted(historicas_pos, key=lambda x: str(x.get("periodo", ""))):
        yr = normalize_year(str(h.get("periodo", "")))
        dw: dict[str, float] = {}
        for p in h.get("top10", []):
            div = str(p.get("divisa", "") or "").upper()
            if div:
                dw[div] = dw.get(div, 0) + (p.get("peso_pct") or 0)
        if dw:
            divisa_series.append((yr, dw))

    if divisa_series:
        section_header("Evolución de exposición por divisa")
        all_divisas: dict[str, float] = {}
        for _, dw in divisa_series:
            for d, w in dw.items():
                all_divisas[d] = all_divisas.get(d, 0) + w
        top_divisas = [d for d, _ in sorted(all_divisas.items(), key=lambda x: x[1], reverse=True)[:8]]
        div_norm = []
        for yr, dw in divisa_series:
            total_w = sum(dw.values())
            div_norm.append((yr, {d: round(w / total_w * 100, 1) for d, w in dw.items()} if total_w else dw))
        xlabels_div = [yr for yr, _ in div_norm]
        fig_div = go.Figure()
        for i, div in enumerate(top_divisas):
            vals = [dw.get(div, 0) for _, dw in div_norm]
            fig_div.add_trace(go.Bar(x=xlabels_div, y=vals, name=div,
                                     marker_color=GEO_COLORS[i % len(GEO_COLORS)],
                                     text=[f"{v:.0f}%" if v >= 5 else "" for v in vals],
                                     textposition="inside",
                                     hovertemplate=f"<b>{div}</b>: %{{y:.1f}}%<extra></extra>"))
        otros_div = [sum(v for d, v in dw.items() if d not in top_divisas) for _, dw in div_norm]
        if any(v > 0 for v in otros_div):
            fig_div.add_trace(go.Bar(x=xlabels_div, y=otros_div, name="Otras",
                                     marker_color="#334155",
                                     hovertemplate="<b>Otras</b>: %{y:.1f}%<extra></extra>"))
        ly_div = chart_layout(260)
        ly_div["yaxis"] = dict(showgrid=True, gridcolor=BORDER, gridwidth=0.5,
                               tickfont=dict(color=TEXT3, size=10),
                               range=[0, 100], ticksuffix="%")
        fig_div.update_layout(barmode="stack", **ly_div)
        st.plotly_chart(fig_div, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CARTERA
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    actuales   = pos_data.get("actuales", [])
    historicas = pos_data.get("historicas", [])
    actuales_sorted = sorted(actuales, key=lambda p: p.get("peso_pct") or 0, reverse=True)

    section_header("Posiciones actuales")

    if actuales_sorted:
        top25 = actuales_sorted[:25]
        max_peso = max((p.get("peso_pct", 0) or 0) for p in top25) or 1

        # Header row
        h1, h2, h3, h4, h5, h6 = st.columns([4, 1, 1, 1, 1, 2])
        for col, lbl in zip([h1,h2,h3,h4,h5,h6],
                            ["Nombre / ISIN","Tipo","País","Divisa","Valor M€","Peso %"]):
            col.markdown(
                f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                f" color:{TEXT3}; text-transform:uppercase; letter-spacing:0.08em;'>"
                f"{lbl}</div>", unsafe_allow_html=True)

        for pos in top25:
            tipo_p = pos.get("tipo", "") or ""
            color  = TIPO_COLOR.get(tipo_p, "#334155")
            peso   = pos.get("peso_pct", 0) or 0
            val_m  = (pos.get("valor_mercado_miles", 0) or 0) / 1000
            bar    = int(peso / max_peso * 100)
            pais_p = infer_country(pos)

            c1, c2, c3, c4, c5, c6 = st.columns([4, 1, 1, 1, 1, 2])
            with c1:
                vcto = f" · {pos['vencimiento']}" if pos.get("vencimiento") else ""
                st.markdown(
                    f"<div style='font-size:13px; font-weight:500; color:{TEXT};'>{pos.get('nombre','')}</div>"
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:9.5px; color:{TEXT3};'>"
                    f"{pos.get('ticker','')}{vcto}</div>",
                    unsafe_allow_html=True)
            with c2:
                st.markdown(
                    f"<span style='background:{color}22; color:{color}; border-radius:3px;"
                    f" padding:2px 6px; font-family:\"DM Mono\",monospace; font-size:9px;"
                    f" letter-spacing:0.04em;'>{tipo_p}</span>",
                    unsafe_allow_html=True)
            with c3:
                st.markdown(f"<span style='font-size:11px; color:{TEXT2};'>{pais_p}</span>",
                            unsafe_allow_html=True)
            with c4:
                st.markdown(f"<span style='font-family:\"DM Mono\",monospace; font-size:11px;"
                            f" color:{TEXT2};'>{pos.get('divisa','')}</span>",
                            unsafe_allow_html=True)
            with c5:
                st.markdown(f"<span style='font-family:\"DM Mono\",monospace; font-size:12px;"
                            f" color:{TEXT2};'>{es(val_m,2)}</span>",
                            unsafe_allow_html=True)
            with c6:
                st.markdown(
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:13px;"
                    f" font-weight:500; color:{ACCENT};'>{es(peso,2)}%</div>"
                    f"<div class='pos-bar-bg'><div class='pos-bar-fill' style='width:{bar}%'></div></div>",
                    unsafe_allow_html=True)
            st.markdown(f"<div style='height:1px; background:{BORDER}; margin:3px 0;'></div>",
                        unsafe_allow_html=True)

        if len(actuales) > 25:
            st.markdown(
                f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                f" color:{TEXT3}; letter-spacing:0.06em; margin-top:4px;'>"
                f"+ {len(actuales)-25} POSICIONES ADICIONALES</div>",
                unsafe_allow_html=True)

    # ── Concentración top-15 ──────────────────────────────────────────────────
    if len(historicas) >= 2:
        section_header("Concentración top-15 posiciones por año")
        hist_s = sorted(historicas, key=lambda h: str(h.get("periodo", "")))
        top15_data = []
        for h in hist_s:
            yr = normalize_year(str(h.get("periodo", "")))
            sorted_top = sorted(h.get("top10", []), key=lambda p: p.get("peso_pct") or 0, reverse=True)[:15]
            total_w = sum(p.get("peso_pct") or 0 for p in sorted_top)
            top5 = [(p.get("nombre", "")[:18], p.get("peso_pct") or 0) for p in sorted_top[:5]]
            top15_data.append({"yr": yr, "total_w": round(total_w, 1), "top5": top5})
        if top15_data:
            fig_evol = go.Figure(go.Bar(
                x=[d["yr"] for d in top15_data],
                y=[d["total_w"] for d in top15_data],
                marker_color=ACCENT,
                hovertemplate="<b>%{x}</b><br>Top-15 peso: %{y:.1f}%<extra></extra>",
            ))
            fig_evol.update_layout(**chart_layout(200, legend=False))
            st.plotly_chart(fig_evol, use_container_width=True)
            cols_t5 = st.columns(min(len(top15_data), 6))
            for i, data in enumerate(top15_data[-6:]):
                with cols_t5[i % len(cols_t5)]:
                    top5_html = "".join(
                        f"<div style='font-family:\"DM Mono\",monospace; font-size:10px;"
                        f" color:{TEXT2}; padding:2px 0;'>"
                        f"<span style='color:{TEXT3};'>{j+1}.</span> {nm} "
                        f"<span style='color:{ACCENT}; font-weight:500;'>{es(w,1)}%</span></div>"
                        for j, (nm, w) in enumerate(data["top5"])
                    )
                    st.markdown(
                        f"<div style='background:{BG2}; border:1px solid {BORDER}; border-radius:4px;"
                        f" padding:10px 12px;'>"
                        f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                        f" font-weight:500; color:{TEXT3}; letter-spacing:0.08em;"
                        f" text-transform:uppercase; margin-bottom:6px;'>{data['yr']}</div>"
                        f"{top5_html}</div>",
                        unsafe_allow_html=True)

    # ── Cambios de cartera año a año ──────────────────────────────────────────
    if len(historicas) >= 2:
        section_header("Cambios de cartera — año a año")

        def compute_changes(hist):
            srt = sorted(hist, key=lambda h: str(h.get("periodo", "")))
            changes = []
            for i in range(1, len(srt)):
                prev_m = {(p.get("ticker") or p.get("nombre","")): p for p in srt[i-1].get("top10",[])}
                curr_m = {(p.get("ticker") or p.get("nombre","")): p for p in srt[i].get("top10",[])}
                entradas = [p for k, p in curr_m.items() if k not in prev_m and (p.get("peso_pct") or 0) >= 1]
                salidas  = [p for k, p in prev_m.items() if k not in curr_m and (p.get("peso_pct") or 0) >= 1]
                dchanges = []
                for k, p in curr_m.items():
                    if k in prev_m:
                        delta = (p.get("peso_pct") or 0) - (prev_m[k].get("peso_pct") or 0)
                        if abs(delta) >= 0.5:
                            dchanges.append({**p, "delta": round(delta, 2)})
                dchanges.sort(key=lambda x: abs(x["delta"]), reverse=True)
                changes.append({
                    "de": srt[i-1]["periodo"], "a": srt[i]["periodo"],
                    "entradas": entradas, "salidas": salidas, "cambios": dchanges[:6],
                })
            return list(reversed(changes))

        def _pos_row(p, border_col, value_col, delta=None):
            sign = f"+{es(delta,2)}%" if delta and delta > 0 else (f"{es(delta,2)}%" if delta else "")
            delta_html = (f"<span style='color:{value_col}; font-size:10px;'> {sign}</span>"
                          if sign else "")
            return (
                f"<div style='background:{BG2}; border-left:2px solid {border_col};"
                f" border-radius:0 3px 3px 0; padding:5px 9px; margin-bottom:4px; font-size:11.5px;'>"
                f"<span style='color:{TEXT}; font-weight:500;'>{p.get('nombre','')[:28]}</span>"
                f"<br><span style='font-family:\"DM Mono\",monospace; font-size:9.5px; color:{TEXT3};'>"
                f"{p.get('ticker','')}</span>"
                f"&nbsp;<span style='color:{border_col}; font-weight:500; font-size:11px;'>"
                f"{es(p.get('peso_pct',0),2)}%</span>"
                f"{delta_html}</div>"
            )

        for ch in compute_changes(historicas):
            n = len(ch["entradas"]) + len(ch["salidas"]) + len(ch["cambios"])
            if n == 0:
                continue
            with st.expander(f"{ch['de']} → {ch['a']}  ·  {n} cambio{'s' if n>1 else ''}"):
                cc1, cc2, cc3 = st.columns(3)
                with cc1:
                    st.markdown(
                        f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                        f" color:{GREEN}; letter-spacing:0.08em; margin-bottom:8px;'>ENTRADAS</div>",
                        unsafe_allow_html=True)
                    for p in ch["entradas"]:
                        st.markdown(_pos_row(p, GREEN, GREEN), unsafe_allow_html=True)
                    if not ch["entradas"]:
                        st.markdown(f"<span style='color:{TEXT3}; font-size:11px;'>—</span>",
                                    unsafe_allow_html=True)
                with cc2:
                    st.markdown(
                        f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                        f" color:{RED}; letter-spacing:0.08em; margin-bottom:8px;'>SALIDAS</div>",
                        unsafe_allow_html=True)
                    for p in ch["salidas"]:
                        st.markdown(_pos_row(p, RED, RED), unsafe_allow_html=True)
                    if not ch["salidas"]:
                        st.markdown(f"<span style='color:{TEXT3}; font-size:11px;'>—</span>",
                                    unsafe_allow_html=True)
                with cc3:
                    st.markdown(
                        f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                        f" color:{ACCENT}; letter-spacing:0.08em; margin-bottom:8px;'>CAMBIOS PESO</div>",
                        unsafe_allow_html=True)
                    for p in ch["cambios"]:
                        d_val = p["delta"]
                        col_d = GREEN if d_val > 0 else RED
                        st.markdown(_pos_row(p, col_d, col_d, delta=d_val), unsafe_allow_html=True)
                    if not ch["cambios"]:
                        st.markdown(f"<span style='color:{TEXT3}; font-size:11px;'>—</span>",
                                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — CONSISTENCIA
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    resumen_g = consist.get("resumen_global", "")
    if resumen_g:
        section_header("Síntesis del track record")
        narrative_block(resumen_g, "SÍNTESIS GLOBAL")

    # Cartas trimestrales
    cartas_list = (letters_d.get("cartas", []) if isinstance(letters_d, dict) else []) or []
    if cartas_list:
        section_header("Cartas trimestrales")
        for carta in cartas_list[:8]:
            fecha_c   = carta.get("fecha", carta.get("date", ""))
            titulo_c  = carta.get("titulo", carta.get("title", ""))
            resumen_c = carta.get("resumen", carta.get("summary", ""))
            with st.expander(f"{fecha_c}  {titulo_c}"):
                if resumen_c:
                    st.markdown(
                        f"<div style='font-size:12.5px; color:{TEXT2}; line-height:1.75;'>"
                        f"{resumen_c}</div>", unsafe_allow_html=True)
    else:
        section_header("Cartas trimestrales")
        empty_state(
            "No se encontraron cartas trimestrales.",
            f"python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto")

    # Periodos — ascending, oldest first
    if periodos_asc:
        section_header("Análisis de consistencia por periodo",
                       subtitle=f"{periodos_asc[0].get('periodo','')[:4]} → {periodos_asc[-1].get('periodo','')[:4]}")

        for pdata in periodos_asc:
            periodo_lbl = pdata.get("periodo", "—")
            score       = pdata.get("consistencia_score")
            tesis       = pdata.get("tesis_gestora", "") or ""
            contexto    = pdata.get("contexto_mercado", "") or ""
            decisiones  = pdata.get("decisiones_tomadas", "") or ""
            resultado   = pdata.get("resultado_real", "") or ""

            def _period_content(t=tesis, ctx=contexto, dec=decisiones, res=resultado):
                p1, p2 = st.columns(2)
                with p1:
                    if ctx:
                        st.markdown(
                            f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                            f" color:{TEXT3}; letter-spacing:0.08em; text-transform:uppercase;"
                            f" margin-bottom:6px;'>Contexto de mercado</div>",
                            unsafe_allow_html=True)
                        st.markdown(f"<div class='sbox'>{ctx[:800]}</div>",
                                    unsafe_allow_html=True)
                with p2:
                    if t:
                        st.markdown(
                            f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                            f" color:{TEXT3}; letter-spacing:0.08em; text-transform:uppercase;"
                            f" margin-bottom:6px;'>Tesis gestora</div>",
                            unsafe_allow_html=True)
                        st.markdown(f"<div class='sbox'>{t[:600]}</div>",
                                    unsafe_allow_html=True)
                    if dec:
                        st.markdown(
                            f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                            f" color:{GREEN}; letter-spacing:0.08em; text-transform:uppercase;"
                            f" margin:10px 0 6px 0;'>Decisiones</div>",
                            unsafe_allow_html=True)
                        st.markdown(f"<div class='sbox' style='max-height:120px;'>{dec}</div>",
                                    unsafe_allow_html=True)
                    if res:
                        st.markdown(
                            f"<div style='font-family:\"DM Mono\",monospace; font-size:9px;"
                            f" color:{YELLOW}; letter-spacing:0.08em; text-transform:uppercase;"
                            f" margin:10px 0 6px 0;'>Resultado real</div>",
                            unsafe_allow_html=True)
                        st.markdown(f"<div class='sbox' style='max-height:100px;'>{res}</div>",
                                    unsafe_allow_html=True)

            consistency_period(periodo_lbl, score, _period_content)

    elif not resumen_g:
        empty_state("Sin datos de consistencia — ejecuta el pipeline con ANTHROPIC_API_KEY.",
                    f"python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — LECTURAS
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    section_header("Lecturas, vídeos y entrevistas")

    if isinstance(lecturas_d, dict):
        lecturas_list = lecturas_d.get("lecturas", [])
    elif isinstance(lecturas_d, list):
        lecturas_list = lecturas_d
    else:
        lecturas_list = []

    TIPO_ICON = {
        "articulo":"ART","video":"VID","entrevista":"ENT",
        "podcast":"POD","perfil_gestor":"PRF","otro":"LNK",
    }

    if lecturas_list:
        for item in lecturas_list:
            tipo_l = item.get("tipo", "otro")
            tag    = TIPO_ICON.get(tipo_l, "LNK")
            url    = item.get("url", "#")
            titulo = item.get("titulo", url)
            desc   = item.get("descripcion", "") or item.get("snippet", "")
            fecha  = item.get("fecha", "")
            fuente = item.get("fuente", "") or item.get("source", "")

            date_html = (f"<span style='font-family:\"DM Mono\",monospace; font-size:9px;"
                         f" color:{TEXT3};'>{fecha}</span>  " if fecha else "")
            src_html  = (f"<span style='font-family:\"DM Mono\",monospace; font-size:9px;"
                         f" color:{TEXT3}; letter-spacing:0.04em;'>{fuente}</span>" if fuente else "")
            desc_html = (f"<div style='font-size:12px; color:{TEXT3}; line-height:1.7;"
                         f" margin-top:8px;'>{desc}</div>" if desc else "")
            st.markdown(f"""
            <div style="background:{BG2}; border:1px solid {BORDER}; border-radius:4px;
                        padding:14px 18px; margin-bottom:6px;">
              <div style="display:flex; align-items:baseline; gap:10px; margin-bottom:6px;">
                <span style="font-family:'DM Mono',monospace; font-size:9px;
                             letter-spacing:0.1em; color:{ACCENT}; background:#0d0f14;
                             border:1px solid {BORDER}; border-radius:3px;
                             padding:2px 6px;">{tag}</span>
                {date_html}{src_html}
              </div>
              <a href="{url}" target="_blank"
                 style="font-size:13.5px; font-weight:600; color:{TEXT};">{titulo}</a>
              {desc_html}
            </div>""", unsafe_allow_html=True)
    else:
        nombre_fondo = d.get("nombre", "")
        gestores_names = [g.get("nombre","") for g in cual.get("gestores",[]) if g.get("nombre")]
        empty_state(
            "Sin lecturas disponibles todavía — ejecuta el pipeline.",
            f"python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto")
        if gestores_names:
            section_header("Búsquedas sugeridas")
            queries = (
                [f'"{nombre_fondo}" entrevista', f'"{nombre_fondo}" carta gestores'] +
                [f'"{n}" gestor fondo inversión' for n in gestores_names[:2]]
            )
            for q in queries:
                enc = urllib.parse.quote(q)
                st.markdown(
                    f"<div style='background:{BG2}; border:1px solid {BORDER}; border-radius:3px;"
                    f" padding:8px 14px; margin-bottom:4px; font-size:12px;'>"
                    f"<a href='https://www.google.com/search?q={enc}' target='_blank'"
                    f" style='color:{ACCENT};'>{q}</a></div>",
                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — ANÁLISIS EXTERNOS
# ══════════════════════════════════════════════════════════════════════════════
with tab7:
    section_header("Análisis publicados por webs especializadas")

    if isinstance(analisis_ext, dict):
        ext_list = analisis_ext.get("analisis_externos", [])
    elif isinstance(analisis_ext, list):
        ext_list = analisis_ext
    else:
        ext_list = []

    SEARCH_DOMAINS = ("google.com","duckduckgo.com","bing.com","yahoo.com")
    ext_real = [it for it in ext_list
                if not any(d in (it.get("url","") or "") for d in SEARCH_DOMAINS)]

    SOURCE_ICON = {
        "saludfinanciera":"SF","astralis":"AS","morningstar":"MS",
        "rankia":"RK","finect":"FN","investing":"IN","expansión":"EX",
    }

    if ext_real:
        for item in ext_real:
            fuente    = item.get("fuente","") or item.get("source","")
            titulo    = item.get("titulo", item.get("title","Sin título"))
            url       = item.get("url","#")
            fecha     = item.get("fecha","")
            resumen_e = (item.get("resumen_generado","") or
                         item.get("resumen","") or item.get("snippet",""))
            palabras  = item.get("palabras_estimadas","")

            tag = next((v for k, v in SOURCE_ICON.items() if k in fuente.lower()), "EXT")
            date_html = (f"<span style='font-family:\"DM Mono\",monospace; font-size:9px;"
                         f" color:{TEXT3};'>{fecha}</span>" if fecha else "")
            words_html = (f"<span style='font-family:\"DM Mono\",monospace; font-size:9px;"
                          f" color:{TEXT3};'> · ~{palabras}w</span>" if palabras else "")
            res_html = (f"<div style='font-size:12.5px; color:{TEXT2}; line-height:1.75;"
                        f" margin-top:10px; border-top:1px solid {BORDER}; padding-top:10px;'>"
                        f"{resumen_e}</div>" if resumen_e else "")
            st.markdown(f"""
            <div style="background:{BG2}; border:1px solid {BORDER}; border-radius:4px;
                        padding:16px 20px; margin-bottom:8px;">
              <div style="display:flex; align-items:baseline; gap:10px; margin-bottom:8px;">
                <span style="font-family:'DM Mono',monospace; font-size:9px; letter-spacing:0.1em;
                             color:{ACCENT}; background:#0d0f14; border:1px solid {BORDER};
                             border-radius:3px; padding:2px 6px;">{tag}</span>
                <span style="font-family:'DM Mono',monospace; font-size:9.5px;
                             color:{TEXT3}; letter-spacing:0.04em;">{fuente}</span>
                {date_html}{words_html}
              </div>
              <a href="{url}" target="_blank"
                 style="font-size:14px; font-weight:600; color:{TEXT}; line-height:1.4;">
                {titulo}
              </a>
              {res_html}
            </div>""", unsafe_allow_html=True)

    elif ext_list and not ext_real:
        empty_state("Los resultados encontrados son URLs de búsqueda — re-ejecuta el pipeline para artículos directos.")
    else:
        nombre_fondo = d.get("nombre", "")
        SOURCES = [
            ("Salud Financiera","saludfinanciera.es"),
            ("Astralis","astralis.es"),
            ("Morningstar","morningstar.es"),
            ("Rankia","rankia.com"),
            ("Finect","finect.com"),
        ]
        empty_state(
            "Sin análisis externos disponibles.",
            f"python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto")

        section_header("Buscar manualmente", subtitle="acceso directo")
        cols_src = st.columns(len(SOURCES))
        for i, (name, site) in enumerate(SOURCES):
            q = urllib.parse.quote(f'"{nombre_fondo}"')
            with cols_src[i]:
                st.markdown(
                    f"<div style='text-align:center; background:{BG2}; border:1px solid {BORDER};"
                    f" border-radius:4px; padding:14px 10px;'>"
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:10px;"
                    f" font-weight:500; color:{TEXT2}; letter-spacing:0.06em;'>{name}</div>"
                    f"<div style='margin-top:6px;'>"
                    f"<a href='https://www.google.com/search?q=site:{site}+{q}' target='_blank'"
                    f" style='font-family:\"DM Mono\",monospace; font-size:9px;"
                    f" color:{ACCENT};'>Buscar →</a></div></div>",
                    unsafe_allow_html=True)

    if meta_report and meta_report.get("issues"):
        section_header("Issues del meta-agente", accent_color=YELLOW_CHART)
        for issue in meta_report["issues"]:
            st.markdown(
                f"<div style='background:{BG2}; border-left:2px solid {YELLOW_CHART};"
                f" border-radius:0 4px 4px 0; padding:6px 14px; margin-bottom:4px;"
                f" font-size:12px; color:{TEXT2};'>{issue}</div>",
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 8 — ARCHIVOS
# ══════════════════════════════════════════════════════════════════════════════
with tab8:
    section_header("Archivos y fuentes analizadas")

    xmls = fuentes.get("xmls_cnmv", [])
    pdfs = fuentes.get("informes_descargados", [])
    urls = fuentes.get("urls_consultadas", [])

    # Metadata table
    lect_count = (len(lecturas_d.get("lecturas", [])) if isinstance(lecturas_d, dict)
                  else len(lecturas_d) if isinstance(lecturas_d, list) else 0)
    ext_count  = len(ext_list) if "ext_list" in dir() else 0

    meta_rows = [
        ("ISIN",                   st.session_state.selected_isin),
        ("Tipo",                   d.get("tipo","—")),
        ("Última actualización",   str(d.get("ultima_actualizacion") or "—")[:19].replace("T"," ")),
        ("XMLs procesados",        str(len(xmls))),
        ("PDFs procesados",        str(len(pdfs))),
        ("Periodos consistencia",  str(len(periodos_asc))),
        ("Lecturas",               str(lect_count)),
        ("Análisis externos",      str(ext_count)),
    ]
    st.markdown(f"<div style='background:{BG2}; border:1px solid {BORDER}; border-radius:4px;"
                f" padding:14px 18px; margin-bottom:16px;'>", unsafe_allow_html=True)
    for label, val in meta_rows:
        stat_row(label, val)
    st.markdown("</div>", unsafe_allow_html=True)

    col_f1, col_f2 = st.columns(2)

    with col_f1:
        if pdfs:
            section_header(f"Informes semestrales CNMV", subtitle=str(len(pdfs)))
            for pdf in sorted(pdfs):
                st.markdown(
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:10px;"
                    f" color:{TEXT2}; padding:3px 0; border-bottom:1px solid {BORDER};'>"
                    f"{pdf}</div>", unsafe_allow_html=True)
        if urls:
            section_header("URLs consultadas")
            for url in urls:
                short = url[:72] + "…" if len(url) > 72 else url
                st.markdown(
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:9.5px;"
                    f" padding:3px 0;'>"
                    f"<a href='{url}' target='_blank' style='color:{ACCENT};'>{short}</a>"
                    f"</div>", unsafe_allow_html=True)

    with col_f2:
        if xmls:
            section_header(f"XMLs CNMV bulk data", subtitle=str(len(xmls)))
            for xml in sorted(xmls):
                st.markdown(
                    f"<div style='font-family:\"DM Mono\",monospace; font-size:10px;"
                    f" color:{TEXT2}; padding:2px 0;'>{xml}</div>",
                    unsafe_allow_html=True)
