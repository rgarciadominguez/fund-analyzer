"""
Fund Analyzer — Streamlit Dashboard v3
"""
import hashlib
import json
import re
import urllib.parse
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fund Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state defaults ────────────────────────────────────────────────────
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False
if "selected_isin" not in st.session_state:
    st.session_state.selected_isin = None

# ── Theme variables ───────────────────────────────────────────────────────────
DK = st.session_state.dark_mode

BG          = "#0f1117" if DK else "#f5f7fa"
BG2         = "#161b27" if DK else "#ffffff"
BG3         = "#1c2333" if DK else "#f0f4f8"
BORDER      = "#2d3748" if DK else "#dde3ed"
TEXT        = "#e0e0e0" if DK else "#1a202c"
TEXT2       = "#8892a4" if DK else "#64748b"
TEXT3       = "#6b7a99" if DK else "#94a3b8"
ACCENT      = "#3b82f6"
GREEN       = "#10b981"
RED         = "#f87171"
YELLOW      = "#f59e0b"
PURPLE      = "#8b5cf6"
HEADER_BG   = "#1a2540" if DK else "#1e3a5f"

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  .stApp {{ background-color: {BG}; color: {TEXT}; }}
  section[data-testid="stSidebar"] {{ display: none; }}
  header[data-testid="stHeader"] {{ display: none; }}
  .block-container {{ padding: 1rem 2rem 2rem 2rem; max-width: 1400px; }}

  .stTabs [data-baseweb="tab-list"] {{
    background: {BG2}; border-radius: 10px; padding: 4px;
    border: 1px solid {BORDER}; gap: 2px;
  }}
  .stTabs [data-baseweb="tab"] {{
    background: transparent; color: {TEXT2}; border-radius: 8px;
    font-size: 13px; font-weight: 500; padding: 6px 14px; border: none;
  }}
  .stTabs [aria-selected="true"] {{ background: {ACCENT} !important; color: white !important; }}
  .stTabs [data-baseweb="tab-panel"] {{ padding-top: 16px; }}

  .card {{
    background: {BG2}; border: 1px solid {BORDER};
    border-radius: 10px; padding: 16px 20px;
  }}
  .kpi-card {{
    background: {BG2}; border: 1px solid {BORDER};
    border-radius: 10px; padding: 14px 18px; text-align: center;
  }}
  .kpi-label {{ font-size: 10px; color: {TEXT2}; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px; }}
  .kpi-value {{ font-size: 22px; font-weight: 700; color: {TEXT}; line-height: 1.1; }}
  .kpi-sub   {{ font-size: 11px; color: {TEXT3}; margin-top: 3px; }}

  .sec {{ font-size: 12px; font-weight: 700; color: {TEXT2}; text-transform: uppercase;
          letter-spacing: 1px; border-bottom: 1px solid {BORDER};
          padding-bottom: 6px; margin: 20px 0 12px 0; }}

  .tl-wrap {{ position: relative; padding-left: 28px; }}
  .tl-line {{ position: absolute; left: 7px; top: 0; bottom: 0; width: 2px; background: {BORDER}; }}
  .tl-item {{ position: relative; margin-bottom: 16px; }}
  .tl-dot {{ width: 10px; height: 10px; background: {ACCENT}; border-radius: 50%;
              position: absolute; left: -24px; top: 4px; border: 2px solid {BG2}; }}
  .tl-dot-reg {{ background: {YELLOW}; }}
  .tl-year {{ display: inline-block; background: {ACCENT}; color: #fff;
              border-radius: 12px; padding: 1px 10px; font-size: 11px;
              font-weight: 700; margin-bottom: 4px; }}
  .tl-year-reg {{ background: {YELLOW}; color: #1a202c; }}
  .tl-text {{ font-size: 13px; color: {TEXT}; line-height: 1.6; }}
  .tl-label {{ font-size: 10px; color: {TEXT3}; text-transform: uppercase; margin-top: 3px; }}

  .sbox {{
    background: {"#111827" if DK else "#f8fafc"};
    border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 14px;
    font-size: 13px; color: {TEXT}; line-height: 1.7;
    max-height: 220px; overflow-y: auto;
  }}

  .pos-row {{ padding: 7px 0; border-bottom: 1px solid {BORDER}; font-size: 13px; }}

  .streamlit-expanderHeader {{
    background: {BG3} !important; border-radius: 8px !important;
    font-size: 13px !important; color: {TEXT} !important;
  }}
  div[data-testid="stExpander"] > details > summary {{
    background: {BG3}; border-radius: 8px; padding: 8px 14px;
  }}

  .stSelectbox > div > div, .stTextInput > div > div > input {{
    background: {BG2} !important; color: {TEXT} !important;
    border-color: {BORDER} !important; font-size: 13px !important;
  }}

  .modebar {{ display: none !important; }}
  hr {{ border-color: {BORDER}; opacity: 0.4; }}
  .stDataFrame {{ background: {BG2}; }}

  .link-pill {{
    display: inline-block; background: {BG3}; border: 1px solid {BORDER};
    border-radius: 20px; padding: 3px 12px; font-size: 11px; font-weight: 600;
    color: {ACCENT}; text-decoration: none; margin-right: 6px; margin-top: 4px;
  }}
  .link-pill:hover {{ background: {ACCENT}; color: white; }}

  .ext-card {{
    background: {BG2}; border: 1px solid {BORDER}; border-radius: 10px;
    padding: 14px 18px; margin-bottom: 10px;
  }}
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_output(isin: str) -> dict:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def load_json(isin: str, filename: str) -> dict | list:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / filename
    if not p.exists():
        return {} if filename.endswith(".json") and "list" not in filename else []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


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
            result.append({"isin": d.name, "nombre": data.get("nombre", d.name)})
        except Exception:
            result.append({"isin": d.name, "nombre": d.name})
    return sorted(result, key=lambda x: x["nombre"])


def es(v, dec=2, suffix="") -> str:
    """Formato español: 1.234,56"""
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
    """'2022-S2' → '2022', '202506' → '2025', '2025-H2' → '2025'"""
    m = re.match(r"^(20\d{2})", str(periodo))
    return m.group(1) if m else str(periodo)


def chart_layout(height=280, legend=True) -> dict:
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT2, size=12),
        height=height,
        margin=dict(l=0, r=0, t=10, b=40),
        showlegend=legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            bgcolor="rgba(0,0,0,0)", font=dict(size=11, color=TEXT2),
        ),
        xaxis=dict(showgrid=False, tickfont=dict(color=TEXT2, size=11),
                   linecolor=BORDER, tickangle=-30),
        yaxis=dict(showgrid=True, gridcolor=BORDER, tickfont=dict(color=TEXT2)),
    )


# ── Country inference ─────────────────────────────────────────────────────────

ISIN_COUNTRY_MAP = {
    "ES": "España", "DE": "Alemania", "FR": "Francia", "IT": "Italia",
    "NL": "Países Bajos", "GB": "Reino Unido", "US": "EE.UU.", "JP": "Japón",
    "CH": "Suiza", "AU": "Australia", "CA": "Canadá", "SE": "Suecia",
    "NO": "Noruega", "DK": "Dinamarca", "FI": "Finlandia", "PT": "Portugal",
    "BE": "Bélgica", "AT": "Austria", "IE": "Irlanda", "MX": "México",
    "BR": "Brasil", "CN": "China", "IN": "India", "KR": "Corea del Sur",
    "TW": "Taiwan", "HK": "Hong Kong", "SG": "Singapur", "ZA": "Sudáfrica",
    "PL": "Polonia", "CZ": "Rep. Checa", "HU": "Hungría", "RO": "Rumanía",
    "TR": "Turquía", "RU": "Rusia", "AE": "EAU", "SA": "Arabia Saudí",
    "LU": "Luxemburgo", "XS": "Internacional", "XF": "Internacional",
    "GR": "Grecia", "SK": "Eslovaquia", "SI": "Eslovenia", "HR": "Croacia",
    "CL": "Chile", "CO": "Colombia", "PE": "Perú", "AR": "Argentina",
    "IL": "Israel", "TH": "Tailandia", "ID": "Indonesia", "MY": "Malasia",
    "PH": "Filipinas", "VN": "Vietnam", "EG": "Egipto", "MA": "Marruecos",
    "NG": "Nigeria", "KE": "Kenia",
}

NAME_COUNTRY_MAP = {
    "SPAIN": "España", "ESPAÑA": "España", "REINO DE ESPAÑA": "España",
    "GERMANY": "Alemania", "BUNDESREPUBLIK": "Alemania", "DEUTSCHLAND": "Alemania",
    "FRANCE": "Francia", "REPUBLIQUE FRANCAISE": "Francia", "FRENCH": "Francia",
    "ITALY": "Italia", "ITALIA": "Italia", "REPUBBLICA ITALIANA": "Italia", "ITALIAN": "Italia",
    "NETHERLANDS": "Países Bajos", "NEDERLAND": "Países Bajos", "DUTCH": "Países Bajos",
    "UNITED KINGDOM": "Reino Unido", "UK GILT": "Reino Unido", "GILT": "Reino Unido",
    "BRITISH": "Reino Unido", "ENGLAND": "Reino Unido",
    "UNITED STATES": "EE.UU.", "US TREASURY": "EE.UU.", "U.S. TREASURY": "EE.UU.",
    "AMERICAN": "EE.UU.", "USA": "EE.UU.",
    "JAPAN": "Japón", "JAPANESE": "Japón", "NIPPON": "Japón",
    "SWITZERLAND": "Suiza", "SWISS": "Suiza", "EIDGENOSSENSCHAFT": "Suiza",
    "CANADA": "Canadá", "CANADIAN": "Canadá",
    "AUSTRALIA": "Australia", "AUSTRALIAN": "Australia",
    "SWEDEN": "Suecia", "SVENSKA": "Suecia", "SWEDISH": "Suecia",
    "NORWAY": "Noruega", "NORGES": "Noruega", "NORWEGIAN": "Noruega",
    "DENMARK": "Dinamarca", "DANISH": "Dinamarca",
    "FINLAND": "Finlandia", "FINNISH": "Finlandia",
    "PORTUGAL": "Portugal", "PORTUGUESE": "Portugal",
    "BELGIUM": "Bélgica", "BELGIQUE": "Bélgica", "BELGIAN": "Bélgica",
    "AUSTRIA": "Austria", "AUSTRIAN": "Austria",
    "IRELAND": "Irlanda", "IRISH": "Irlanda",
    "MEXICO": "México", "MEXICAN": "México",
    "BRAZIL": "Brasil", "BRASIL": "Brasil", "BRAZILIAN": "Brasil",
    "CHINA": "China", "CHINESE": "China", "PEOPLES REPUBLIC": "China",
    "INDIA": "India", "INDIAN": "India",
    "KOREA": "Corea del Sur", "REPUBLIC OF KOREA": "Corea del Sur", "KOREAN": "Corea del Sur",
    "TAIWAN": "Taiwan",
    "HONG KONG": "Hong Kong",
    "SINGAPORE": "Singapur",
    "SOUTH AFRICA": "Sudáfrica",
    "POLAND": "Polonia", "POLSKA": "Polonia", "POLISH": "Polonia",
    "CZECH": "Rep. Checa",
    "HUNGARY": "Hungría", "HUNGARIAN": "Hungría",
    "TURKEY": "Turquía", "TURKIYE": "Turquía", "TURKISH": "Turquía",
    "RUSSIA": "Rusia", "RUSSIAN": "Rusia",
    "GREECE": "Grecia", "GREEK": "Grecia", "HELLENIC": "Grecia",
    "CHILE": "Chile", "CHILEAN": "Chile",
    "COLOMBIA": "Colombia", "COLOMBIAN": "Colombia",
    "PERU": "Perú", "PERUVIAN": "Perú",
    "ARGENTINA": "Argentina", "ARGENTINE": "Argentina",
    "ISRAEL": "Israel", "ISRAELI": "Israel",
}

DIVISA_REGION = {
    "EUR": "Eurozona", "USD": "EE.UU.", "GBP": "Reino Unido",
    "JPY": "Japón", "CHF": "Suiza", "SEK": "Suecia", "NOK": "Noruega",
    "DKK": "Dinamarca", "AUD": "Australia", "CAD": "Canadá",
    "HKD": "Hong Kong", "SGD": "Singapur", "CNY": "China", "CNH": "China",
    "BRL": "Brasil", "MXN": "México", "INR": "India",
    "KRW": "Corea del Sur", "TWD": "Taiwan", "ZAR": "Sudáfrica",
    "PLN": "Polonia", "CZK": "Rep. Checa", "HUF": "Hungría",
    "TRY": "Turquía", "RUB": "Rusia",
}

GEO_COLORS = [
    "#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#06b6d4", "#f97316", "#84cc16", "#ec4899", "#6b7280",
]


def infer_country(pos: dict) -> str:
    ticker = str(pos.get("ticker", "") or "").upper().strip()
    nombre = str(pos.get("nombre", "") or "").upper()
    tipo   = str(pos.get("tipo", "") or "").upper()

    # 1. ISIN prefix del ticker (más fiable: indica país emisor)
    if len(ticker) >= 2 and ticker[:2].isalpha():
        cc = ticker[:2]
        if cc in ISIN_COUNTRY_MAP:
            c = ISIN_COUNTRY_MAP[cc]
            # Evitar LU para fondos que invierten internacionalmente
            if c != "Luxemburgo":
                return c

    # 2. Nombre del instrumento → keywords de país
    for kw, country in NAME_COUNTRY_MAP.items():
        if kw in nombre:
            return country

    # 3. ETF/IIC/ETC → divisa como proxy regional
    if tipo in ("IIC", "PARTICIPACIONES", "ETC", "ETF"):
        div = str(pos.get("divisa", "") or "")
        return DIVISA_REGION.get(div, "Internacional")

    # 4. Divisa como último recurso
    div = str(pos.get("divisa", "") or "")
    if div in DIVISA_REGION:
        return DIVISA_REGION[div]

    return "Otros"


def manager_slug(name: str) -> str:
    """'Juan García López' → 'juan-garcia-lopez'"""
    s = name.lower().strip()
    s = re.sub(r"[áàä]", "a", s)
    s = re.sub(r"[éèë]", "e", s)
    s = re.sub(r"[íìï]", "i", s)
    s = re.sub(r"[óòö]", "o", s)
    s = re.sub(r"[úùü]", "u", s)
    s = re.sub(r"[ñ]", "n", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


TIPO_COLOR = {
    "REPO": YELLOW, "BONO": ACCENT, "IIC": GREEN,
    "PARTICIPACIONES": GREEN, "PAGARE": PURPLE,
    "OBLIGACION": "#60a5fa", "RENTA FIJA": ACCENT, "ETC": "#34d399",
}
MIX_COLORS = {
    "renta_fija_pct": ACCENT, "rv_pct": GREEN,
    "iic_pct": PURPLE, "liquidez_pct": YELLOW, "depositos_pct": "#6b7280",
}
MIX_LABELS = {
    "renta_fija_pct": "Renta Fija", "rv_pct": "Renta Variable",
    "iic_pct": "IIC / ETF", "liquidez_pct": "Liquidez", "depositos_pct": "Depósitos",
}

# ── TOP BAR ───────────────────────────────────────────────────────────────────
funds = discover_funds()

top_l, top_m, top_r = st.columns([2, 6, 2])

with top_l:
    st.markdown(f'<div style="font-size:18px;font-weight:800;color:{ACCENT};padding-top:6px">📊 Fund Analyzer</div>', unsafe_allow_html=True)

with top_m:
    nombres = [f["nombre"] for f in funds]
    isins   = [f["isin"]   for f in funds]
    default_idx = 0
    if st.session_state.selected_isin and st.session_state.selected_isin in isins:
        default_idx = isins.index(st.session_state.selected_isin)

    sel_nombre = st.selectbox(
        "Buscar fondo por nombre",
        options=nombres,
        index=default_idx,
        label_visibility="collapsed",
    )
    sel_idx = nombres.index(sel_nombre)
    st.session_state.selected_isin = isins[sel_idx]

with top_r:
    st.markdown("<div style='padding-top:4px'>", unsafe_allow_html=True)
    mode_label = "☀️ Claro" if DK else "🌙 Oscuro"
    if st.button(mode_label, use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<hr style='margin:8px 0 14px 0'>", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
d         = load_output(st.session_state.selected_isin)
kpis      = d.get("kpis", {})
cual      = d.get("cualitativo", {})
cuant     = d.get("cuantitativo", {})
pos_data  = d.get("posiciones", {})
consist   = d.get("analisis_consistencia", {})
fuentes   = d.get("fuentes", {})
periodos  = sorted(consist.get("periodos", []), key=lambda p: str(p.get("periodo", "")), reverse=True)

# External data files
letters_d    = load_json(st.session_state.selected_isin, "letters_data.json")
lecturas_d   = load_json(st.session_state.selected_isin, "lecturas.json")
analisis_ext = load_json(st.session_state.selected_isin, "analisis_externos.json")
meta_report  = load_json(st.session_state.selected_isin, "meta_report.json")

# ── FUND HEADER ───────────────────────────────────────────────────────────────
clasificacion = kpis.get("clasificacion", "—")
perfil        = kpis.get("perfil_riesgo", "—")
fecha_reg     = kpis.get("fecha_registro", "—")
gestora_name  = d.get("gestora", "—")
depositario   = kpis.get("depositario", "—")
divisa_f      = kpis.get("divisa", "EUR")

st.markdown(f"""
<div style="background:{'linear-gradient(135deg,#1a2540,#1c3a5f)' if DK else 'linear-gradient(135deg,#1e3a5f,#1a5276)'};
border-radius:12px;padding:18px 24px;margin-bottom:12px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start">
    <div>
      <div style="font-size:20px;font-weight:800;color:#fff">{d.get('nombre', sel_nombre)}</div>
      <div style="font-size:12px;color:#94a3b8;margin-top:4px">{gestora_name}</div>
    </div>
    <div style="text-align:right">
      <span style="background:#ffffff22;color:#fff;border-radius:6px;padding:4px 12px;font-size:13px;font-weight:700">
        {st.session_state.selected_isin}
      </span>
      <div style="font-size:11px;color:#94a3b8;margin-top:4px">Registro: {fecha_reg}</div>
    </div>
  </div>
  <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:6px">
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">{clasificacion}</span>
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">Riesgo {perfil}/7</span>
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">{divisa_f}</span>
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">Depositario: {depositario}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI ROW ───────────────────────────────────────────────────────────────────
aum       = kpis.get("aum_actual_meur")
part      = kpis.get("num_participes")
part_ant  = kpis.get("num_participes_anterior")
ter       = kpis.get("ter_pct")
gestion   = kpis.get("coste_gestion_pct")
deposito  = kpis.get("coste_deposito_pct")
vol       = kpis.get("volatilidad_pct")

part_delta_html = ""
if part and part_ant:
    chg = (part - part_ant) / part_ant * 100
    col_chg = GREEN if chg > 0 else RED
    sign = "▲" if chg > 0 else "▼"
    part_delta_html = f'<div class="kpi-sub" style="color:{col_chg}">{sign} {es(abs(chg),1)}% vs anterior</div>'

k1, k2, k3, k4, k5, k6 = st.columns(6)
kpi_defs = [
    (k1, "AUM", f'<div class="kpi-value" style="color:{ACCENT}">{meur(aum)}</div>', ""),
    (k2, "Partícipes", f'<div class="kpi-value">{es(part,0)}</div>', part_delta_html),
    (k3, "TER", f'<div class="kpi-value">{pct(ter)}</div>',
     f'<div class="kpi-sub">Gestión {pct(gestion)} + Dep. {pct(deposito)}</div>'),
    (k4, "Volatilidad VL", f'<div class="kpi-value">{pct(vol)}</div>', ""),
    (k5, "Riesgo", f'<div class="kpi-value">{perfil} <span style="font-size:14px;color:{TEXT3}">/ 7</span></div>', ""),
    (k6, "Posiciones", f'<div class="kpi-value">{len(pos_data.get("actuales",[]))}</div>',
     '<div class="kpi-sub">activos en cartera</div>'),
]
for col, label, val_html, sub_html in kpi_defs:
    with col:
        st.markdown(f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          {val_html}{sub_html}
        </div>""", unsafe_allow_html=True)

st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📋 Resumen",
    "📈 Evolutivo",
    "💼 Cartera",
    "🎯 Consistencia",
    "🔗 Lecturas",
    "🔍 Análisis externos",
    "📁 Archivos",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RESUMEN
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    c_left, c_right = st.columns([3, 2])

    with c_left:
        # Resumen ejecutivo
        st.markdown('<div class="sec">Resumen</div>', unsafe_allow_html=True)
        estrategia = cual.get("estrategia") or cual.get("filosofia_inversion") or ""
        if estrategia:
            st.markdown(f'<div class="card" style="font-size:14px;line-height:1.8;color:{TEXT}">{estrategia}</div>', unsafe_allow_html=True)

        # Filosofía
        filosofia = cual.get("filosofia_inversion") or ""
        proceso   = cual.get("proceso_seleccion") or ""
        if filosofia and filosofia != estrategia:
            st.markdown('<div class="sec">Filosofía de inversión</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card"><div class="sbox" style="max-height:160px">{filosofia}</div></div>', unsafe_allow_html=True)

        if proceso:
            st.markdown('<div class="sec">Proceso de selección</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card"><div class="sbox" style="max-height:130px">{proceso}</div></div>', unsafe_allow_html=True)

        # Visión gestora año a año
        if periodos:
            st.markdown('<div class="sec">Visión de los gestores — año a año</div>', unsafe_allow_html=True)
            periodo_labels = [p.get("periodo", "—") for p in periodos]
            sel_period = st.selectbox("Periodo", periodo_labels, key="vision_period", label_visibility="collapsed")
            pdata = next((p for p in periodos if p.get("periodo") == sel_period), {})

            tesis      = pdata.get("tesis_gestora", "")
            decisiones = pdata.get("decisiones_tomadas", "")
            contexto   = pdata.get("contexto_mercado", "")

            if tesis:
                st.markdown(f"""
                <div class="card" style="border-left:3px solid {ACCENT};margin-bottom:8px">
                  <div style="font-size:11px;font-weight:700;color:{ACCENT};text-transform:uppercase;margin-bottom:6px">Tesis gestora</div>
                  <div class="sbox" style="max-height:150px;border:none;padding:0;background:transparent">{tesis}</div>
                </div>""", unsafe_allow_html=True)
            if decisiones:
                st.markdown(f"""
                <div class="card" style="border-left:3px solid {GREEN};margin-bottom:8px">
                  <div style="font-size:11px;font-weight:700;color:{GREEN};text-transform:uppercase;margin-bottom:6px">Decisiones tomadas</div>
                  <div class="sbox" style="max-height:130px;border:none;padding:0;background:transparent">{decisiones}</div>
                </div>""", unsafe_allow_html=True)
            if contexto:
                with st.expander("Ver contexto de mercado completo"):
                    st.markdown(f'<div style="font-size:13px;color:{TEXT};line-height:1.7">{contexto}</div>', unsafe_allow_html=True)

    with c_right:
        # ── Gestores con links externos ───────────────────────────────────────
        gestores = cual.get("gestores", [])
        if gestores:
            st.markdown('<div class="sec">Equipo gestor</div>', unsafe_allow_html=True)
            for g in gestores:
                nombre_g   = g.get("nombre", "")
                cargo_g    = g.get("cargo", "")
                back_g     = g.get("background", "")
                anio_g     = g.get("anio_incorporacion", "")
                slug       = manager_slug(nombre_g)
                q_enc      = urllib.parse.quote(f'"{nombre_g}" gestor fondo')

                links_html = f"""
                <div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:4px">
                  <a class="link-pill" href="https://citywire.com/selector/manager/profile/{slug}" target="_blank">Citywire</a>
                  <a class="link-pill" href="https://www.trustnet.com/factsheets/manager/{slug}" target="_blank">Trustnet</a>
                  <a class="link-pill" href="https://www.finect.com/user/{slug}" target="_blank">Finect</a>
                  <a class="link-pill" href="https://www.google.com/search?q={q_enc}+site:citywire.com" target="_blank">Google</a>
                </div>"""

                st.markdown(f"""
                <div class="card" style="margin-bottom:8px">
                  <div style="font-size:14px;font-weight:700;color:{TEXT}">👤 {nombre_g}</div>
                  <div style="font-size:12px;color:{TEXT2};margin-top:2px">{cargo_g}{"  ·  Desde "+str(anio_g) if anio_g else ""}</div>
                  {"<div style='font-size:12px;color:"+TEXT3+";margin-top:6px;line-height:1.5'>"+back_g+"</div>" if back_g else ""}
                  {links_html}
                </div>""", unsafe_allow_html=True)

        # ── Historia del fondo — timeline visual completo ─────────────────────
        historia = cual.get("historia_fondo", "")
        periodos_asc = sorted(consist.get("periodos", []), key=lambda p: str(p.get("periodo", "")))

        # Construir eventos del timeline
        timeline_events = []

        # 1. Historia fondo (eventos regulatorios / fundacionales)
        if historia:
            for frag in re.split(r"\n+", historia):
                frag = frag.strip()
                if len(frag) < 20:
                    continue
                year_m = re.search(r"\b(20\d{2}|19\d{2})\b", frag)
                yr = year_m.group(1) if year_m else ""
                timeline_events.append({"year": yr, "text": frag, "type": "regulatory"})

        # 2. Periodos de consistencia → un evento por año
        for p in periodos_asc:
            yr = normalize_year(str(p.get("periodo", "")))
            tesis_p   = (p.get("tesis_gestora", "") or "")[:200]
            contexto_p = (p.get("contexto_mercado", "") or "")[:200]
            decisiones_p = (p.get("decisiones_tomadas", "") or "")[:150]
            short_text = tesis_p or contexto_p
            if not short_text:
                continue
            timeline_events.append({
                "year": yr,
                "text": short_text,
                "decisions": decisiones_p,
                "type": "annual",
            })

        if timeline_events:
            st.markdown('<div class="sec">Historia del fondo — timeline</div>', unsafe_allow_html=True)
            st.markdown('<div class="tl-wrap"><div class="tl-line"></div>', unsafe_allow_html=True)
            for ev in timeline_events:
                is_reg  = ev["type"] == "regulatory"
                dot_cls = "tl-dot tl-dot-reg" if is_reg else "tl-dot"
                yr_cls  = "tl-year tl-year-reg" if is_reg else "tl-year"
                yr_badge = f'<span class="{yr_cls}">{ev["year"]}</span>' if ev["year"] else ""
                decisions_html = ""
                if ev.get("decisions"):
                    decisions_html = f'<div class="tl-label" style="color:{GREEN};margin-top:4px">📌 {ev["decisions"][:120]}</div>'
                st.markdown(f"""
                <div class="tl-item">
                  <div class="{dot_cls}"></div>
                  {yr_badge}
                  <div class="tl-text">{ev["text"]}</div>
                  {decisions_html}
                </div>""", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # Tipo de activos
        tipo_activos = cual.get("tipo_activos", "")
        if tipo_activos:
            st.markdown('<div class="sec">Universo de inversión</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card" style="font-size:13px;color:{TEXT};line-height:1.6">{tipo_activos}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — EVOLUTIVO
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    # ── AUM ──────────────────────────────────────────────────────────────────
    st.markdown('<div class="sec">Evolución del patrimonio (AUM) y Valor Liquidativo</div>', unsafe_allow_html=True)
    serie_aum = [s for s in cuant.get("serie_aum", []) if s.get("valor_meur") and len(str(s["periodo"])) <= 7]

    if serie_aum:
        serie_aum_s = sorted(serie_aum, key=lambda x: str(x["periodo"]))
        # Dedup: si mismo año aparece varias veces, coger el máximo
        year_map: dict[str, dict] = {}
        for s in serie_aum_s:
            yr = normalize_year(str(s["periodo"]))
            if yr not in year_map or (s.get("valor_meur") or 0) > (year_map[yr].get("valor_meur") or 0):
                year_map[yr] = s
        deduped = sorted(year_map.values(), key=lambda x: normalize_year(str(x["periodo"])))
        labels = [normalize_year(str(s["periodo"])) for s in deduped]
        values = [s["valor_meur"] for s in deduped]
        vls    = [s.get("vl") for s in deduped]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=labels, y=values, name="AUM (M€)",
            marker_color=ACCENT,
            hovertemplate="<b>%{x}</b><br>AUM: %{y:.3f} M€<extra></extra>",
        ))
        vl_x = [labels[i] for i, v in enumerate(vls) if v]
        vl_y = [v for v in vls if v]
        if vl_y:
            fig.add_trace(go.Scatter(
                x=vl_x, y=vl_y, mode="lines+markers", name="VL",
                yaxis="y2", line=dict(color=GREEN, width=2),
                marker=dict(size=7, color=GREEN),
                hovertemplate="<b>%{x}</b><br>VL: %{y:.4f}<extra></extra>",
            ))
        ly = chart_layout(300)
        ly["yaxis2"] = dict(overlaying="y", side="right", title="VL",
                             title_font=dict(color=GREEN), tickfont=dict(color=GREEN),
                             showgrid=False)
        ly["yaxis"]["title"] = "M€"
        fig.update_layout(**ly)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos de AUM histórico")

    col_part, col_ter = st.columns(2)

    # ── Partícipes — barras ───────────────────────────────────────────────────
    with col_part:
        st.markdown('<div class="sec">Evolución de partícipes</div>', unsafe_allow_html=True)
        serie_p = cuant.get("serie_participes", [])
        part_points = [{"periodo": s["periodo"], "valor": s["valor"]} for s in serie_p if s.get("valor")]
        if part and not any(normalize_year(str(p["periodo"])) == "2025" for p in part_points):
            part_points.append({"periodo": "2025", "valor": part})
        if part_ant and not any(normalize_year(str(p["periodo"])) == "2024" for p in part_points):
            part_points.append({"periodo": "2024", "valor": part_ant})

        # Dedup by year
        yr_map_p: dict[str, float] = {}
        for p in part_points:
            yr = normalize_year(str(p["periodo"]))
            if yr not in yr_map_p:
                yr_map_p[yr] = p["valor"]
        part_sorted = sorted(yr_map_p.items())

        if part_sorted:
            fig_p = go.Figure(go.Bar(
                x=[x[0] for x in part_sorted],
                y=[x[1] for x in part_sorted],
                marker_color=PURPLE,
                hovertemplate="<b>%{x}</b><br>Partícipes: %{y:.0f}<extra></extra>",
            ))
            fig_p.update_layout(**chart_layout(240, legend=False))
            st.plotly_chart(fig_p, use_container_width=True)
        else:
            st.info("Sin datos de partícipes históricos")

    # ── TER — barras agrupadas ────────────────────────────────────────────────
    with col_ter:
        st.markdown('<div class="sec">Evolución TER y comisión de gestión</div>', unsafe_allow_html=True)
        serie_ter = cuant.get("serie_ter", [])
        ter_points = [t for t in serie_ter if t.get("ter_pct") or t.get("coste_gestion_pct")]
        if ter and not any(normalize_year(str(t.get("periodo",""))) == "2025" for t in ter_points):
            ter_points.append({"periodo": "2025", "ter_pct": ter, "coste_gestion_pct": gestion})

        # Dedup by year
        yr_map_ter: dict[str, dict] = {}
        for t in ter_points:
            yr = normalize_year(str(t.get("periodo", "")))
            if yr not in yr_map_ter:
                yr_map_ter[yr] = t
        ter_sorted = sorted(yr_map_ter.items())

        if ter_sorted:
            xlabels = [x[0] for x in ter_sorted]
            ter_y   = [x[1].get("ter_pct") for x in ter_sorted]
            gest_y  = [x[1].get("coste_gestion_pct") for x in ter_sorted]
            fig_ter = go.Figure()
            if any(v for v in ter_y):
                fig_ter.add_trace(go.Bar(
                    x=xlabels, y=ter_y, name="TER %",
                    marker_color=RED,
                    hovertemplate="TER: %{y:.2f}%<extra></extra>",
                ))
            if any(v for v in gest_y):
                fig_ter.add_trace(go.Bar(
                    x=xlabels, y=gest_y, name="Gestión %",
                    marker_color=YELLOW,
                    hovertemplate="Gestión: %{y:.2f}%<extra></extra>",
                ))
            fig_ter.update_layout(barmode="group", **chart_layout(240))
            st.plotly_chart(fig_ter, use_container_width=True)
        else:
            if ter:
                st.metric("TER actual", pct(ter))
                st.metric("Comisión gestión", pct(gestion))
            else:
                st.info("Sin datos de TER histórico")

    # ── Mix activos (stacked bars) ────────────────────────────────────────────
    st.markdown('<div class="sec">Evolución por tipo de activo</div>', unsafe_allow_html=True)
    mix_hist = cuant.get("mix_activos_historico", [])
    if mix_hist:
        # Dedup: un punto por año
        yr_map_mix: dict[str, dict] = {}
        for m in mix_hist:
            yr = normalize_year(str(m.get("periodo", "")))
            if yr not in yr_map_mix:
                yr_map_mix[yr] = m
        mix_s = sorted(yr_map_mix.items())
        xlabels = [x[0] for x in mix_s]
        mix_rows = [x[1] for x in mix_s]
        fig_mix = go.Figure()
        for key, label in MIX_LABELS.items():
            vals = [min(m.get(key, 0) or 0, 100) for m in mix_rows]
            if any(v > 0 for v in vals):
                fig_mix.add_trace(go.Bar(
                    x=xlabels, y=vals, name=label,
                    marker_color=MIX_COLORS[key],
                    hovertemplate=f"<b>{label}</b>: %{{y:.1f}}%<extra></extra>",
                ))
        fig_mix.update_layout(barmode="stack", **chart_layout(260))
        st.plotly_chart(fig_mix, use_container_width=True)

    # ── Geografía por país — stacked bars evolutivo ───────────────────────────
    posiciones_actuales = pos_data.get("actuales", [])
    historicas_pos = pos_data.get("historicas", [])

    # Construir serie geográfica desde historicas
    geo_series: list[tuple[str, dict]] = []  # [(year, {country: pct})]
    for h in sorted(historicas_pos, key=lambda x: str(x.get("periodo", ""))):
        yr = normalize_year(str(h.get("periodo", "")))
        country_weights: dict[str, float] = {}
        for p in h.get("top10", []):
            c = infer_country(p)
            country_weights[c] = country_weights.get(c, 0) + (p.get("peso_pct") or 0)
        if country_weights:
            geo_series.append((yr, country_weights))

    # Agregar posiciones actuales si no hay historico
    if not geo_series and posiciones_actuales:
        country_weights: dict[str, float] = {}
        for p in posiciones_actuales:
            c = infer_country(p)
            country_weights[c] = country_weights.get(c, 0) + (p.get("peso_pct") or 0)
        if country_weights:
            geo_series.append(("Actual", country_weights))

    if geo_series:
        st.markdown('<div class="sec">Distribución geográfica por país del emisor</div>', unsafe_allow_html=True)
        # Obtener top 8 países por peso promedio
        all_countries: dict[str, float] = {}
        for _, cw in geo_series:
            for c, w in cw.items():
                all_countries[c] = all_countries.get(c, 0) + w
        top_countries = [c for c, _ in sorted(all_countries.items(), key=lambda x: x[1], reverse=True)[:8]]

        xlabels_geo = [yr for yr, _ in geo_series]
        fig_geo = go.Figure()
        for i, country in enumerate(top_countries):
            vals = []
            for _, cw in geo_series:
                vals.append(cw.get(country, 0))
            color = GEO_COLORS[i % len(GEO_COLORS)]
            fig_geo.add_trace(go.Bar(
                x=xlabels_geo, y=vals, name=country,
                marker_color=color,
                hovertemplate=f"<b>{country}</b>: %{{y:.1f}}%<extra></extra>",
            ))
        # "Otros" bucket
        otros_vals = []
        for _, cw in geo_series:
            rest = sum(w for c, w in cw.items() if c not in top_countries)
            otros_vals.append(rest)
        if any(v > 0 for v in otros_vals):
            fig_geo.add_trace(go.Bar(
                x=xlabels_geo, y=otros_vals, name="Otros",
                marker_color="#6b7280",
                hovertemplate="<b>Otros</b>: %{y:.1f}%<extra></extra>",
            ))
        fig_geo.update_layout(barmode="stack", **chart_layout(280))
        st.caption("País inferido por prefijo ISIN del instrumento / nombre del emisor / divisa")
        st.plotly_chart(fig_geo, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CARTERA
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    actuales   = pos_data.get("actuales", [])
    historicas = pos_data.get("historicas", [])

    # Ordenar por peso desc
    actuales_sorted = sorted(actuales, key=lambda p: p.get("peso_pct") or 0, reverse=True)

    # ── Top posiciones actuales ───────────────────────────────────────────────
    st.markdown('<div class="sec">Principales posiciones actuales</div>', unsafe_allow_html=True)

    if actuales_sorted:
        top25 = actuales_sorted[:25]
        max_peso = max((p.get("peso_pct", 0) or 0) for p in top25) or 1

        # Header
        h1, h2, h3, h4, h5, h6 = st.columns([4, 1, 1, 1, 1, 2])
        for col, lbl in zip([h1, h2, h3, h4, h5, h6], ["Nombre / ISIN", "Tipo", "País", "Divisa", "Valor (M€)", "Peso %"]):
            col.markdown(f'<div style="font-size:10px;color:{TEXT3};text-transform:uppercase;font-weight:600">{lbl}</div>', unsafe_allow_html=True)

        current_tipo = None
        for pos in top25:
            tipo_p = pos.get("tipo", "")
            color  = TIPO_COLOR.get(tipo_p, "#6b7280")
            peso   = pos.get("peso_pct", 0) or 0
            val_m  = (pos.get("valor_mercado_miles", 0) or 0) / 1000
            bar    = int(peso / max_peso * 100)
            pais_p = infer_country(pos)

            if tipo_p != current_tipo:
                current_tipo = tipo_p
                st.markdown(f'<div style="font-size:10px;color:{color};font-weight:700;text-transform:uppercase;'
                            f'margin:8px 0 4px 0;padding-left:2px">{tipo_p}</div>', unsafe_allow_html=True)

            c1, c2, c3, c4, c5, c6 = st.columns([4, 1, 1, 1, 1, 2])
            with c1:
                vcto = f" · {pos['vencimiento']}" if pos.get("vencimiento") else ""
                st.markdown(
                    f'<div style="font-size:13px;font-weight:500;color:{TEXT}">{pos.get("nombre","")}</div>'
                    f'<div style="font-size:10px;color:{TEXT3}">{pos.get("ticker","")}{vcto}</div>',
                    unsafe_allow_html=True)
            with c2:
                st.markdown(f'<span style="background:{color}22;color:{color};border-radius:4px;'
                            f'padding:2px 7px;font-size:10px;font-weight:600">{tipo_p}</span>',
                            unsafe_allow_html=True)
            with c3:
                st.markdown(f'<span style="font-size:11px;color:{TEXT2}">{pais_p}</span>', unsafe_allow_html=True)
            with c4:
                st.markdown(f'<span style="font-size:12px;color:{TEXT2}">{pos.get("divisa","")}</span>', unsafe_allow_html=True)
            with c5:
                st.markdown(f'<span style="font-size:12px;color:{TEXT2}">{es(val_m,2)}</span>', unsafe_allow_html=True)
            with c6:
                st.markdown(
                    f'<div style="font-size:13px;font-weight:700;color:{ACCENT}">{es(peso,2)}%</div>'
                    f'<div style="background:{BG3};border-radius:3px;height:4px;margin-top:3px">'
                    f'<div style="background:{ACCENT};border-radius:3px;height:4px;width:{bar}%"></div></div>',
                    unsafe_allow_html=True)
            st.markdown(f'<hr style="border:none;border-top:1px solid {BORDER};margin:4px 0">', unsafe_allow_html=True)

        if len(actuales) > 25:
            st.caption(f"+ {len(actuales) - 25} posiciones adicionales")

    # ── Evolución peso top posiciones (líneas) ────────────────────────────────
    if len(historicas) >= 2:
        st.markdown('<div class="sec">Evolución del peso de las principales posiciones</div>', unsafe_allow_html=True)
        hist_s = sorted(historicas, key=lambda h: str(h.get("periodo", "")))

        all_tickers: dict[str, dict] = {}
        for h in hist_s:
            for p in h.get("top10", []):
                key = p.get("ticker") or p.get("nombre", "")
                if key not in all_tickers:
                    all_tickers[key] = {"nombre": p.get("nombre", key), "periodos": {}}
                all_tickers[key]["periodos"][h["periodo"]] = p.get("peso_pct", 0) or 0

        top_tickers = sorted(all_tickers.items(), key=lambda x: sum(x[1]["periodos"].values()), reverse=True)[:10]
        xlabels = [normalize_year(str(h["periodo"])) for h in hist_s]

        def ticker_color(t):
            h_val = int(hashlib.md5(t.encode()).hexdigest()[:6], 16)
            hue = h_val % 360
            return f"hsl({hue},65%,55%)"

        fig_evol = go.Figure()
        for ticker, info in top_tickers:
            y = [info["periodos"].get(h["periodo"], None) for h in hist_s]
            fig_evol.add_trace(go.Scatter(
                x=xlabels, y=y, mode="lines+markers",
                name=info["nombre"][:25],
                line=dict(width=2, color=ticker_color(ticker)),
                marker=dict(size=6),
                connectgaps=False,
                hovertemplate=f"<b>{info['nombre'][:30]}</b><br>%{{y:.2f}}%<extra></extra>",
            ))
        fig_evol.update_layout(**chart_layout(300))
        st.plotly_chart(fig_evol, use_container_width=True)

    # ── Cambios relevantes año a año ─────────────────────────────────────────
    if len(historicas) >= 2:
        st.markdown('<div class="sec">Cambios relevantes de cartera — año a año</div>', unsafe_allow_html=True)

        def compute_changes(hist):
            srt = sorted(hist, key=lambda h: str(h.get("periodo", "")))
            changes = []
            for i in range(1, len(srt)):
                prev_m = {(p.get("ticker") or p.get("nombre","")): p for p in srt[i-1].get("top10",[])}
                curr_m = {(p.get("ticker") or p.get("nombre","")): p for p in srt[i].get("top10",[])}
                entradas = [p for k,p in curr_m.items() if k not in prev_m and (p.get("peso_pct") or 0) >= 1]
                salidas  = [p for k,p in prev_m.items() if k not in curr_m and (p.get("peso_pct") or 0) >= 1]
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

        def pos_card(p, bcolor, tcolor, show_delta=False, delta=None):
            delta_html = ""
            if show_delta and delta is not None:
                sign = "+" if delta > 0 else ""
                delta_html = f'<span style="color:{tcolor};font-weight:700"> {sign}{es(delta,2)}%</span>'
            return (f'<div style="background:{"#0d2b1f" if tcolor==GREEN else "#2b0d0d" if tcolor==RED else BG3};'
                    f'border-left:3px solid {bcolor};border-radius:0 6px 6px 0;'
                    f'padding:6px 10px;margin-bottom:5px;font-size:12px">'
                    f'<span style="color:{TEXT};font-weight:600">{p.get("nombre","")[:28]}</span>'
                    f'<br><span style="color:{TEXT3}">{p.get("ticker","")}</span>'
                    f'&nbsp;<span style="color:{bcolor};font-weight:700">{es(p.get("peso_pct",0),2)}%</span>'
                    f'{delta_html}</div>')

        for ch in compute_changes(historicas):
            n = len(ch["entradas"]) + len(ch["salidas"]) + len(ch["cambios"])
            if n == 0:
                continue
            with st.expander(f"**{ch['de']} → {ch['a']}** · {n} cambio{'s' if n>1 else ''} detectado{'s' if n>1 else ''}"):
                cc1, cc2, cc3 = st.columns(3)
                with cc1:
                    st.markdown(f'<div style="font-size:11px;color:{GREEN};font-weight:700;text-transform:uppercase;margin-bottom:6px">▲ Entradas</div>', unsafe_allow_html=True)
                    if ch["entradas"]:
                        for p in ch["entradas"]:
                            st.markdown(pos_card(p, GREEN, GREEN), unsafe_allow_html=True)
                    else:
                        st.caption("—")
                with cc2:
                    st.markdown(f'<div style="font-size:11px;color:{RED};font-weight:700;text-transform:uppercase;margin-bottom:6px">▼ Salidas</div>', unsafe_allow_html=True)
                    if ch["salidas"]:
                        for p in ch["salidas"]:
                            st.markdown(pos_card(p, RED, RED), unsafe_allow_html=True)
                    else:
                        st.caption("—")
                with cc3:
                    st.markdown(f'<div style="font-size:11px;color:{ACCENT};font-weight:700;text-transform:uppercase;margin-bottom:6px">⇅ Cambios de peso</div>', unsafe_allow_html=True)
                    if ch["cambios"]:
                        for p in ch["cambios"]:
                            d_val = p["delta"]
                            col_d = GREEN if d_val > 0 else RED
                            st.markdown(pos_card(p, col_d, col_d, show_delta=True, delta=d_val), unsafe_allow_html=True)
                    else:
                        st.caption("—")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CONSISTENCIA
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    resumen_g = consist.get("resumen_global", "")
    if resumen_g:
        st.markdown(f"""
        <div class="card" style="border-left:3px solid {ACCENT};margin-bottom:16px;font-size:14px;color:{TEXT};line-height:1.7">
          <div style="font-size:11px;font-weight:700;color:{ACCENT};text-transform:uppercase;margin-bottom:6px">Síntesis global</div>
          {resumen_g}
        </div>""", unsafe_allow_html=True)

    if periodos:
        st.markdown('<div class="sec">Análisis de consistencia — por periodo</div>', unsafe_allow_html=True)
        for pdata in periodos:
            periodo_lbl = pdata.get("periodo", "—")
            tesis       = pdata.get("tesis_gestora", "")
            contexto    = pdata.get("contexto_mercado", "")
            decisiones  = pdata.get("decisiones_tomadas", "")
            resultado   = pdata.get("resultado_real", "")
            score       = pdata.get("consistencia_score")

            score_html = ""
            if score is not None:
                col_sc = GREEN if score >= 7 else YELLOW if score >= 4 else RED
                score_html = f'<span style="background:{col_sc}22;color:{col_sc};border-radius:4px;padding:1px 8px;font-size:11px;font-weight:700;margin-left:8px">Score {score}/10</span>'

            with st.expander(f"**{periodo_lbl}**{score_html}", expanded=(pdata == periodos[0])):
                p1, p2 = st.columns(2)
                with p1:
                    if contexto:
                        st.markdown(f'<div style="font-size:11px;font-weight:700;color:{TEXT2};text-transform:uppercase;margin-bottom:6px">Contexto de mercado</div>', unsafe_allow_html=True)
                        st.markdown(f'<div class="sbox">{contexto[:800]}{"..." if len(contexto)>800 else ""}</div>', unsafe_allow_html=True)
                with p2:
                    if tesis:
                        st.markdown(f'<div style="font-size:11px;font-weight:700;color:{TEXT2};text-transform:uppercase;margin-bottom:6px">Tesis gestora</div>', unsafe_allow_html=True)
                        st.markdown(f'<div class="sbox">{tesis[:600]}{"..." if len(tesis)>600 else ""}</div>', unsafe_allow_html=True)
                    if decisiones:
                        st.markdown(f'<div style="font-size:11px;font-weight:700;color:{GREEN};text-transform:uppercase;margin:8px 0 6px 0">Decisiones</div>', unsafe_allow_html=True)
                        st.markdown(f'<div class="sbox" style="max-height:120px">{decisiones}</div>', unsafe_allow_html=True)
                    if resultado:
                        st.markdown(f'<div style="font-size:11px;font-weight:700;color:{YELLOW};text-transform:uppercase;margin:8px 0 6px 0">Resultado real</div>', unsafe_allow_html=True)
                        st.markdown(f'<div class="sbox" style="max-height:100px">{resultado}</div>', unsafe_allow_html=True)

    # ── Hechos relevantes — todos los años ────────────────────────────────────
    st.markdown('<div class="sec">Hechos relevantes históricos</div>', unsafe_allow_html=True)

    # Combinar historia_fondo + periodos de consistencia (desc)
    hechos = []
    historia = cual.get("historia_fondo", "")
    if historia:
        for b in re.split(r"\n+", historia):
            b = b.strip()
            if len(b) < 20:
                continue
            year_m = re.search(r"\b(20\d{2}|19\d{2})\b", b)
            hechos.append({"year": year_m.group(1) if year_m else "", "text": b, "source": "regulatorio"})

    # Añadir contexto de todos los periodos
    for p in periodos:  # ya están en desc
        yr = normalize_year(str(p.get("periodo", "")))
        ctx = (p.get("contexto_mercado", "") or "")[:300]
        dec = (p.get("decisiones_tomadas", "") or "")[:200]
        if ctx or dec:
            combined = ctx + (f"\n📌 {dec}" if dec else "")
            hechos.append({"year": yr, "text": combined, "source": "gestion"})

    if hechos:
        for h_item in hechos:
            yr_lbl = h_item["year"]
            col_h  = YELLOW if h_item["source"] == "regulatorio" else ACCENT
            yr_badge = f'<span style="font-weight:700;color:{col_h};margin-right:8px">{yr_lbl}</span>' if yr_lbl else ""
            st.markdown(f"""
            <div style="background:{BG3};border-left:3px solid {col_h};border-radius:0 8px 8px 0;
            padding:8px 14px;margin-bottom:6px;font-size:13px;line-height:1.6">
              {yr_badge}<span style="color:{TEXT}">{h_item['text']}</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.info("No hay hechos relevantes disponibles para este fondo.")

    # ── Cartas trimestrales ───────────────────────────────────────────────────
    cartas_list = letters_d.get("cartas", []) or letters_d.get("letters", [])
    if cartas_list:
        st.markdown('<div class="sec">Cartas trimestrales — resumen</div>', unsafe_allow_html=True)
        for carta in cartas_list[:8]:
            fecha_c  = carta.get("fecha", carta.get("date", ""))
            titulo_c = carta.get("titulo", carta.get("title", ""))
            resumen_c = carta.get("resumen", carta.get("summary", ""))
            with st.expander(f"{fecha_c}  {titulo_c}"):
                if resumen_c:
                    st.markdown(f'<div style="font-size:13px;color:{TEXT};line-height:1.7">{resumen_c}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="sec">Cartas trimestrales</div>', unsafe_allow_html=True)
        nombre_f = d.get("nombre", "")
        gestores_ns = [g.get("nombre","") for g in cual.get("gestores",[])]
        st.markdown(f"""
        <div class="card" style="border:1px dashed {BORDER}">
          <div style="font-size:13px;color:{TEXT2};line-height:1.7">
            No se encontraron cartas trimestrales para <strong>{nombre_f}</strong>.<br>
            Re-ejecuta el pipeline:
            <code style="font-size:12px;background:{BG3};padding:2px 8px;border-radius:4px">
              python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto
            </code>
          </div>
        </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — LECTURAS
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="sec">Lecturas, vídeos y entrevistas recomendadas</div>', unsafe_allow_html=True)

    # lecturas_d puede ser list o dict
    if isinstance(lecturas_d, dict):
        lecturas_list = lecturas_d.get("lecturas", [])
    elif isinstance(lecturas_d, list):
        lecturas_list = lecturas_d
    else:
        lecturas_list = []

    TIPO_ICON = {"articulo": "📄", "video": "🎥", "entrevista": "🎙️",
                 "podcast": "🎧", "perfil_gestor": "👤", "otro": "🔗"}

    if lecturas_list:
        for item in lecturas_list:
            tipo_l = item.get("tipo", "otro")
            icon   = TIPO_ICON.get(tipo_l, "🔗")
            url    = item.get("url", "#")
            titulo = item.get("titulo", url)
            desc   = item.get("descripcion", "") or item.get("snippet", "")
            fecha  = item.get("fecha", "")
            fuente = item.get("fuente", "") or item.get("source", "")

            st.markdown(f"""
            <div class="card" style="margin-bottom:8px">
              <div style="display:flex;justify-content:space-between;align-items:flex-start">
                <div>
                  <span style="font-size:12px;background:{BG3};border-radius:4px;padding:2px 8px;
                  color:{TEXT2};margin-right:8px">{icon} {tipo_l.replace("_"," ").capitalize()}</span>
                  {"<span style='font-size:11px;color:"+TEXT3+"'>"+fecha+"</span>" if fecha else ""}
                  <br>
                  <a href="{url}" target="_blank" style="font-size:14px;font-weight:600;
                  color:{ACCENT};text-decoration:none">{titulo}</a>
                  {"<div style='font-size:12px;color:"+TEXT2+";margin-top:2px'>"+fuente+"</div>" if fuente else ""}
                </div>
              </div>
              {"<div style='font-size:13px;color:"+TEXT+";margin-top:8px;line-height:1.6'>"+str(desc)+"</div>" if desc else ""}
            </div>""", unsafe_allow_html=True)
    else:
        nombre_fondo = d.get("nombre", "")
        gestores_names = [g.get("nombre","") for g in cual.get("gestores",[])]
        st.markdown(f"""
        <div class="card" style="border:1px dashed {BORDER}">
          <div style="font-size:14px;font-weight:600;color:{TEXT};margin-bottom:10px">
            🔍 Aún no se han buscado lecturas para este fondo
          </div>
          <div style="font-size:13px;color:{TEXT2};line-height:1.7">
            Para generarlas automáticamente, ejecuta:
            <code style="font-size:12px;background:{BG3};padding:2px 8px;border-radius:4px">
              python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto
            </code>
          </div>
        </div>""", unsafe_allow_html=True)

        if gestores_names:
            st.markdown('<div class="sec" style="margin-top:16px">Búsquedas sugeridas</div>', unsafe_allow_html=True)
            queries = [f'"{nombre_fondo}" entrevista', f'"{nombre_fondo}" carta gestores'] + \
                      [f'"{n}" gestor fondo entrevista' for n in gestores_names[:2]]
            for q in queries:
                enc = urllib.parse.quote(q)
                st.markdown(
                    f'<div style="background:{BG3};border-radius:6px;padding:8px 12px;margin-bottom:6px;font-size:13px">'
                    f'🔎 <a href="https://www.google.com/search?q={enc}" target="_blank" '
                    f'style="color:{ACCENT};text-decoration:none">{q}</a></div>',
                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — ANÁLISIS EXTERNOS
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.markdown('<div class="sec">Análisis publicados por webs especializadas</div>', unsafe_allow_html=True)

    # analisis_ext puede ser dict o list
    if isinstance(analisis_ext, dict):
        ext_list = analisis_ext.get("analisis_externos", [])
    elif isinstance(analisis_ext, list):
        ext_list = analisis_ext
    else:
        ext_list = []

    SOURCE_ICONS = {
        "saludfinanciera.es": "💚", "astralis.es": "⭐", "morningstar.es": "🌟",
        "rankia.com": "📊", "finect.com": "🔵", "investing.com": "📈",
        "elblogsalmon.com": "🐟", "expansión": "📰", "cinco dias": "📰",
    }

    if ext_list:
        for item in ext_list:
            fuente = item.get("fuente", "") or item.get("source", "")
            titulo = item.get("titulo", item.get("title", "Sin título"))
            url    = item.get("url", "#")
            fecha  = item.get("fecha", "")
            resumen_e = item.get("resumen", "") or item.get("snippet", "")
            palabras = item.get("palabras_estimadas", "")

            icon = "🔍"
            for k, v in SOURCE_ICONS.items():
                if k in fuente.lower():
                    icon = v
                    break

            palabras_html = f'<span style="font-size:10px;color:{TEXT3}"> · ~{palabras} palabras</span>' if palabras else ""
            st.markdown(f"""
            <div class="ext-card">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px">
                <div>
                  <span style="font-size:12px;background:{BG3};border-radius:4px;padding:2px 8px;color:{TEXT2}">
                    {icon} {fuente}
                  </span>
                  {f'<span style="font-size:11px;color:{TEXT3};margin-left:8px">{fecha}</span>' if fecha else ""}
                  {palabras_html}
                </div>
              </div>
              <div style="margin-top:8px">
                <a href="{url}" target="_blank" style="font-size:15px;font-weight:700;
                color:{ACCENT};text-decoration:none">{titulo}</a>
              </div>
              {f'<div style="font-size:13px;color:{TEXT};margin-top:8px;line-height:1.65">{resumen_e}</div>' if resumen_e else ""}
            </div>""", unsafe_allow_html=True)

    else:
        # Mostrar búsquedas sugeridas para el usuario
        nombre_fondo = d.get("nombre", "")
        SOURCES = [
            ("Salud Financiera", "saludfinanciera.es", "💚"),
            ("Astralis", "astralis.es", "⭐"),
            ("Morningstar", "morningstar.es", "🌟"),
            ("Rankia", "rankia.com", "📊"),
            ("Finect", "finect.com", "🔵"),
        ]
        st.markdown(f"""
        <div class="card" style="border:1px dashed {BORDER};margin-bottom:16px">
          <div style="font-size:14px;font-weight:600;color:{TEXT};margin-bottom:10px">
            🔍 Sin análisis externos disponibles todavía
          </div>
          <div style="font-size:13px;color:{TEXT2};line-height:1.7">
            El agente de lecturas buscará análisis de este fondo en webs especializadas.<br>
            Ejecuta: <code style="background:{BG3};padding:2px 8px;border-radius:4px">python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto</code>
          </div>
        </div>""", unsafe_allow_html=True)

        st.markdown('<div class="sec">Buscar manualmente en:</div>', unsafe_allow_html=True)
        cols_src = st.columns(len(SOURCES))
        for i, (name, site, icon) in enumerate(SOURCES):
            q = urllib.parse.quote(f'"{nombre_fondo}"')
            with cols_src[i]:
                st.markdown(
                    f'<div style="text-align:center;background:{BG3};border-radius:10px;padding:14px;border:1px solid {BORDER}">'
                    f'<div style="font-size:24px">{icon}</div>'
                    f'<div style="font-size:12px;font-weight:700;color:{TEXT};margin:4px 0">{name}</div>'
                    f'<a href="https://www.google.com/search?q=site:{site}+{q}" target="_blank" '
                    f'style="font-size:11px;color:{ACCENT}">Buscar →</a></div>',
                    unsafe_allow_html=True)

    # Meta-report issues (si existe)
    if meta_report and meta_report.get("issues"):
        st.markdown('<div class="sec" style="margin-top:20px">⚠️ Issues detectados por meta-agente</div>', unsafe_allow_html=True)
        for issue in meta_report["issues"]:
            st.markdown(
                f'<div style="background:{BG3};border-left:3px solid {YELLOW};border-radius:0 6px 6px 0;'
                f'padding:6px 12px;margin-bottom:4px;font-size:13px;color:{TEXT}">⚠️ {issue}</div>',
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — ARCHIVOS
# ══════════════════════════════════════════════════════════════════════════════
with tab7:
    st.markdown('<div class="sec">Archivos y fuentes analizadas</div>', unsafe_allow_html=True)

    xmls = fuentes.get("xmls_cnmv", [])
    pdfs = fuentes.get("informes_descargados", [])
    urls = fuentes.get("urls_consultadas", [])

    col_f1, col_f2 = st.columns(2)

    with col_f1:
        if pdfs:
            st.markdown(f"""
            <div class="card">
              <div style="font-size:12px;font-weight:700;color:{ACCENT};text-transform:uppercase;margin-bottom:10px">
                📄 Informes semestrales CNMV ({len(pdfs)})
              </div>""", unsafe_allow_html=True)
            for pdf in sorted(pdfs):
                st.markdown(f'<div style="font-size:12px;color:{TEXT};padding:3px 0;border-bottom:1px solid {BORDER}">📋 {pdf}</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        if urls:
            st.markdown(f"""
            <div class="card" style="margin-top:10px">
              <div style="font-size:12px;font-weight:700;color:{PURPLE};text-transform:uppercase;margin-bottom:10px">
                🌐 URLs consultadas
              </div>""", unsafe_allow_html=True)
            for url in urls:
                short = url[:70] + "..." if len(url) > 70 else url
                st.markdown(f'<div style="font-size:11px;color:{ACCENT};padding:3px 0">'
                            f'<a href="{url}" target="_blank" style="color:{ACCENT};text-decoration:none">{short}</a>'
                            f'</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with col_f2:
        if xmls:
            st.markdown(f"""
            <div class="card">
              <div style="font-size:12px;font-weight:700;color:{GREEN};text-transform:uppercase;margin-bottom:10px">
                🗂️ XMLs CNMV bulk data ({len(xmls)})
              </div>""", unsafe_allow_html=True)
            for xml in sorted(xmls):
                st.markdown(f'<div style="font-size:11px;color:{TEXT2};padding:2px 0">{xml}</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(f"""
    <div class="card" style="margin-top:12px">
      <div style="font-size:12px;font-weight:700;color:{TEXT2};text-transform:uppercase;margin-bottom:10px">ℹ️ Metadatos del análisis</div>
      <table style="width:100%;font-size:13px;border-collapse:collapse">
        <tr><td style="color:{TEXT3};padding:4px 0;width:160px">ISIN</td><td style="color:{TEXT}">{st.session_state.selected_isin}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">Tipo</td><td style="color:{TEXT}">{d.get('tipo','—')}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">Última actualización</td><td style="color:{TEXT}">{str(d.get('ultima_actualizacion') or '—')[:19].replace('T',' ')}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">XMLs procesados</td><td style="color:{TEXT}">{len(xmls)}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">PDFs procesados</td><td style="color:{TEXT}">{len(pdfs)}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">Periodos consistencia</td><td style="color:{TEXT}">{len(periodos)}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">Lecturas</td><td style="color:{TEXT}">{len(lecturas_list) if isinstance(lecturas_d, list) else len(lecturas_d.get("lecturas",[]) if isinstance(lecturas_d, dict) else [])}</td></tr>
        <tr><td style="color:{TEXT3};padding:4px 0">Análisis externos</td><td style="color:{TEXT}">{len(ext_list)}</td></tr>
      </table>
    </div>""", unsafe_allow_html=True)
