"""
Fund Analyzer — Streamlit Dashboard v2
"""
import hashlib
import json
import re
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
HEADER_TEXT = "#ffffff"

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  .stApp {{ background-color: {BG}; color: {TEXT}; }}
  section[data-testid="stSidebar"] {{ display: none; }}
  header[data-testid="stHeader"] {{ display: none; }}
  .block-container {{ padding: 1rem 2rem 2rem 2rem; max-width: 1400px; }}

  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {{
    background: {BG2};
    border-radius: 10px;
    padding: 4px;
    border: 1px solid {BORDER};
    gap: 2px;
  }}
  .stTabs [data-baseweb="tab"] {{
    background: transparent;
    color: {TEXT2};
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    padding: 6px 16px;
    border: none;
  }}
  .stTabs [aria-selected="true"] {{
    background: {ACCENT} !important;
    color: white !important;
  }}
  .stTabs [data-baseweb="tab-panel"] {{
    padding-top: 16px;
  }}

  /* Cards */
  .card {{
    background: {BG2};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 16px 20px;
  }}
  .kpi-card {{
    background: {BG2};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 14px 18px;
    text-align: center;
  }}
  .kpi-label {{ font-size: 10px; color: {TEXT2}; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px; }}
  .kpi-value {{ font-size: 22px; font-weight: 700; color: {TEXT}; line-height: 1.1; }}
  .kpi-sub   {{ font-size: 11px; color: {TEXT3}; margin-top: 3px; }}

  /* Section titles */
  .sec {{ font-size: 12px; font-weight: 700; color: {TEXT2}; text-transform: uppercase;
          letter-spacing: 1px; border-bottom: 1px solid {BORDER};
          padding-bottom: 6px; margin: 20px 0 12px 0; }}

  /* Timeline */
  .tl-item {{
    border-left: 2px solid {ACCENT};
    padding: 6px 0 6px 16px;
    margin-bottom: 8px;
    position: relative;
  }}
  .tl-dot {{
    width: 8px; height: 8px; background: {ACCENT};
    border-radius: 50%; position: absolute; left: -5px; top: 10px;
  }}
  .tl-year {{ font-size: 11px; font-weight: 700; color: {ACCENT}; }}
  .tl-text {{ font-size: 13px; color: {TEXT}; line-height: 1.5; }}

  /* Badge */
  .badge {{
    display: inline-block;
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 600;
    margin-right: 4px;
  }}

  /* Scroll box */
  .sbox {{
    background: {"#111827" if DK else "#f8fafc"};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 12px 14px;
    font-size: 13px;
    color: {TEXT};
    line-height: 1.7;
    max-height: 220px;
    overflow-y: auto;
  }}

  /* Position row */
  .pos-row {{
    padding: 7px 0;
    border-bottom: 1px solid {BORDER};
    font-size: 13px;
  }}

  /* Expander */
  .streamlit-expanderHeader {{
    background: {BG3} !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    color: {TEXT} !important;
  }}
  div[data-testid="stExpander"] > details > summary {{
    background: {BG3};
    border-radius: 8px;
    padding: 8px 14px;
  }}

  /* selectbox / text_input */
  .stSelectbox > div > div, .stTextInput > div > div > input {{
    background: {BG2} !important;
    color: {TEXT} !important;
    border-color: {BORDER} !important;
    font-size: 13px !important;
  }}

  /* hide plotly modebar */
  .modebar {{ display: none !important; }}

  /* hr */
  hr {{ border-color: {BORDER}; opacity: 0.4; }}

  /* Dataframe */
  .stDataFrame {{ background: {BG2}; }}
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_output(isin: str) -> dict:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def discover_funds() -> list[dict]:
    """Returns [{isin, nombre}] sorted by nombre."""
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


def chart_layout(height=280, legend=True) -> dict:
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT2, size=12),
        height=height,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=legend,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            bgcolor="rgba(0,0,0,0)", font=dict(size=11, color=TEXT2),
        ),
        xaxis=dict(showgrid=False, tickfont=dict(color=TEXT2), linecolor=BORDER),
        yaxis=dict(showgrid=True, gridcolor=BORDER, tickfont=dict(color=TEXT2)),
    )


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

# ── TOP BAR: search + dark mode ───────────────────────────────────────────────
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

    search_cols = st.columns([4, 1])
    with search_cols[0]:
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

# ── FUND HEADER ───────────────────────────────────────────────────────────────
clasificacion = kpis.get("clasificacion", "—")
perfil        = kpis.get("perfil_riesgo", "—")
fecha_reg     = kpis.get("fecha_registro", "—")
gestora       = d.get("gestora", "—")
depositario   = kpis.get("depositario", "—")
divisa        = kpis.get("divisa", "EUR")

st.markdown(f"""
<div style="background:{'linear-gradient(135deg,#1a2540,#1c3a5f)' if DK else 'linear-gradient(135deg,#1e3a5f,#1a5276)'};
border-radius:12px;padding:18px 24px;margin-bottom:12px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start">
    <div>
      <div style="font-size:20px;font-weight:800;color:#fff">{d.get('nombre', sel_nombre)}</div>
      <div style="font-size:12px;color:#94a3b8;margin-top:4px">{gestora}</div>
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
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">{divisa}</span>
    <span style="background:#ffffff1a;color:#e0e0e0;border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600">Depositario: {depositario}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI ROW (siempre visible) ─────────────────────────────────────────────────
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
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Resumen",
    "📈 Evolutivo",
    "💼 Cartera",
    "🎯 Consistencia",
    "🔗 Lecturas",
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
        objetivos = cual.get("objetivos_reales") or ""
        if filosofia and filosofia != estrategia:
            st.markdown('<div class="sec">Filosofía de inversión</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card"><div class="sbox" style="max-height:160px">{filosofia}</div></div>', unsafe_allow_html=True)

        # Estrategia y proceso
        if proceso:
            st.markdown('<div class="sec">Proceso de selección</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card"><div class="sbox" style="max-height:130px">{proceso}</div></div>', unsafe_allow_html=True)

        # Visión gestora año a año
        if periodos:
            st.markdown('<div class="sec">Visión de los gestores — año a año</div>', unsafe_allow_html=True)
            periodo_labels = [p.get("periodo", "—") for p in periodos]
            sel_period = st.selectbox("Periodo", periodo_labels, key="vision_period", label_visibility="collapsed")
            pdata = next((p for p in periodos if p.get("periodo") == sel_period), {})

            tesis     = pdata.get("tesis_gestora", "")
            decisiones = pdata.get("decisiones_tomadas", "")
            contexto  = pdata.get("contexto_mercado", "")

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
        # Gestores
        gestores = cual.get("gestores", [])
        if gestores:
            st.markdown('<div class="sec">Equipo gestor</div>', unsafe_allow_html=True)
            for g in gestores:
                st.markdown(f"""
                <div class="card" style="margin-bottom:8px">
                  <div style="font-size:14px;font-weight:700;color:{TEXT}">👤 {g.get('nombre','')}</div>
                  <div style="font-size:12px;color:{TEXT2};margin-top:2px">{g.get('cargo','')}</div>
                  {"<div style='font-size:12px;color:"+TEXT3+";margin-top:4px'>"+g.get('background','')+"</div>" if g.get('background') else ""}
                </div>""", unsafe_allow_html=True)

        # Historia del fondo — timeline
        historia = cual.get("historia_fondo", "")
        if historia:
            st.markdown('<div class="sec">Historia del fondo</div>', unsafe_allow_html=True)
            # Intentar dividir por párrafos / frases relevantes
            partes = [p.strip() for p in re.split(r'\n{2,}|(?<=[.!?])\s{2,}', historia) if len(p.strip()) > 30]
            if not partes:
                partes = [historia]

            # Si hay año en el texto, usarlo como etiqueta de timeline
            for p in partes[:6]:
                year_m = re.search(r'\b(20\d{2}|19\d{2})\b', p)
                year_lbl = year_m.group(1) if year_m else ""
                st.markdown(f"""
                <div class="tl-item">
                  <div class="tl-dot"></div>
                  {"<div class='tl-year'>"+year_lbl+"</div>" if year_lbl else ""}
                  <div class="tl-text">{p}</div>
                </div>""", unsafe_allow_html=True)

        # Tipo de activos
        tipo_activos = cual.get("tipo_activos", "")
        if tipo_activos:
            st.markdown('<div class="sec">Universo de inversión</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="card" style="font-size:13px;color:{TEXT};line-height:1.6">{tipo_activos}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — EVOLUTIVO
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    # ── AUM + VL ─────────────────────────────────────────────────────────────
    st.markdown('<div class="sec">Evolución del patrimonio (AUM) y Valor Liquidativo</div>', unsafe_allow_html=True)
    serie_aum = [s for s in cuant.get("serie_aum", []) if s.get("valor_meur") and len(str(s["periodo"])) <= 7]

    if serie_aum:
        serie_aum_s = sorted(serie_aum, key=lambda x: str(x["periodo"]))
        labels = [str(s["periodo"]) for s in serie_aum_s]
        values = [s["valor_meur"] for s in serie_aum_s]
        vls    = [s.get("vl") for s in serie_aum_s]

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

    # ── Partícipes ────────────────────────────────────────────────────────────
    with col_part:
        st.markdown('<div class="sec">Evolución de partícipes</div>', unsafe_allow_html=True)
        serie_p = cuant.get("serie_participes", [])
        # También usar num_participes del kpi como punto más reciente
        part_points = [{"periodo": s["periodo"], "valor": s["valor"]} for s in serie_p if s.get("valor")]
        if part and kpis.get("fecha_registro"):
            part_points.append({"periodo": "2025-S2", "valor": part})
        if part_ant:
            part_points.append({"periodo": "2024-S2", "valor": part_ant})
        part_points = sorted({p["periodo"]: p for p in part_points}.values(), key=lambda x: str(x["periodo"]))

        if part_points:
            fig_p = go.Figure(go.Scatter(
                x=[p["periodo"] for p in part_points],
                y=[p["valor"] for p in part_points],
                mode="lines+markers",
                line=dict(color=PURPLE, width=2),
                marker=dict(size=8, color=PURPLE),
                fill="tozeroy",
                fillcolor="rgba(139,92,246,0.13)",
                hovertemplate="<b>%{x}</b><br>Partícipes: %{y:.0f}<extra></extra>",
            ))
            fig_p.update_layout(**chart_layout(240, legend=False))
            st.plotly_chart(fig_p, use_container_width=True)
        else:
            st.info("Sin datos de partícipes históricos")

    # ── TER ───────────────────────────────────────────────────────────────────
    with col_ter:
        st.markdown('<div class="sec">Evolución TER y comisión de gestión</div>', unsafe_allow_html=True)
        serie_ter = cuant.get("serie_ter", [])
        # Añadir dato actual si hay
        ter_points = [t for t in serie_ter if t.get("ter_pct") or t.get("coste_gestion_pct")]
        if ter and ter_points and not any(str(t.get("periodo","")).startswith("2025") for t in ter_points):
            ter_points.append({"periodo": "2025", "ter_pct": ter, "coste_gestion_pct": gestion})
        ter_points = sorted(ter_points, key=lambda x: str(x["periodo"]))

        if ter_points:
            fig_ter = go.Figure()
            ter_y = [t.get("ter_pct") for t in ter_points]
            gest_y = [t.get("coste_gestion_pct") for t in ter_points]
            xlabels = [str(t["periodo"]) for t in ter_points]
            if any(ter_y):
                fig_ter.add_trace(go.Scatter(
                    x=xlabels, y=ter_y, mode="lines+markers", name="TER %",
                    line=dict(color=RED, width=2), marker=dict(size=7),
                    hovertemplate="TER: %{y:.2f}%<extra></extra>",
                ))
            if any(gest_y):
                fig_ter.add_trace(go.Scatter(
                    x=xlabels, y=gest_y, mode="lines+markers", name="Gestión %",
                    line=dict(color=YELLOW, width=2, dash="dot"), marker=dict(size=7),
                    hovertemplate="Gestión: %{y:.2f}%<extra></extra>",
                ))
            fig_ter.update_layout(**chart_layout(240))
            st.plotly_chart(fig_ter, use_container_width=True)
        else:
            if ter:
                st.metric("TER actual", pct(ter))
                st.metric("Comisión gestión", pct(gestion))
            else:
                st.info("Sin datos de TER histórico")

    # ── Mix activos (stacked) ─────────────────────────────────────────────────
    st.markdown('<div class="sec">Evolución por tipo de activo</div>', unsafe_allow_html=True)
    mix_hist = cuant.get("mix_activos_historico", [])
    if mix_hist:
        mix_s = sorted(mix_hist, key=lambda m: str(m.get("periodo", "")))
        xlabels = [str(m["periodo"]) for m in mix_s]
        fig_mix = go.Figure()
        for key, label in MIX_LABELS.items():
            vals = [min(m.get(key, 0) or 0, 100) for m in mix_s]
            if any(v > 0 for v in vals):
                fig_mix.add_trace(go.Bar(
                    x=xlabels, y=vals, name=label,
                    marker_color=MIX_COLORS[key],
                    hovertemplate=f"<b>{label}</b>: %{{y:.1f}}%<extra></extra>",
                ))
        fig_mix.update_layout(barmode="stack", **chart_layout(260))
        st.plotly_chart(fig_mix, use_container_width=True)

    # ── Geografía ─────────────────────────────────────────────────────────────
    geo_hist = cuant.get("mix_geografico_historico", [])
    posiciones_actuales = pos_data.get("actuales", [])

    # Si no hay geo_hist explícito, inferirlo desde posiciones por divisa
    if not geo_hist and posiciones_actuales:
        st.markdown('<div class="sec">Distribución geográfica (por divisa de la posición)</div>', unsafe_allow_html=True)
        divisa_totals: dict[str, float] = {}
        for p in posiciones_actuales:
            div = p.get("divisa", "Otras")
            divisa_totals[div] = divisa_totals.get(div, 0) + (p.get("peso_pct") or 0)
        divisa_totals = dict(sorted(divisa_totals.items(), key=lambda x: x[1], reverse=True))
        fig_geo = go.Figure(go.Pie(
            labels=list(divisa_totals.keys()),
            values=list(divisa_totals.values()),
            hole=0.45,
            marker=dict(line=dict(color=BG, width=2)),
            textinfo="label+percent",
            textfont=dict(size=12),
            hovertemplate="<b>%{label}</b><br>%{value:.1f}%<extra></extra>",
        ))
        fig_geo.update_layout(**chart_layout(260, legend=False))
        st.plotly_chart(fig_geo, use_container_width=True)
    elif geo_hist:
        st.markdown('<div class="sec">Evolución geográfica</div>', unsafe_allow_html=True)
        st.info("Datos geográficos históricos disponibles — próximamente")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CARTERA
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    actuales   = pos_data.get("actuales", [])
    historicas = pos_data.get("historicas", [])

    # ── Top 25 posiciones actuales ────────────────────────────────────────────
    st.markdown('<div class="sec">Principales posiciones actuales</div>', unsafe_allow_html=True)

    if actuales:
        top25 = actuales[:25]
        max_peso = max((p.get("peso_pct", 0) or 0) for p in top25) or 1

        # Header
        h1, h2, h3, h4, h5 = st.columns([4, 1, 1, 1, 2])
        for col, lbl in zip([h1, h2, h3, h4, h5], ["Nombre / ISIN", "Tipo", "Divisa", "Valor (M€)", "Peso %"]):
            col.markdown(f'<div style="font-size:10px;color:{TEXT3};text-transform:uppercase;font-weight:600">{lbl}</div>', unsafe_allow_html=True)

        # Group by tipo for sub-headers
        current_tipo = None
        for pos in top25:
            tipo_p = pos.get("tipo", "")
            color  = TIPO_COLOR.get(tipo_p, "#6b7280")
            peso   = pos.get("peso_pct", 0) or 0
            val_m  = (pos.get("valor_mercado_miles", 0) or 0) / 1000  # → M€
            bar    = int(peso / max_peso * 100)

            if tipo_p != current_tipo:
                current_tipo = tipo_p
                st.markdown(f'<div style="font-size:10px;color:{color};font-weight:700;text-transform:uppercase;'
                            f'margin:8px 0 4px 0;padding-left:2px">{tipo_p}</div>', unsafe_allow_html=True)

            c1, c2, c3, c4, c5 = st.columns([4, 1, 1, 1, 2])
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
                st.markdown(f'<span style="font-size:12px;color:{TEXT2}">{pos.get("divisa","")}</span>', unsafe_allow_html=True)
            with c4:
                st.markdown(f'<span style="font-size:12px;color:{TEXT2}">{es(val_m,2)}</span>', unsafe_allow_html=True)
            with c5:
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

        # Construir mapa: ticker → [pesos por periodo]
        all_tickers: dict[str, dict] = {}
        for h in hist_s:
            for p in h.get("top10", []):
                key = p.get("ticker") or p.get("nombre", "")
                if key not in all_tickers:
                    all_tickers[key] = {"nombre": p.get("nombre", key), "periodos": {}}
                all_tickers[key]["periodos"][h["periodo"]] = p.get("peso_pct", 0) or 0

        # Top 10 por peso promedio
        top_tickers = sorted(all_tickers.items(), key=lambda x: sum(x[1]["periodos"].values()), reverse=True)[:10]
        xlabels = [h["periodo"] for h in hist_s]

        def ticker_color(t):
            h = int(hashlib.md5(t.encode()).hexdigest()[:6], 16)
            hue = h % 360
            return f"hsl({hue},65%,55%)"

        fig_evol = go.Figure()
        for ticker, info in top_tickers:
            y = [info["periodos"].get(p, None) for p in xlabels]
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

        for ch in compute_changes(historicas):
            n = len(ch["entradas"]) + len(ch["salidas"]) + len(ch["cambios"])
            if n == 0:
                continue
            with st.expander(f"**{ch['de']} → {ch['a']}** · {n} cambio{'s' if n>1 else ''} detectado{'s' if n>1 else ''}"):
                cc1, cc2, cc3 = st.columns(3)

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

    # Resumen global
    resumen_g = consist.get("resumen_global", "")
    if resumen_g:
        st.markdown(f"""
        <div class="card" style="border-left:3px solid {ACCENT};margin-bottom:16px;font-size:14px;color:{TEXT};line-height:1.7">
          <div style="font-size:11px;font-weight:700;color:{ACCENT};text-transform:uppercase;margin-bottom:6px">Síntesis global</div>
          {resumen_g}
        </div>""", unsafe_allow_html=True)

    if periodos:
        # Análisis año a año
        st.markdown('<div class="sec">Análisis de consistencia — por periodo</div>', unsafe_allow_html=True)
        for pdata in periodos:
            periodo_lbl = pdata.get("periodo", "—")
            tesis      = pdata.get("tesis_gestora", "")
            contexto   = pdata.get("contexto_mercado", "")
            decisiones = pdata.get("decisiones_tomadas", "")

            with st.expander(f"**{periodo_lbl}**", expanded=(pdata == periodos[0])):
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

    # Hechos relevantes
    historia = cual.get("historia_fondo", "")
    if historia:
        st.markdown('<div class="sec">Hechos relevantes</div>', unsafe_allow_html=True)
        # Dividir por líneas / bloques
        bloques = [b.strip() for b in re.split(r'\n{1,}', historia) if len(b.strip()) > 20]
        for b in bloques:
            year_m = re.search(r'\b(20\d{2}|19\d{2})\b', b)
            year_lbl = year_m.group(1) if year_m else ""
            st.markdown(f"""
            <div style="background:{BG3};border-left:3px solid {YELLOW};border-radius:0 8px 8px 0;
            padding:8px 14px;margin-bottom:6px;font-size:13px">
              {"<span style='font-weight:700;color:"+YELLOW+";margin-right:8px'>"+year_lbl+"</span>" if year_lbl else ""}
              <span style="color:{TEXT}">{b}</span>
            </div>""", unsafe_allow_html=True)

    # Cartas trimestrales placeholder
    cartas = fuentes.get("cartas_gestores", [])
    letras_data_path = Path(__file__).parent.parent / "data" / "funds" / st.session_state.selected_isin / "letters_data.json"
    letters_d = {}
    if letras_data_path.exists():
        try:
            letters_d = json.loads(letras_data_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    cartas_list = letters_d.get("cartas", []) or letters_d.get("letters", [])
    if cartas_list:
        st.markdown('<div class="sec">Cartas trimestrales — resumen</div>', unsafe_allow_html=True)
        for carta in cartas_list[:6]:
            fecha  = carta.get("fecha", carta.get("date", ""))
            titulo = carta.get("titulo", carta.get("title", ""))
            resumen_c = carta.get("resumen", carta.get("summary", ""))
            with st.expander(f"{fecha}  {titulo}"):
                if resumen_c:
                    st.markdown(f'<div style="font-size:13px;color:{TEXT};line-height:1.7">{resumen_c}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="sec">Cartas trimestrales</div>', unsafe_allow_html=True)
        st.info("No se encontraron cartas trimestrales para este fondo. "
                "El agente de cartas busca en la web de la gestora. "
                "Puedes re-ejecutar el pipeline con fuentes=1 para intentarlo.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — LECTURAS
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="sec">Lecturas, vídeos y entrevistas recomendadas</div>', unsafe_allow_html=True)

    # Intentar cargar desde un archivo de lecturas si existe
    lecturas_path = Path(__file__).parent.parent / "data" / "funds" / st.session_state.selected_isin / "lecturas.json"
    lecturas_data = []
    if lecturas_path.exists():
        try:
            lecturas_data = json.loads(lecturas_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    TIPO_ICON = {"articulo": "📄", "video": "🎥", "entrevista": "🎙️", "podcast": "🎧", "otro": "🔗"}

    if lecturas_data:
        for item in lecturas_data:
            tipo_l = item.get("tipo", "otro")
            icon   = TIPO_ICON.get(tipo_l, "🔗")
            url    = item.get("url", "#")
            titulo = item.get("titulo", url)
            desc   = item.get("descripcion", "")
            fecha  = item.get("fecha", "")
            fuente = item.get("fuente", "")

            st.markdown(f"""
            <div class="card" style="margin-bottom:8px">
              <div style="display:flex;justify-content:space-between;align-items:flex-start">
                <div>
                  <span style="font-size:12px;background:{BG3};border-radius:4px;padding:2px 8px;
                  color:{TEXT2};margin-right:8px">{icon} {tipo_l.capitalize()}</span>
                  {"<span style='font-size:11px;color:"+TEXT3+"'>"+fecha+"</span>" if fecha else ""}
                  <br>
                  <a href="{url}" target="_blank" style="font-size:14px;font-weight:600;
                  color:{ACCENT};text-decoration:none">{titulo}</a>
                  {"<div style='font-size:12px;color:"+TEXT2+";margin-top:2px'>"+fuente+"</div>" if fuente else ""}
                </div>
              </div>
              {"<div style='font-size:13px;color:"+TEXT+";margin-top:8px;line-height:1.6'>"+desc+"</div>" if desc else ""}
            </div>""", unsafe_allow_html=True)
    else:
        # Construir búsqueda sugerida
        nombre_fondo = d.get("nombre", "")
        gestores_names = [g.get("nombre","") for g in cual.get("gestores",[])]
        st.markdown(f"""
        <div class="card" style="border:1px dashed {BORDER}">
          <div style="font-size:14px;font-weight:600;color:{TEXT};margin-bottom:10px">
            🔍 Aún no se han buscado lecturas para este fondo
          </div>
          <div style="font-size:13px;color:{TEXT2};line-height:1.7">
            Para generar lecturas automáticamente, ejecuta el pipeline con el agente de lecturas habilitado.<br>
            Se buscarán: artículos, entrevistas y vídeos sobre <strong>{nombre_fondo}</strong>
            {"y los gestores: <strong>"+(", ".join(gestores_names))+"</strong>" if gestores_names else ""}.
          </div>
          <div style="margin-top:12px">
            <code style="font-size:12px;background:{BG3};padding:6px 12px;border-radius:6px">
              python -m agents.orchestrator --isin {st.session_state.selected_isin} --auto
            </code>
          </div>
        </div>""", unsafe_allow_html=True)

        # Sugerir búsquedas manuales
        if gestores_names:
            st.markdown('<div class="sec" style="margin-top:16px">Sugerencias de búsqueda manual</div>', unsafe_allow_html=True)
            queries = [
                f'"{nombre_fondo}" entrevista',
                f'"{nombre_fondo}" cartera permanente gestor',
            ] + [f'"{n}" gestor fondo entrevista' for n in gestores_names[:2]]
            for q in queries:
                st.markdown(
                    f'<div style="background:{BG3};border-radius:6px;padding:8px 12px;margin-bottom:6px;font-size:13px">'
                    f'🔎 <a href="https://www.google.com/search?q={q.replace(" ","+")}" target="_blank" '
                    f'style="color:{ACCENT};text-decoration:none">{q}</a></div>',
                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — ARCHIVOS
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.markdown('<div class="sec">Archivos y fuentes analizadas</div>', unsafe_allow_html=True)

    xmls    = fuentes.get("xmls_cnmv", [])
    pdfs    = fuentes.get("informes_descargados", [])
    urls    = fuentes.get("urls_consultadas", [])

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

    # Metadatos del análisis
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
      </table>
    </div>""", unsafe_allow_html=True)
