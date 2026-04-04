"""
Fund Analyzer — Streamlit Dashboard
Ejecutar: streamlit run dashboard/app.py
Deploy:   Streamlit Cloud (conectar repo GitHub, apuntar a dashboard/app.py)
"""
import json
import os
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fund Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personalizado ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Fondo oscuro tipo dashboard financiero */
    .stApp { background-color: #0f1117; color: #e0e0e0; }
    section[data-testid="stSidebar"] { background-color: #161b27; }

    /* KPI cards */
    .kpi-card {
        background: #1c2333;
        border: 1px solid #2d3748;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
        margin-bottom: 8px;
    }
    .kpi-label { font-size: 11px; color: #8892a4; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 4px; }
    .kpi-value { font-size: 26px; font-weight: 700; color: #ffffff; line-height: 1.1; }
    .kpi-sub   { font-size: 12px; color: #6b7a99; margin-top: 4px; }
    .kpi-up    { color: #34d399; }
    .kpi-down  { color: #f87171; }
    .kpi-neutral { color: #60a5fa; }

    /* Section headers */
    .section-header {
        font-size: 13px;
        font-weight: 600;
        color: #8892a4;
        text-transform: uppercase;
        letter-spacing: 1px;
        border-bottom: 1px solid #2d3748;
        padding-bottom: 6px;
        margin: 24px 0 14px 0;
    }

    /* Fund header */
    .fund-header {
        background: linear-gradient(135deg, #1a2540 0%, #1c2333 100%);
        border: 1px solid #2d3748;
        border-radius: 12px;
        padding: 20px 28px;
        margin-bottom: 20px;
    }
    .fund-name { font-size: 22px; font-weight: 700; color: #fff; }
    .fund-meta { font-size: 13px; color: #8892a4; margin-top: 4px; }
    .fund-badge {
        display: inline-block;
        background: #1e3a5f;
        color: #60a5fa;
        font-size: 11px;
        font-weight: 600;
        padding: 3px 10px;
        border-radius: 20px;
        margin-right: 6px;
    }

    /* Position table */
    .pos-row {
        display: flex;
        align-items: center;
        padding: 8px 0;
        border-bottom: 1px solid #1e2637;
        font-size: 13px;
    }
    .pos-bar-bg {
        background: #1e2637;
        border-radius: 4px;
        height: 6px;
        width: 100%;
    }
    .pos-bar-fill {
        background: #3b82f6;
        border-radius: 4px;
        height: 6px;
    }

    /* Timeline / consistencia */
    .period-card {
        background: #1c2333;
        border-left: 3px solid #3b82f6;
        border-radius: 0 8px 8px 0;
        padding: 14px 18px;
        margin-bottom: 10px;
    }
    .period-title { font-size: 14px; font-weight: 700; color: #60a5fa; margin-bottom: 6px; }
    .period-body  { font-size: 13px; color: #b0bec5; line-height: 1.6; }

    /* Scrollable text box */
    .scroll-box {
        background: #161b27;
        border: 1px solid #2d3748;
        border-radius: 8px;
        padding: 14px;
        font-size: 13px;
        color: #b0bec5;
        line-height: 1.7;
        max-height: 200px;
        overflow-y: auto;
    }

    /* Hide default Streamlit header/footer */
    header[data-testid="stHeader"] { display: none; }
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_output(isin: str) -> dict:
    p = Path(__file__).parent.parent / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def discover_isins() -> list[str]:
    base = Path(__file__).parent.parent / "data" / "funds"
    if not base.exists():
        return []
    return sorted([
        d.name for d in base.iterdir()
        if d.is_dir() and (d / "output.json").exists()
    ])


def fmt_meur(v) -> str:
    if v is None:
        return "—"
    return f"{v:,.2f} M€"


def fmt_pct(v) -> str:
    if v is None:
        return "—"
    return f"{v:.2f}%"


def fmt_int(v) -> str:
    if v is None:
        return "—"
    return f"{int(v):,}"


TIPO_COLOR = {
    "REPO": "#f59e0b",
    "BONO": "#3b82f6",
    "IIC": "#10b981",
    "PARTICIPACIONES": "#10b981",
    "PAGARE": "#a78bfa",
    "OBLIGACION": "#60a5fa",
    "RENTA FIJA": "#3b82f6",
    "ETC": "#34d399",
}

MIX_COLORS = {
    "renta_fija_pct": "#3b82f6",
    "rv_pct": "#10b981",
    "iic_pct": "#8b5cf6",
    "liquidez_pct": "#f59e0b",
    "depositos_pct": "#6b7280",
}
MIX_LABELS = {
    "renta_fija_pct": "Renta Fija",
    "rv_pct": "Renta Variable",
    "iic_pct": "IIC / ETF",
    "liquidez_pct": "Liquidez",
    "depositos_pct": "Depósitos",
}

# ── Sidebar — selector de fondo ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📊 Fund Analyzer")
    st.markdown("---")

    isins = discover_isins()
    if not isins:
        st.error("No hay fondos en data/funds/")
        st.stop()

    selected_isin = st.selectbox("Seleccionar fondo", isins, format_func=lambda x: x)

    d = load_output(selected_isin)
    if not d:
        st.error(f"No se pudo cargar output.json para {selected_isin}")
        st.stop()

    nombre = d.get("nombre", selected_isin)
    st.markdown(f"**{nombre}**")
    st.caption(f"Actualizado: {d.get('ultima_actualizacion', '')[:10]}")

    st.markdown("---")
    st.caption("Fund Analyzer · v1.0")

# ── Main content ──────────────────────────────────────────────────────────────
kpis = d.get("kpis", {})
cual = d.get("cualitativo", {})
cuant = d.get("cuantitativo", {})
posiciones = d.get("posiciones", {}).get("actuales", [])
consistencia = d.get("analisis_consistencia", {})
periodos = sorted(
    consistencia.get("periodos", []),
    key=lambda p: p.get("periodo", ""),
    reverse=True,
)

# ── HEADER ────────────────────────────────────────────────────────────────────
tipo = d.get("tipo", "ES")
gestora = d.get("gestora", "—")
depositario = kpis.get("depositario", "—")
fecha_reg = kpis.get("fecha_registro", "—")
clasificacion = kpis.get("clasificacion", "—")

st.markdown(f"""
<div class="fund-header">
  <div class="fund-name">{nombre}</div>
  <div class="fund-meta" style="margin-top:6px;">
    <span class="fund-badge">{selected_isin}</span>
    <span class="fund-badge">{tipo}</span>
    <span class="fund-badge">{clasificacion}</span>
    <span class="fund-badge">Riesgo {kpis.get('perfil_riesgo', '—')}/7</span>
  </div>
  <div class="fund-meta" style="margin-top:8px;">
    <strong>Gestora:</strong> {gestora} &nbsp;·&nbsp;
    <strong>Depositario:</strong> {depositario} &nbsp;·&nbsp;
    <strong>Registro:</strong> {fecha_reg}
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI ROW ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5, c6 = st.columns(6)

part = kpis.get("num_participes")
part_ant = kpis.get("num_participes_anterior")
part_delta = ""
if part and part_ant:
    pct_change = (part - part_ant) / part_ant * 100
    arrow = "▲" if pct_change > 0 else "▼"
    color_class = "kpi-up" if pct_change > 0 else "kpi-down"
    part_delta = f'<div class="kpi-sub {color_class}">{arrow} {abs(pct_change):.1f}% vs anterior</div>'

with c1:
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">AUM</div>
      <div class="kpi-value kpi-neutral">{fmt_meur(kpis.get('aum_actual_meur'))}</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">Partícipes</div>
      <div class="kpi-value">{fmt_int(part)}</div>
      {part_delta}
    </div>""", unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">TER</div>
      <div class="kpi-value">{fmt_pct(kpis.get('ter_pct'))}</div>
      <div class="kpi-sub">gestión {fmt_pct(kpis.get('coste_gestion_pct'))} + dep. {fmt_pct(kpis.get('coste_deposito_pct'))}</div>
    </div>""", unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">Volatilidad VL</div>
      <div class="kpi-value">{fmt_pct(kpis.get('volatilidad_pct'))}</div>
    </div>""", unsafe_allow_html=True)

with c5:
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">Perfil Riesgo</div>
      <div class="kpi-value">{kpis.get('perfil_riesgo', '—')} <span style="font-size:14px;color:#6b7a99">/ 7</span></div>
    </div>""", unsafe_allow_html=True)

with c6:
    n_activos = len(posiciones)
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">Activos en cartera</div>
      <div class="kpi-value">{n_activos}</div>
      <div class="kpi-sub">posiciones actuales</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 1: AUM chart + Mix activos ────────────────────────────────────────────
col_aum, col_mix = st.columns([3, 2])

with col_aum:
    st.markdown('<div class="section-header">Patrimonio histórico (AUM)</div>', unsafe_allow_html=True)
    serie_aum = cuant.get("serie_aum", [])
    if serie_aum:
        # Filtrar solo los que tienen valor real (excluir los del XML sin VL)
        aum_clean = [
            {"periodo": s["periodo"], "aum": s["valor_meur"], "vl": s.get("vl")}
            for s in serie_aum
            if s.get("valor_meur") and s["periodo"] not in ["202506"]
        ]
        # Añadir XML si hay
        for s in serie_aum:
            if s["periodo"] == "202506":
                aum_clean.append({"periodo": "2025-jun*", "aum": s["valor_meur"], "vl": None})

        labels = [a["periodo"] for a in aum_clean]
        values = [a["aum"] for a in aum_clean]
        vls = [a.get("vl") for a in aum_clean]

        fig_aum = go.Figure()
        fig_aum.add_trace(go.Bar(
            x=labels, y=values,
            marker_color=["#3b82f6" if "jun" not in l else "#6b7280" for l in labels],
            name="AUM (M€)",
            hovertemplate="<b>%{x}</b><br>AUM: %{y:.2f} M€<extra></extra>",
        ))
        # VL line
        vl_x = [labels[i] for i, v in enumerate(vls) if v]
        vl_y = [v for v in vls if v]
        if vl_y:
            fig_aum.add_trace(go.Scatter(
                x=vl_x, y=vl_y,
                mode="lines+markers",
                name="Valor Liquidativo",
                yaxis="y2",
                line=dict(color="#34d399", width=2),
                marker=dict(size=7, color="#34d399"),
                hovertemplate="<b>%{x}</b><br>VL: %{y:.4f}<extra></extra>",
            ))

        fig_aum.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8892a4", size=12),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02,
                bgcolor="rgba(0,0,0,0)", font=dict(size=11),
            ),
            xaxis=dict(showgrid=False, tickfont=dict(color="#8892a4")),
            yaxis=dict(
                showgrid=True, gridcolor="#1e2637",
                title="M€", title_font=dict(color="#8892a4"),
                tickfont=dict(color="#8892a4"),
            ),
            yaxis2=dict(
                overlaying="y", side="right",
                title="VL", title_font=dict(color="#34d399"),
                tickfont=dict(color="#34d399"),
                showgrid=False,
            ),
            margin=dict(l=0, r=0, t=10, b=0),
            height=260,
            barmode="group",
        )
        st.plotly_chart(fig_aum, use_container_width=True)
    else:
        st.info("Sin datos de AUM histórico")

with col_mix:
    st.markdown('<div class="section-header">Mix de activos — último S2</div>', unsafe_allow_html=True)
    mix_hist = cuant.get("mix_activos_historico", [])
    if mix_hist:
        # Tomar el más reciente
        latest_mix = sorted(mix_hist, key=lambda m: str(m.get("periodo", "")), reverse=True)[0]
        mix_keys = [k for k in MIX_LABELS if latest_mix.get(k, 0)]
        mix_vals = [latest_mix.get(k, 0) for k in mix_keys]
        mix_lbls = [MIX_LABELS[k] for k in mix_keys]
        mix_cols = [MIX_COLORS[k] for k in mix_keys]

        fig_mix = go.Figure(go.Pie(
            labels=mix_lbls,
            values=mix_vals,
            hole=0.55,
            marker=dict(colors=mix_cols, line=dict(color="#0f1117", width=2)),
            textinfo="percent",
            textfont=dict(size=12, color="#fff"),
            hovertemplate="<b>%{label}</b><br>%{value:.1f}%<extra></extra>",
        ))
        fig_mix.add_annotation(
            text=f"<b>{latest_mix.get('periodo', '')}</b>",
            x=0.5, y=0.5,
            font=dict(size=14, color="#fff"),
            showarrow=False,
        )
        fig_mix.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8892a4"),
            showlegend=True,
            legend=dict(
                orientation="v", x=1.0, y=0.5,
                bgcolor="rgba(0,0,0,0)",
                font=dict(size=11, color="#b0bec5"),
            ),
            margin=dict(l=0, r=80, t=10, b=0),
            height=260,
        )
        st.plotly_chart(fig_mix, use_container_width=True)
    else:
        st.info("Sin datos de mix de activos")

# ── ROW 2: Mix histórico stacked bar ─────────────────────────────────────────
st.markdown('<div class="section-header">Mix de activos — evolución histórica</div>', unsafe_allow_html=True)

mix_hist = cuant.get("mix_activos_historico", [])
if len(mix_hist) > 1:
    mix_sorted = sorted(mix_hist, key=lambda m: str(m.get("periodo", "")))
    periodos_mix = [str(m["periodo"]) for m in mix_sorted]

    fig_stack = go.Figure()
    for key, label in MIX_LABELS.items():
        vals = [m.get(key, 0) or 0 for m in mix_sorted]
        if any(v > 0 for v in vals):
            # Cap RF at 100 for 2017 outlier
            vals = [min(v, 100) for v in vals]
            fig_stack.add_trace(go.Bar(
                x=periodos_mix, y=vals,
                name=label,
                marker_color=MIX_COLORS[key],
                hovertemplate=f"<b>{label}</b><br>%{{y:.1f}}%<extra></extra>",
            ))

    fig_stack.update_layout(
        barmode="stack",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#8892a4", size=12),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            bgcolor="rgba(0,0,0,0)", font=dict(size=11),
        ),
        xaxis=dict(showgrid=False, tickfont=dict(color="#8892a4")),
        yaxis=dict(
            showgrid=True, gridcolor="#1e2637",
            title="%", tickfont=dict(color="#8892a4"),
            range=[0, 110],
        ),
        margin=dict(l=0, r=0, t=10, b=0),
        height=220,
    )
    st.plotly_chart(fig_stack, use_container_width=True)

# ── ROW 3: Posiciones + Filosofía ─────────────────────────────────────────────
col_pos, col_fil = st.columns([3, 2])

with col_pos:
    st.markdown('<div class="section-header">Cartera actual — Top posiciones</div>', unsafe_allow_html=True)
    if posiciones:
        max_peso = max((p.get("peso_pct", 0) or 0) for p in posiciones)
        top_n = posiciones[:15]

        # Header
        h1, h2, h3, h4 = st.columns([3, 1, 1, 2])
        h1.markdown('<span style="font-size:11px;color:#6b7a99;text-transform:uppercase">Nombre / ISIN</span>', unsafe_allow_html=True)
        h2.markdown('<span style="font-size:11px;color:#6b7a99;text-transform:uppercase">Tipo</span>', unsafe_allow_html=True)
        h3.markdown('<span style="font-size:11px;color:#6b7a99;text-transform:uppercase">Div.</span>', unsafe_allow_html=True)
        h4.markdown('<span style="font-size:11px;color:#6b7a99;text-transform:uppercase">Peso</span>', unsafe_allow_html=True)

        for pos in top_n:
            peso = pos.get("peso_pct", 0) or 0
            tipo_p = pos.get("tipo", "")
            color = TIPO_COLOR.get(tipo_p, "#6b7280")
            bar_pct = int(peso / max_peso * 100) if max_peso else 0

            c1, c2, c3, c4 = st.columns([3, 1, 1, 2])
            with c1:
                st.markdown(
                    f'<div style="font-size:13px;color:#e0e0e0;font-weight:500">{pos.get("nombre","")}</div>'
                    f'<div style="font-size:11px;color:#6b7a99">{pos.get("ticker","")}</div>',
                    unsafe_allow_html=True
                )
            with c2:
                st.markdown(
                    f'<span style="background:{color}22;color:{color};border-radius:4px;padding:2px 7px;font-size:11px;font-weight:600">{tipo_p}</span>',
                    unsafe_allow_html=True
                )
            with c3:
                st.markdown(f'<span style="font-size:12px;color:#8892a4">{pos.get("divisa","")}</span>', unsafe_allow_html=True)
            with c4:
                st.markdown(
                    f'<div style="font-size:13px;font-weight:700;color:#60a5fa">{peso:.2f}%</div>'
                    f'<div style="background:#1e2637;border-radius:3px;height:5px;margin-top:3px">'
                    f'<div style="background:#3b82f6;border-radius:3px;height:5px;width:{bar_pct}%"></div></div>',
                    unsafe_allow_html=True
                )
            st.markdown('<hr style="border:none;border-top:1px solid #1e2637;margin:4px 0">', unsafe_allow_html=True)

        if len(posiciones) > 15:
            st.caption(f"+ {len(posiciones) - 15} posiciones adicionales")

    # ── Cambios relevantes año a año ─────────────────────────────────────────
    historicas = d.get("posiciones", {}).get("historicas", [])
    if len(historicas) >= 2:
        st.markdown('<div class="section-header" style="margin-top:20px">Cambios relevantes de cartera — año a año</div>', unsafe_allow_html=True)

        def compute_portfolio_changes(hist):
            srt = sorted(hist, key=lambda h: str(h.get("periodo", "")))
            changes = []
            for i in range(1, len(srt)):
                prev_pos = srt[i - 1].get("top10", [])
                curr_pos = srt[i].get("top10", [])
                prev_map = {(p.get("ticker") or p.get("nombre", "")): p for p in prev_pos}
                curr_map = {(p.get("ticker") or p.get("nombre", "")): p for p in curr_pos}
                entradas = [p for k, p in curr_map.items() if k not in prev_map and (p.get("peso_pct") or 0) >= 1]
                salidas  = [p for k, p in prev_map.items() if k not in curr_map and (p.get("peso_pct") or 0) >= 1]
                peso_changes = []
                for k, p in curr_map.items():
                    if k in prev_map:
                        delta = (p.get("peso_pct") or 0) - (prev_map[k].get("peso_pct") or 0)
                        if abs(delta) >= 0.5:
                            peso_changes.append({**p, "delta": round(delta, 2)})
                peso_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)
                changes.append({
                    "de": srt[i - 1]["periodo"],
                    "a": srt[i]["periodo"],
                    "entradas": entradas,
                    "salidas": salidas,
                    "cambios_peso": peso_changes[:6],
                })
            return list(reversed(changes))

        cambios = compute_portfolio_changes(historicas)
        for ch in cambios:
            label = f"**{ch['de']} → {ch['a']}**"
            n_cambios = len(ch["entradas"]) + len(ch["salidas"]) + len(ch["cambios_peso"])
            if n_cambios == 0:
                continue
            with st.expander(f"{ch['de']} → {ch['a']}  ·  {n_cambios} cambios detectados"):
                ec1, ec2, ec3 = st.columns(3)
                with ec1:
                    st.markdown('<span style="font-size:11px;color:#34d399;text-transform:uppercase;font-weight:600">▲ Entradas</span>', unsafe_allow_html=True)
                    if ch["entradas"]:
                        for p in ch["entradas"]:
                            st.markdown(
                                f'<div style="background:#0d2b1f;border-left:3px solid #34d399;border-radius:0 6px 6px 0;'
                                f'padding:6px 10px;margin-bottom:5px;font-size:12px">'
                                f'<span style="color:#e0e0e0;font-weight:600">{p.get("nombre","")}</span><br>'
                                f'<span style="color:#6b7a99">{p.get("ticker","")}</span> &nbsp;'
                                f'<span style="color:#34d399;font-weight:700">{p.get("peso_pct",0):.2f}%</span>'
                                f'</div>',
                                unsafe_allow_html=True
                            )
                    else:
                        st.caption("Sin entradas nuevas")
                with ec2:
                    st.markdown('<span style="font-size:11px;color:#f87171;text-transform:uppercase;font-weight:600">▼ Salidas</span>', unsafe_allow_html=True)
                    if ch["salidas"]:
                        for p in ch["salidas"]:
                            st.markdown(
                                f'<div style="background:#2b0d0d;border-left:3px solid #f87171;border-radius:0 6px 6px 0;'
                                f'padding:6px 10px;margin-bottom:5px;font-size:12px">'
                                f'<span style="color:#e0e0e0;font-weight:600">{p.get("nombre","")}</span><br>'
                                f'<span style="color:#6b7a99">{p.get("ticker","")}</span> &nbsp;'
                                f'<span style="color:#f87171;font-weight:700">{p.get("peso_pct",0):.2f}%</span>'
                                f'</div>',
                                unsafe_allow_html=True
                            )
                    else:
                        st.caption("Sin salidas")
                with ec3:
                    st.markdown('<span style="font-size:11px;color:#60a5fa;text-transform:uppercase;font-weight:600">⇅ Cambios de peso</span>', unsafe_allow_html=True)
                    if ch["cambios_peso"]:
                        for p in ch["cambios_peso"]:
                            delta = p["delta"]
                            dcolor = "#34d399" if delta > 0 else "#f87171"
                            dsign = "+" if delta > 0 else ""
                            st.markdown(
                                f'<div style="background:#111827;border-left:3px solid {dcolor};border-radius:0 6px 6px 0;'
                                f'padding:6px 10px;margin-bottom:5px;font-size:12px">'
                                f'<span style="color:#e0e0e0;font-weight:600">{p.get("nombre","")}</span><br>'
                                f'<span style="color:#6b7a99">{p.get("ticker","")}</span> &nbsp;'
                                f'<span style="color:{dcolor};font-weight:700">{dsign}{delta:.2f}%</span>'
                                f'</div>',
                                unsafe_allow_html=True
                            )
                    else:
                        st.caption("Sin cambios significativos")

with col_fil:
    st.markdown('<div class="section-header">Filosofía e inversión</div>', unsafe_allow_html=True)

    gestores = cual.get("gestores", [])
    if gestores:
        for g in gestores:
            st.markdown(
                f'<div style="background:#1c2333;border-radius:8px;padding:10px 14px;margin-bottom:8px">'
                f'<span style="font-size:14px;font-weight:600;color:#fff">👤 {g.get("nombre","")}</span>'
                f'<span style="font-size:12px;color:#6b7a99;margin-left:8px">{g.get("cargo","")}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    filosofia = cual.get("filosofia_inversion", "") or cual.get("estrategia", "")
    if filosofia:
        st.markdown(
            f'<div class="scroll-box">{filosofia}</div>',
            unsafe_allow_html=True
        )

    st.markdown('<div class="section-header" style="margin-top:16px">Historia del fondo</div>', unsafe_allow_html=True)
    historia = cual.get("historia_fondo", "")
    if historia:
        # Dividir por frases clave
        items = [h.strip() for h in historia.replace("\n", " ").split("  ") if len(h.strip()) > 20]
        for item in items[:4]:
            st.markdown(
                f'<div style="background:#1c2333;border-left:3px solid #f59e0b;border-radius:0 6px 6px 0;'
                f'padding:8px 12px;margin-bottom:6px;font-size:12px;color:#b0bec5">{item}</div>',
                unsafe_allow_html=True
            )

# ── ROW 4: Análisis de consistencia ──────────────────────────────────────────
st.markdown('<div class="section-header">Análisis de consistencia — visión gestora por periodo</div>', unsafe_allow_html=True)

if periodos:
    # Tabs por año
    tab_labels = [p.get("periodo", "—") for p in periodos]
    if len(tab_labels) > 0:
        tabs = st.tabs(tab_labels)
        for tab, periodo in zip(tabs, periodos):
            with tab:
                tc1, tc2 = st.columns(2)
                with tc1:
                    st.markdown("**Contexto de mercado**")
                    ctx = periodo.get("contexto_mercado", "")
                    if ctx:
                        # Mostrar los primeros 600 chars con expander para más
                        preview = ctx[:600] + ("..." if len(ctx) > 600 else "")
                        st.markdown(
                            f'<div class="scroll-box" style="max-height:180px">{ctx}</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        st.caption("Sin datos de contexto")

                with tc2:
                    st.markdown("**Tesis y decisiones del gestor**")
                    tesis = periodo.get("tesis_gestora", "")
                    decisiones = periodo.get("decisiones_tomadas", "")
                    combined = ""
                    if tesis:
                        combined += tesis
                    if decisiones:
                        combined += f"\n\n**Decisiones:** {decisiones}"
                    if combined:
                        st.markdown(
                            f'<div class="scroll-box" style="max-height:180px">{combined.replace(chr(10), "<br>")}</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        st.caption("Sin datos")
else:
    st.info("Sin periodos de análisis de consistencia")

# ── Resumen global ─────────────────────────────────────────────────────────────
resumen = consistencia.get("resumen_global", "")
if resumen:
    st.markdown('<div class="section-header">Resumen global del análisis</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="background:#1a2540;border:1px solid #2d3748;border-radius:10px;padding:16px 20px;'
        f'font-size:13px;color:#b0bec5;line-height:1.7">{resumen}</div>',
        unsafe_allow_html=True
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    '<div style="text-align:center;font-size:11px;color:#3d4a5c;padding:10px">'
    'Fund Analyzer · Datos extraídos de CNMV · Actualizado automáticamente'
    '</div>',
    unsafe_allow_html=True
)
