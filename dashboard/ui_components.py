"""
UI Components — Executive theme for Fund Analyzer (dark/light aware)
All functions read from THEME dict; call set_theme() before rendering.
"""
import streamlit as st

# ── Default: dark executive palette ──────────────────────────────────────────
THEME: dict[str, str] = {
    "accent":  "#4fc3f7",
    "bg2":     "#13161e",
    "border":  "#1e2130",
    "text":    "#e8e8e8",
    "text2":   "#bbb",
    "text3":   "#666",
    "green":   "#4caf50",
    "red":     "#ef5350",
    "yellow":  "#f59e0b",
    "sbox_bg": "#0d0f14",
}


def set_theme(**kwargs: str) -> None:
    """Override theme tokens. Call once per rerun before rendering components."""
    THEME.update(kwargs)


# ── Helper: short aliases ─────────────────────────────────────────────────────
def _a() -> str: return THEME["accent"]
def _b2() -> str: return THEME["bg2"]
def _br() -> str: return THEME["border"]
def _t() -> str: return THEME["text"]
def _t2() -> str: return THEME["text2"]
def _t3() -> str: return THEME["text3"]


# ── Components ────────────────────────────────────────────────────────────────

def section_header(title: str, subtitle: str = "", accent_color: str = "") -> None:
    """Replaces all st.subheader / st.markdown('## ...')."""
    ac = accent_color or _a()
    sub_html = (
        f"<span style='font-size:11px; color:{_t3()}; margin-left:8px;'>{subtitle}</span>"
        if subtitle else ""
    )
    st.markdown(f"""
    <div style="display:flex; align-items:baseline; gap:12px;
                margin:1.6rem 0 0.8rem 0; padding-bottom:8px;
                border-bottom:1px solid {_br()};">
      <span style="font-family:'DM Mono',monospace; font-size:10px;
                   letter-spacing:0.12em; text-transform:uppercase;
                   color:{ac};">■</span>
      <span style="font-size:13px; font-weight:600; color:{_t()};
                   letter-spacing:0.03em;">{title}</span>
      {sub_html}
    </div>
    """, unsafe_allow_html=True)


def narrative_block(text: str, label: str = "SÍNTESIS") -> None:
    """Replaces st.info / st.write for long narrative text."""
    if not text:
        return
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:80px 1fr;
                gap:0; margin-bottom:1.1rem;">
      <div style="font-family:'DM Mono',monospace; font-size:9px;
                  letter-spacing:0.14em; text-transform:uppercase;
                  color:{_a()}; padding-top:3px;
                  border-right:1px solid {_br()};
                  padding-right:12px; line-height:1.5;">{label}</div>
      <div style="padding-left:16px; font-size:13px;
                  line-height:1.8; color:{_t2()};">{text}</div>
    </div>
    """, unsafe_allow_html=True)


def timeline_item(year: str, text: str, color: str = "") -> None:
    """Single-column timeline entry. Use ascending order (oldest first)."""
    if not text:
        return
    c = color or _a()
    year_html = (
        f"<span style='font-family:\"DM Mono\",monospace; font-size:11px;"
        f" font-weight:500; color:{c};'>{year}</span>"
        if year else
        f"<span style='color:{_br()};'>—</span>"
    )
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:52px 1px 1fr;
                gap:0 16px; margin-bottom:0.9rem; align-items:start;">
      <div style="text-align:right; padding-top:2px;">{year_html}</div>
      <div style="background:{_br()}; width:1px; min-height:100%;"></div>
      <div style="font-size:12.5px; line-height:1.7; color:{_t2()};
                  padding-top:2px;">{text}</div>
    </div>
    """, unsafe_allow_html=True)


def dual_timeline_item(year: str, tesis: str, decisiones: str) -> None:
    """Two-column timeline: Tesis ← Year → Decisiones. Use ascending order."""
    if not tesis and not decisiones:
        return
    t = tesis or f"<span style='color:{_t3()};'>—</span>"
    d = decisiones or f"<span style='color:{_t3()};'>—</span>"
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:1fr 56px 1fr;
                gap:12px; margin-bottom:1.4rem; align-items:start;">
      <div style="text-align:right; font-size:12px; line-height:1.7;
                  color:{_t3()}; padding:8px 12px; background:{_b2()};
                  border:1px solid {_br()}; border-radius:4px;">{t}</div>
      <div style="text-align:center; padding-top:8px;">
        <span style="font-family:'DM Mono',monospace; font-size:10px;
                     font-weight:600; color:{_b2()}; background:{_a()};
                     border-radius:3px; padding:3px 6px;">{year}</span>
      </div>
      <div style="font-size:12px; line-height:1.7; color:{_t3()};
                  padding:8px 12px; background:{_b2()};
                  border:1px solid {_br()}; border-radius:4px;">{d}</div>
    </div>
    """, unsafe_allow_html=True)


def consistency_period(period: str, score, content_fn) -> None:
    """Expander for a consistency period with a colored accent bar."""
    try:
        s = float(score) if score is not None else None
    except (TypeError, ValueError):
        s = None
    if s is not None:
        color = THEME["green"] if s >= 7 else THEME["red"] if s < 4 else THEME["yellow"]
        label = f"{period}  ·  Score {int(s)}/10"
    else:
        color = _br()
        label = period
    with st.expander(label):
        st.markdown(
            f'<div style="height:2px; background:{color}; '
            f'margin-bottom:12px; border-radius:1px;"></div>',
            unsafe_allow_html=True,
        )
        content_fn()


def stat_row(label: str, value: str) -> None:
    """One-line label → value row for metadata tables."""
    st.markdown(f"""
    <div style="display:flex; justify-content:space-between;
                padding:5px 0; border-bottom:1px solid {_br()};
                font-size:12px;">
      <span style="font-family:'DM Mono',monospace; font-size:10px;
                   letter-spacing:0.06em; text-transform:uppercase;
                   color:{_t3()};">{label}</span>
      <span style="color:{_t()};">{value}</span>
    </div>
    """, unsafe_allow_html=True)


def empty_state(message: str, command: str = "") -> None:
    """Displayed when a section has no data yet."""
    cmd_html = (
        f"<code style='font-family:\"DM Mono\",monospace; font-size:11px;"
        f" background:{THEME['sbox_bg']}; padding:2px 8px; border-radius:3px;"
        f" color:{_a()};'>{command}</code>"
        if command else ""
    )
    st.markdown(f"""
    <div style="background:{_b2()}; border:1px dashed {_br()};
                border-radius:4px; padding:18px 20px; color:{_t3()};
                font-family:'DM Mono',monospace; font-size:11px;
                letter-spacing:0.04em;">
      {message}
      {"<br><span style='margin-top:6px;display:inline-block'>" + cmd_html + "</span>" if cmd_html else ""}
    </div>
    """, unsafe_allow_html=True)
