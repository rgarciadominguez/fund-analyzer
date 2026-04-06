"""
UI Components — Executive dark terminal theme for Fund Analyzer
All functions emit raw HTML via st.markdown(unsafe_allow_html=True).
No logic, no data — pure presentation layer.
"""
import streamlit as st

_ACCENT  = "#4fc3f7"
_BG2     = "#13161e"
_BORDER  = "#1e2130"
_TEXT2   = "#bbb"
_TEXT3   = "#666"
_GREEN   = "#4caf50"
_RED     = "#ef5350"


def section_header(title: str, subtitle: str = "", accent_color: str = _ACCENT) -> None:
    """Replaces all st.subheader / st.markdown('## ...')."""
    sub_html = (
        f"<span style='font-size:11px; color:#444; margin-left:8px;'>{subtitle}</span>"
        if subtitle else ""
    )
    st.markdown(f"""
    <div style="display:flex; align-items:baseline; gap:12px;
                margin:1.6rem 0 0.8rem 0; padding-bottom:8px;
                border-bottom:1px solid {_BORDER};">
      <span style="font-family:'DM Mono',monospace; font-size:10px;
                   letter-spacing:0.12em; text-transform:uppercase;
                   color:{accent_color};">■</span>
      <span style="font-size:13px; font-weight:600; color:#e8e8e8;
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
                  color:{_ACCENT}; padding-top:3px;
                  border-right:1px solid {_BORDER};
                  padding-right:12px; line-height:1.5;">{label}</div>
      <div style="padding-left:16px; font-size:13px;
                  line-height:1.8; color:{_TEXT2};">{text}</div>
    </div>
    """, unsafe_allow_html=True)


def timeline_item(year: str, text: str, color: str = _ACCENT) -> None:
    """Single-column timeline entry. Order: ascending (oldest first)."""
    if not text:
        return
    year_html = (
        f"<span style='font-family:\"DM Mono\",monospace; font-size:11px;"
        f" font-weight:500; color:{color};'>{year}</span>"
        if year else
        f"<span style='color:{_BORDER};'>—</span>"
    )
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:52px 1px 1fr;
                gap:0 16px; margin-bottom:0.9rem; align-items:start;">
      <div style="text-align:right; padding-top:2px;">{year_html}</div>
      <div style="background:{_BORDER}; width:1px; min-height:100%;"></div>
      <div style="font-size:12.5px; line-height:1.7; color:#999;
                  padding-top:2px;">{text}</div>
    </div>
    """, unsafe_allow_html=True)


def dual_timeline_item(year: str, tesis: str, decisiones: str) -> None:
    """Two-column timeline: Tesis ← Year → Decisiones. Order: ascending."""
    if not tesis and not decisiones:
        return
    t = tesis or "<span style='color:#333'>—</span>"
    d = decisiones or "<span style='color:#333'>—</span>"
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:1fr 56px 1fr;
                gap:12px; margin-bottom:1.4rem; align-items:start;">
      <div style="text-align:right; font-size:12px; line-height:1.7;
                  color:#888; padding:8px 12px; background:{_BG2};
                  border:1px solid {_BORDER}; border-radius:4px;">{t}</div>
      <div style="text-align:center; padding-top:8px;">
        <span style="font-family:'DM Mono',monospace; font-size:10px;
                     font-weight:600; color:#0d0f14; background:{_ACCENT};
                     border-radius:3px; padding:3px 6px;">{year}</span>
      </div>
      <div style="font-size:12px; line-height:1.7; color:#888;
                  padding:8px 12px; background:{_BG2};
                  border:1px solid {_BORDER}; border-radius:4px;">{d}</div>
    </div>
    """, unsafe_allow_html=True)


def consistency_period(period: str, score, content_fn) -> None:
    """Expander for a consistency period with a colored accent bar."""
    try:
        s = float(score) if score is not None else None
    except (TypeError, ValueError):
        s = None

    if s is not None:
        color  = _GREEN if s >= 7 else _RED if s < 4 else "#f59e0b"
        label  = f"{period}  ·  Score {int(s)}/10"
    else:
        color  = _BORDER
        label  = period

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
                padding:5px 0; border-bottom:1px solid {_BORDER};
                font-size:12px;">
      <span style="font-family:'DM Mono',monospace; font-size:10px;
                   letter-spacing:0.06em; text-transform:uppercase;
                   color:{_TEXT3};">{label}</span>
      <span style="color:#e8e8e8;">{value}</span>
    </div>
    """, unsafe_allow_html=True)


def empty_state(message: str, command: str = "") -> None:
    """Displayed when a section has no data yet."""
    cmd_html = (
        f"<code style='font-family:\"DM Mono\",monospace; font-size:11px;"
        f" background:#0d0f14; padding:2px 8px; border-radius:3px;"
        f" color:{_ACCENT};'>{command}</code>"
        if command else ""
    )
    st.markdown(f"""
    <div style="background:{_BG2}; border:1px dashed {_BORDER};
                border-radius:4px; padding:18px 20px; color:{_TEXT3};
                font-family:'DM Mono',monospace; font-size:11px;
                letter-spacing:0.04em;">
      {message}
      {"<br><span style='margin-top:6px;display:inline-block'>" + cmd_html + "</span>" if cmd_html else ""}
    </div>
    """, unsafe_allow_html=True)
