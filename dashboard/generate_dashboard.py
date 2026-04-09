"""
Dashboard HTML Generator — reads output.json for ANY fund and produces HTML dashboard.
Applies all formatting rules learned from Avantage Fund pattern.
Usage: python generate_dashboard.py [ISIN]
"""
import json
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
ISIN = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
FUND_DIR = ROOT / "data" / "funds" / ISIN
OUTPUT = Path(__file__).parent / f"fund-{ISIN}.html"


def load_data():
    with open(FUND_DIR / "output.json", encoding="utf-8") as f:
        data = json.load(f)

    # ── Data resilience: fill gaps from available data ──
    cuant = data.setdefault("cuantitativo", {})

    # Compute/fix serie_vl_base100 from serie_aum VL data
    # Filter out anomalous VL values (>500 or <1 = data corruption)
    aum_series = cuant.get("serie_aum", [])
    valid_vl = [s for s in aum_series if s.get("vl") and 1 < s["vl"] < 500]

    # Also filter out partial-year periods like "202506" (keep only YYYY or YYYY-SX)
    valid_vl = [s for s in valid_vl if len(str(s.get("periodo", ""))) <= 7]

    if valid_vl:
        first_vl = valid_vl[0]["vl"]
        cuant["serie_vl_base100"] = [
            {"periodo": s.get("periodo", ""), "vl": s["vl"], "base100": round(s["vl"] / first_vl * 100, 1)}
            for s in valid_vl
        ]

    # Also clean serie_aum: filter anomalous entries
    if aum_series:
        cuant["serie_aum"] = [s for s in aum_series if len(str(s.get("periodo", ""))) <= 7]

    # If gestora is empty, try cnmv_data
    if not data.get("gestora"):
        cnmv_path = FUND_DIR / "cnmv_data.json"
        if cnmv_path.exists():
            try:
                cnmv = json.loads(cnmv_path.read_text(encoding="utf-8"))
                data["gestora"] = cnmv.get("gestora", "") or cnmv.get("gestora_pdf", "")
                # Also fill kpis if missing
                for k, v in cnmv.get("kpis", {}).items():
                    if v is not None and not data.get("kpis", {}).get(k):
                        data.setdefault("kpis", {})[k] = v
            except Exception:
                pass

    # If equipo is empty, try cnmv_data or manager_profile
    if not data.get("gestores", {}).get("equipo"):
        mgr_path = FUND_DIR / "manager_profile.json"
        if mgr_path.exists():
            try:
                mgr = json.loads(mgr_path.read_text(encoding="utf-8"))
                equipo = mgr.get("equipo_gestor", [])
                if equipo:
                    data.setdefault("gestores", {})["equipo"] = equipo
            except Exception:
                pass

    return data


import re as _re

def build_classes_table(data):
    """Build classes table dynamically from cuantitativo data."""
    cuant = data.get("cuantitativo", {})
    com_series = cuant.get("serie_comisiones_por_clase", [])
    ter_series = cuant.get("serie_ter_por_clase", [])
    isin = data.get("isin", "")

    # Get latest comisiones and TER per class
    clases = {}
    if com_series:
        latest_com = com_series[-1].get("clases", {})
        for cls, val in latest_com.items():
            clases.setdefault(cls, {})["com_gestion"] = val
    if ter_series:
        latest_ter = ter_series[-1].get("clases", {})
        for cls, val in latest_ter.items():
            clases.setdefault(cls, {})["ter"] = val

    if not clases:
        # Fallback: single class from kpis
        k = data.get("kpis", {})
        if k.get("coste_gestion_pct"):
            clases["A"] = {"com_gestion": k["coste_gestion_pct"], "ter": k.get("ter_pct")}

    if not clases:
        return '<p class="pr" style="color:var(--ink-4);font-style:italic;">Información de clases no disponible.</p>'

    rows = ""
    for cls_name in sorted(clases.keys()):
        cls_data = clases[cls_name]
        com = cls_data.get("com_gestion")
        ter = cls_data.get("ter")
        # Derive ISIN for class (heuristic: base ISIN with class suffix)
        cls_isin = isin  # Best we can do without explicit mapping
        rows += f'<tr><td><strong>Clase {cls_name}</strong></td><td>{cls_isin}</td><td>{p(com)}</td><td>{p(ter)}</td><td style="font-family:\'Source Sans 3\';font-size:12px;">—</td><td style="font-family:\'Source Sans 3\';font-size:12px;">—</td></tr>'

    return f"""<table class="rt mb20">
    <thead><tr><th>Clase</th><th>ISIN</th><th>Com. Gestión</th><th>TER</th><th>Dividendos</th><th>Mín. inversión</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>"""


def render_narrative_inline(text, fund_name=""):
    """Convert analyst_synthesis text (with **bold** markdown) to HTML paragraphs with subsection headers.
    Skips redundant title headers (e.g. 'RESUMEN EJECUTIVO: FONDO X') and avoids stacking headers."""
    if not text:
        return '<p class="pr" style="color:var(--ink-4);font-style:italic;">Sección pendiente de análisis. Ejecutar analyst_agent.</p>'

    # Clean fund name for comparison
    fund_lower = (fund_name or "").lower().split(",")[0].strip()

    paragraphs = text.split("\n\n")
    html = ""
    prev_was_header = False

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Detect header: line that is ONLY **bold text**
        is_header = para.startswith("**") and para.endswith("**") and para.count("**") == 2

        if is_header:
            header_text = para.strip("*").strip()
            header_lower = header_text.lower()

            # Skip redundant headers that just repeat the fund name or section title
            if fund_lower and fund_lower in header_lower:
                continue
            if any(skip in header_lower for skip in ["resumen ejecutivo", "informe analítico", "informe para comité"]):
                continue

            # Don't stack headers — if previous was also a header, skip this one
            if prev_was_header:
                continue

            html += f'<div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);margin-top:20px;">{header_text}</div>'
            prev_was_header = True
        else:
            formatted = _re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', para)
            html += f'<p class="pr">{formatted}</p>'
            prev_was_header = False

    return html


def f(val, d=0, s=""):
    """Spanish number format"""
    if val is None or val == "": return "—"
    if isinstance(val, str):
        try: val = float(val)
        except ValueError: return val
    r = f"{val:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return r + s


def p(val):
    return f(val, 1, "%") if val is not None else "—"


# ═══════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════

CSS = """<style>
:root{--ink:#0e0e0e;--ink-2:#2a2a2a;--ink-3:#555;--ink-4:#888;--ink-5:#bbb;--rule:#d0d0d0;--rule-light:#e8e8e8;--paper:#fafaf8;--paper-2:#f3f3f0;--paper-3:#ececea;--white:#fff;--navy:#0c2340;--navy-mid:#1a3a5c;--navy-pale:#e8eef5;--pos:#1a4d2e;--neg:#6b1a1a;--pos-bg:#f0f7f2;--neg-bg:#fdf2f2;}
[data-theme="dark"]{--ink:#e8e4dc;--ink-2:#c8c4bc;--ink-3:#908c84;--ink-4:#5c5850;--ink-5:#3c3830;--rule:#2e2c28;--rule-light:#252320;--paper:#111110;--paper-2:#181816;--paper-3:#1e1d1b;--white:#111110;--navy:#6a9ec8;--navy-mid:#4a7ea8;--navy-pale:#141e28;--pos:#2a7a44;--neg:#c04040;--pos-bg:#0e1a12;--neg-bg:#1a0e0e;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}html{font-size:14px;}
body{font-family:'Source Sans 3',sans-serif;background:var(--paper);color:var(--ink);-webkit-font-smoothing:antialiased;line-height:1.6;}
/* HEADER */
.lh{background:var(--navy);}
.lh-top{display:flex;align-items:stretch;border-bottom:1px solid rgba(255,255,255,0.10);}
.lh-left{padding:16px 28px;border-right:1px solid rgba(255,255,255,0.10);min-width:320px;display:flex;flex-direction:column;gap:4px;}
.lh-fund{font-family:'EB Garamond',serif;font-size:20px;font-weight:500;color:#fff;line-height:1.2;}
.lh-meta-line{font-size:10px;color:rgba(255,255,255,0.38);letter-spacing:0.3px;}
.lh-meta-line strong{color:rgba(255,255,255,0.7);font-weight:500;}
.lh-center{flex:1;display:flex;flex-direction:column;justify-content:center;padding:12px 24px;gap:3px;}
.lh-cv{font-family:'Source Code Pro',monospace;font-size:11px;color:rgba(255,255,255,0.65);display:flex;gap:6px;align-items:center;}
.lh-cl{font-size:9px;color:rgba(255,255,255,0.28);text-transform:uppercase;letter-spacing:0.8px;min-width:52px;}
.lh-right{display:flex;align-items:center;gap:24px;padding:14px 36px 14px 24px;border-left:1px solid rgba(255,255,255,0.07);margin-left:auto;}
.lh-rd{display:flex;flex-direction:column;align-items:center;gap:2px;}
.lh-rd-v{font-family:'Source Code Pro',monospace;font-size:11.5px;color:rgba(255,255,255,0.75);}
.lh-rd-l{font-size:8px;color:rgba(255,255,255,0.25);text-transform:uppercase;letter-spacing:0.8px;}
.lh-aum{text-align:right;}.lh-aum-v{font-family:'Source Code Pro',monospace;font-size:20px;color:#fff;letter-spacing:-0.5px;line-height:1;}
.lh-aum-l{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:rgba(255,255,255,0.28);margin-top:4px;}
.srri-pips{display:flex;gap:2px;}.srri-pip{width:9px;height:9px;border:1px solid rgba(255,255,255,0.20);}.srri-pip.on{background:rgba(255,255,255,0.65);border-color:rgba(255,255,255,0.65);}
.srri-l{font-size:8px;color:rgba(255,255,255,0.25);text-transform:uppercase;letter-spacing:0.8px;margin-top:2px;}
.theme-toggle{background:none;border:1px solid rgba(255,255,255,0.15);color:rgba(255,255,255,0.40);font-family:'Source Sans 3';font-size:11px;padding:5px 11px;cursor:pointer;white-space:nowrap;margin-left:12px;}
.theme-toggle:hover{color:rgba(255,255,255,0.75);border-color:rgba(255,255,255,0.30);}
/* TABS */
.tabbar{background:var(--navy);padding:0 28px;display:flex;border-top:1px solid rgba(255,255,255,0.06);overflow-x:auto;}.tabbar::-webkit-scrollbar{display:none;}
.tb{background:none;border:none;border-bottom:2px solid transparent;padding:9px 16px 8px;font-family:'Source Sans 3';font-size:11.5px;color:rgba(255,255,255,0.35);cursor:pointer;white-space:nowrap;transition:color 0.15s;}
.tb:hover{color:rgba(255,255,255,0.65);}.tb.on{color:rgba(255,255,255,0.88);border-bottom-color:rgba(255,255,255,0.55);}
/* BODY */
.body{max-width:1280px;margin:0 auto;padding:36px 36px 72px;}.pane{display:none;}.pane.on{display:block;}
.pane-header{display:flex;align-items:baseline;justify-content:space-between;padding-bottom:12px;margin-bottom:24px;border-bottom:2px solid var(--ink);}
.pane-h1{font-family:'EB Garamond',serif;font-size:26px;font-weight:400;color:var(--ink);letter-spacing:-0.3px;line-height:1;}
.pane-dl{font-size:10.5px;color:var(--ink-4);text-transform:uppercase;letter-spacing:0.8px;}
.sr{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--ink-4);margin:24px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--rule);}.sr:first-child{margin-top:0;}
.pr{font-size:13.5px;line-height:1.78;color:var(--ink-2);}.pr+.pr{margin-top:10px;}.pr strong{color:var(--ink);font-weight:600;}
.col2{display:grid;grid-template-columns:1fr 1fr;gap:20px;}.col3{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;}
.mb16{margin-bottom:16px;}.mb20{margin-bottom:20px;}.mb24{margin-bottom:24px;}
hr.hr{border:none;border-top:1px solid var(--rule);margin:24px 0;}
/* KPI */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);border-top:2px solid var(--rule);border-bottom:1px solid var(--rule);margin-bottom:20px;}
.kpi-cell{padding:12px 18px;border-right:1px solid var(--rule);}.kpi-cell:first-child{padding-left:0;}.kpi-cell:last-child{border-right:none;}
.kpi-label{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--ink-4);margin-bottom:4px;}
.kpi-value{font-family:'Source Code Pro',monospace;font-size:20px;font-weight:400;color:var(--ink);letter-spacing:-0.5px;line-height:1;}
.kpi-value.pos{color:var(--pos);}.kpi-value.neg{color:var(--neg);}.kpi-sub{font-size:10px;color:var(--ink-4);margin-top:4px;}
/* TABLE */
.rt{width:100%;border-collapse:collapse;font-size:13px;}.rt thead tr{border-bottom:1px solid var(--ink);}
.rt th{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--ink-3);padding:0 10px 7px;text-align:right;}.rt th:first-child{text-align:left;padding-left:0;}
.rt td{padding:8px 10px;text-align:right;font-family:'Source Code Pro',monospace;font-size:12px;color:var(--ink-2);border-bottom:1px solid var(--rule-light);}
.rt td:first-child{font-family:'Source Sans 3';font-size:13px;font-weight:500;text-align:left;color:var(--ink);padding-left:0;}
.rt tbody tr:hover td{background:var(--paper-2);}.pos-v{color:var(--pos);}.neg-v{color:var(--neg);}
/* PRINCIPLES */
.prin{margin-top:8px;}.prin-i{display:grid;grid-template-columns:20px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid var(--rule-light);align-items:baseline;}
.prin-i:last-child{border-bottom:none;}.prin-n{font-family:'Source Code Pro';font-size:10px;color:var(--ink-4);}
.prin-b{font-size:12.5px;color:var(--ink-2);line-height:1.5;}.prin-b strong{color:var(--ink);font-weight:600;}
/* TIMELINE — dashboard original style */
.timeline{position:relative;padding-left:32px;}
.timeline::before{content:'';position:absolute;left:9px;top:10px;bottom:10px;width:1px;background:var(--rule);}
.tl-item{position:relative;margin-bottom:28px;}
.tl-dot{position:absolute;left:-28px;top:4px;width:14px;height:14px;border-radius:50%;background:var(--paper);border:2.5px solid var(--navy-mid);z-index:1;}
.tl-dot.dot-hito{border-color:var(--pos);}.tl-dot.dot-strat{border-color:#d4920a;}.tl-dot.dot-market{border-color:var(--navy-mid);}.tl-dot.dot-crisis{border-color:var(--neg);}.tl-dot.dot-reg{border-color:var(--ink-4);}
.tl-date{font-family:'Source Code Pro',monospace;font-size:11px;color:var(--ink-4);margin-bottom:5px;}
.tl-tag{display:inline-block;font-size:9px;font-weight:600;letter-spacing:0.5px;padding:2px 8px;border-radius:4px;margin-bottom:6px;text-transform:uppercase;}
.tag-hito{background:var(--pos-bg);color:var(--pos);}.tag-strat{background:#fdf5e0;color:#8a6a00;}.tag-market{background:var(--navy-pale);color:var(--navy-mid);}.tag-crisis{background:var(--neg-bg);color:var(--neg);}.tag-reg{background:var(--paper-3);color:var(--ink-4);}
[data-theme="dark"] .tag-hito{background:#0a2a1c;color:#4ecf99;}[data-theme="dark"] .tag-strat{background:#2a200a;color:#e8c56c;}[data-theme="dark"] .tag-market{background:#141e28;color:#6a9ec8;}[data-theme="dark"] .tag-crisis{background:#1a0e0e;color:#c04040;}[data-theme="dark"] .tag-reg{background:#1e1d1b;color:#908c84;}
.tl-title{font-size:14px;font-weight:600;color:var(--ink);margin-bottom:5px;}.tl-desc{font-size:13px;color:var(--ink-3);line-height:1.65;}
/* MANAGER */
.mgr{display:grid;grid-template-columns:170px 1fr;border-top:1px solid var(--rule);padding:18px 0;}
.mgr:last-of-type{border-bottom:1px solid var(--rule);}
.mgr-s{padding-right:20px;border-right:1px solid var(--rule);}
.mgr-av{width:48px;height:48px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:'EB Garamond',serif;font-size:18px;color:#fff;margin-bottom:8px;}
.mgr-nm{font-family:'EB Garamond',serif;font-size:17px;color:var(--ink);line-height:1.2;margin-bottom:3px;}
.mgr-rl{font-size:10px;color:var(--ink-4);text-transform:uppercase;letter-spacing:0.4px;line-height:1.4;margin-bottom:8px;}
.mgr-cv{font-size:11px;color:var(--ink-3);line-height:1.5;}.mgr-cv li{margin-bottom:2px;}
.mgr-b{padding-left:20px;}
/* STRATEGY MATRIX */
.strat-row{display:grid;grid-template-columns:100px 1fr 1fr 1fr;border-top:1px solid var(--rule-light);}
.strat-row:first-of-type{border-top:1px solid var(--rule);}
.strat-yr{padding:12px 12px;font-family:'Source Code Pro';font-size:12px;font-weight:500;color:var(--navy);border-right:1px solid var(--rule-light);background:var(--navy-pale);white-space:nowrap;}
.strat-c{padding:12px 10px;font-size:12px;color:var(--ink-2);line-height:1.6;border-right:1px solid var(--rule-light);}
.strat-c:last-child{border-right:none;}.strat-c strong{color:var(--ink);font-weight:600;}
/* PORTFOLIO TABLE */
.pt-wrap{overflow-x:auto;}.pt{width:100%;border-collapse:collapse;white-space:nowrap;}
.pt thead tr{border-bottom:2px solid var(--ink);}
.pt th{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:var(--ink-2);padding:0 8px 8px;text-align:left;}
.pt td{padding:7px 8px;font-family:'Source Code Pro';font-size:11px;color:var(--ink-2);text-align:right;border-bottom:1px solid var(--rule-light);}
.pt td:first-child{font-family:'Source Sans 3';font-size:12.5px;font-weight:500;color:var(--ink);text-align:left;padding-left:0;}
.pt tbody tr:hover td{background:var(--paper-2);}
.wbar{display:flex;align-items:center;gap:5px;}.wfill{height:4px;border-radius:1px;}
.tp-rv{background:#1a3a5c;color:#fff;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:600;font-family:'Source Sans 3';}
.tp-rf{background:#8c3214;color:#fff;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:600;font-family:'Source Sans 3';}
.tp-otro{background:#8a6a00;color:#fff;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:600;font-family:'Source Sans 3';}
.delta-new{font-size:9px;font-weight:600;color:var(--pos);background:var(--pos-bg);padding:1px 5px;border-radius:2px;}
.delta-up{color:var(--pos);}.delta-down{color:var(--neg);}
/* SOURCES — card style */
.src-card{background:var(--paper-2);border:1px solid var(--rule);border-radius:8px;padding:18px 22px;margin-bottom:14px;}
.src-card:hover{border-color:var(--navy-mid);}
.src-head{display:flex;align-items:center;gap:14px;margin-bottom:10px;}
.src-logo{width:40px;height:40px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-family:'Source Sans 3';font-size:13px;font-weight:700;color:#fff;flex-shrink:0;}
.src-info{flex:1;}
.src-o{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;color:var(--navy);line-height:1.3;}
.src-author{font-size:11px;color:var(--ink-4);margin-top:1px;}
.src-date{font-family:'Source Code Pro';font-size:10px;color:var(--ink-4);flex-shrink:0;}
.src-t{font-size:14px;font-weight:600;color:var(--ink);margin-bottom:8px;line-height:1.35;}
.exp-btn{background:none;border:none;cursor:pointer;color:var(--navy);font-size:11px;font-family:'Source Sans 3';padding:0;display:flex;align-items:center;gap:4px;margin-bottom:6px;}
.exp-body{display:none;font-size:12.5px;color:var(--ink-3);line-height:1.65;background:var(--paper-3);border-radius:6px;padding:12px 14px;margin-bottom:10px;}
.exp-body.open{display:block;}
.src-lnk{display:inline-flex;align-items:center;gap:4px;font-size:11px;color:var(--navy);text-decoration:none;border:1px solid var(--navy);border-radius:4px;padding:4px 10px;transition:background 0.15s;}
.src-lnk:hover{background:var(--navy-pale);}
/* DOCS */
.doc-grp{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1.2px;color:var(--ink-3);padding:18px 0 5px;border-bottom:1px solid var(--rule);}
.doc-grp:first-child{padding-top:0;}
.doc-r{display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid var(--rule-light);}
.doc-r:hover{background:var(--paper-2);}
.doc-ext{font-family:'Source Code Pro';font-size:8px;font-weight:600;color:var(--ink-4);background:var(--paper-3);padding:2px 4px;min-width:26px;text-align:center;flex-shrink:0;}
.doc-nm{font-size:12px;color:var(--ink);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.doc-mt{font-family:'Source Code Pro';font-size:9px;color:var(--ink-4);}
.doc-a{font-size:10px;color:var(--navy);text-decoration:none;flex-shrink:0;}
/* CHART */
.ch-b{margin-bottom:20px;padding:12px 8px 8px;}.ch-l{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1.1px;color:var(--ink-4);margin-bottom:14px;border-bottom:1px solid var(--rule-light);padding-bottom:5px;}
.ch-h{height:180px;position:relative;padding:4px 0;}.ch-hm{height:220px;position:relative;padding:4px 0;}
/* CHART SELECTOR */
.ch-sel{display:inline-flex;align-items:center;gap:6px;float:right;font-size:10px;color:var(--ink-4);}
.ch-sel select{background:var(--paper-2);border:1px solid var(--rule);padding:2px 6px;font-size:10px;font-family:'Source Sans 3';color:var(--ink);}
/* RESPONSIVE */
@media(max-width:900px){.lh-top{flex-wrap:wrap;}.lh-center{display:none;}.body{padding:20px 16px;}.col2,.col3{grid-template-columns:1fr;}.kpi-row{grid-template-columns:1fr 1fr;}.mgr{grid-template-columns:1fr;}.strat-row{grid-template-columns:60px 1fr;}}
</style>"""


# ═══════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════

def format_date(date_str):
    """Convert '31/07/2014' or '22/09/2017' to 'Julio 2014' or 'Septiembre 2017'"""
    months = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
              7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    if not date_str:
        return "—"
    try:
        parts = date_str.split("/")
        if len(parts) == 3:
            return f"{months.get(int(parts[1]), parts[1])} {parts[2]}"
    except Exception:
        pass
    return date_str


def build_header(data):
    k = data.get("kpis", {})
    srri = k.get("perfil_riesgo", 3) or 3
    pips = "".join(f'<div class="srri-pip{" on" if i < srri else ""}"></div>' for i in range(7))

    nombre = data.get("nombre", "Fondo sin nombre")
    gestora = data.get("gestora", k.get("gestora", ""))
    depositario = k.get("depositario", "")
    isin = data.get("isin", "")
    fecha_inicio = format_date(k.get("fecha_registro", ""))
    divisa = k.get("divisa", "EUR")
    # Try to get Morningstar category from analyst text, fallback to CNMV clasificacion
    raw_clasif = k.get("clasificacion", "")
    nombre_lower = nombre.lower()
    # Infer category from fund name and type
    if "cartera permanente" in nombre_lower:
        clasificacion = "Mixto Moderado Global"
    elif "flexible" in nombre_lower or "mixto" in nombre_lower:
        clasificacion = "Mixto Flexible Global"
    elif raw_clasif and raw_clasif.lower() != "global":
        clasificacion = raw_clasif
    else:
        clasificacion = "Mixto Flexible Global" if raw_clasif == "Global" else raw_clasif
    # Morningstar stars: only show if we know the rating
    stars = "★★★★★" if k.get("rating_morningstar") else ""

    # Gestor principal: from gestores.equipo[0] or analyst_synthesis
    equipo = data.get("gestores", {}).get("equipo", [])
    gestores_str = " · ".join(equipo[:3]) if equipo else ""

    return f"""
<header class="lh">
  <div class="lh-top">
    <!-- ZONA 1: Nombre, gestora, depositario -->
    <div class="lh-left">
      <div class="lh-fund">{nombre} {f'<span style="color:rgba(255,255,255,0.45);font-size:14px;margin-left:4px;">{stars}</span>' if stars else ''}</div>
      {f'<div class="lh-meta-line" style="margin-top:3px;">Gestora: <strong>{gestora}</strong></div>' if gestora else ''}
      {f'<div class="lh-meta-line">Depositario: <strong>{depositario}</strong></div>' if depositario else ''}
    </div>

    <!-- ZONA 2: AUM + Riesgo UCITS -->
    <div style="display:flex;align-items:center;gap:28px;padding:14px 32px;border-left:1px solid rgba(255,255,255,0.08);">
      <div class="lh-aum">
        <div class="lh-aum-v">€{f(k.get('aum_actual_meur'),1)}M</div>
        <div class="lh-aum-l">AUM</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px;">
        <div class="srri-pips">{pips}</div>
        <span style="font-size:10px;color:rgba(255,255,255,0.50);letter-spacing:0.5px;font-weight:500;">SRRI {srri} / 7</span>
      </div>
    </div>

    <!-- ZONA 3: Inicio, Categoría, Gestor, Divisa -->
    <div style="display:flex;flex-direction:column;justify-content:center;gap:2px;padding:14px 28px;border-left:1px solid rgba(255,255,255,0.08);">
      {f'<div class="lh-cv"><span class="lh-cl">Inicio</span> <span style="color:rgba(255,255,255,0.80);">{fecha_inicio}</span></div>' if fecha_inicio else ''}
      {f'<div class="lh-cv"><span class="lh-cl">Categoría</span> <span style="color:rgba(255,255,255,0.80);">{clasificacion}</span></div>' if clasificacion else ''}
      {f'<div class="lh-cv"><span class="lh-cl">Gestores</span> <span style="color:rgba(255,255,255,0.80);">{gestores_str}</span></div>' if gestores_str else ''}
      <div class="lh-cv"><span class="lh-cl">Divisa</span> <span style="color:rgba(255,255,255,0.80);">{divisa}</span></div>
    </div>

    <!-- ZONA 4: Botón dark/light -->
    <div style="display:flex;align-items:center;padding:14px 24px;border-left:1px solid rgba(255,255,255,0.08);margin-left:auto;">
      <button class="theme-toggle" onclick="toggleTheme()"><span id="thlbl">Modo oscuro</span></button>
    </div>
  </div>
  <nav class="tabbar">
    <button class="tb on" onclick="goTab(0,this)">Resumen</button>
    <button class="tb" onclick="goTab(1,this)">Historia</button>
    <button class="tb" onclick="goTab(2,this)">Gestores</button>
    <button class="tb" onclick="goTab(3,this)">Evolución</button>
    <button class="tb" onclick="goTab(4,this)">Estrategia</button>
    <button class="tb" onclick="goTab(5,this)">Cartera</button>
    <button class="tb" onclick="goTab(6,this)">Fuentes externas</button>
    <button class="tb" onclick="goTab(7,this)">Documentos</button>
  </nav>
</header>"""


# ═══════════════════════════════════════════════════════════════
# TAB 1: RESUMEN
# ═══════════════════════════════════════════════════════════════

def build_tab_resumen(data):
    s = data.get("analyst_synthesis", {}).get("resumen", {})
    k = data.get("kpis", {})
    cuant = data.get("cuantitativo", {})
    vl = cuant.get("serie_vl_base100", [])

    # Rentabilidades
    ret_cells = ""
    ret_ths = ""
    for i in range(1, len(vl)):
        prev, curr = vl[i-1].get("base100",0), vl[i].get("base100",0)
        if prev > 0:
            r = round((curr/prev-1)*100, 1)
            c = "pos-v" if r >= 0 else "neg-v"
            sg = "+" if r >= 0 else ""
            ret_cells += f"<td class='{c}'>{sg}{f(r,1)}%</td>"
            ret_ths += f"<th>{vl[i].get('periodo','')}</th>"

    fort = "".join(f'<div class="prin-i"><span class="prin-n">✓</span><span class="prin-b">{x}</span></div>' for x in s.get("fortalezas",[]))
    risk = "".join(f'<div class="prin-i"><span class="prin-n">⚠</span><span class="prin-b">{x}</span></div>' for x in s.get("riesgos",[]))

    # Narrative from analyst_synthesis (generated by Gemini, fund-specific)
    texto_resumen = s.get("texto", "")
    # Parse into paragraphs for Narrative-style rendering
    # Uses global render_narrative_inline

    # Compromiso gestor
    compromiso = s.get("compromiso_gestor", "")
    para_quien = s.get("para_quien_es", "")

    return f"""
<section class="pane on" id="p0">
  <div class="pane-header"><h1 class="pane-h1">Resumen ejecutivo</h1><span class="pane-dl">Informe analítico</span></div>

  <div class="mb24">
    {render_narrative_inline(texto_resumen, data.get("nombre",""))}
  </div>

  {f'''<div class="col2 mb20">
    {f'<div><div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Para quién es adecuado</div><p class="pr">{para_quien}</p></div>' if para_quien else ''}
    {f'<div><div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Compromiso del gestor</div><p class="pr">{compromiso}</p></div>' if compromiso else ''}
  </div>''' if para_quien or compromiso else ''}

  <div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Rentabilidad y volatilidad anual <span style="font-weight:400;font-size:8px;letter-spacing:0;">(fuente: Morningstar · datos diarios)</span></div>
  <div class="col2 mb20">
    <div class="ch-b"><div class="ch-hm"><canvas id="mst-ret"></canvas></div><div id="mst-ret-note" style="font-size:10px;color:var(--ink-4);font-style:italic;margin-top:4px;"></div></div>
    <div class="ch-b"><div class="ch-hm"><canvas id="mst-vol"></canvas></div></div>
  </div>

  <div class="col2 mb20">
    <div>
      <div class="sr" style="color:var(--pos);">Fortalezas</div>
      <div class="prin">{fort}</div>
    </div>
    <div>
      <div class="sr" style="color:var(--neg);">Riesgos</div>
      <div class="prin">{risk}</div>
    </div>
  </div>

  <div class="sr">Clases disponibles</div>
  {build_classes_table(data)}

  <div class="sr">Evolución de comisiones <span class="ch-sel"><label>Clase:</label><select id="com-sel" onchange="buildComChart()"><option value="A">A</option><option value="B">B</option></select></span></div>
  <div class="ch-b"><div class="ch-h"><canvas id="c-com"></canvas></div>
    <p style="font-size:10px;color:var(--ink-4);margin-top:6px;font-style:italic;">* Datos de 2014-2015 excluidos por inconsistencia entre TER y comisión de gestión en fuentes CNMV.</p>
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 2: HISTORIA
# ═══════════════════════════════════════════════════════════════

def build_tab_historia(data):
    import re
    s = data.get("analyst_synthesis", {}).get("historia", {})
    hitos = s.get("hitos", [])
    k = data.get("kpis", {})
    cuant = data.get("cuantitativo", {})
    vl = cuant.get("serie_vl_base100", [])

    # Narrative from analyst_synthesis
    texto = s.get("texto", "")

    # Uses global render_narrative_inline

    # Calculate KPIs dynamically
    years_since = ""
    if k.get("anio_creacion"):
        years_since = str(datetime.now().year - int(k["anio_creacion"]))
    fecha_inicio = format_date(k.get("fecha_registro", ""))

    # CAGR from VL
    cagr_str = "—"
    if len(vl) >= 2:
        first_vl = vl[0].get("base100", 100)
        last_vl = vl[-1].get("base100", 100)
        n_years = len(vl) - 1
        if first_vl > 0 and n_years > 0:
            cagr = ((last_vl / first_vl) ** (1 / n_years) - 1) * 100
            cagr_str = f"~{f(cagr, 1)}%"

    # Best/worst year
    best_yr, best_ret, worst_yr, worst_ret = "", 0, "", 0
    for i in range(1, len(vl)):
        prev = vl[i - 1].get("base100", 0)
        curr = vl[i].get("base100", 0)
        if prev > 0:
            ret = (curr / prev - 1) * 100
            yr = vl[i].get("periodo", "")
            if ret > best_ret:
                best_ret = ret
                best_yr = yr
            if ret < worst_ret:
                worst_ret = ret
                worst_yr = yr

    # Timeline from analyst hitos (dynamic, not hardcoded)
    tl = ""
    for h in hitos:
        anio = h.get("anio", "")
        evento = h.get("evento", "")
        # Auto-classify by keywords
        dot_cls = "dot-hito"
        tag_cls = "tag-hito"
        tag_text = "Hito"
        ev_lower = evento.lower()
        if any(w in ev_lower for w in ["crisis", "salida", "caída", "pérdida", "negativ"]):
            dot_cls = "dot-crisis"; tag_cls = "tag-crisis"; tag_text = "Crisis"
        elif any(w in ev_lower for w in ["estrateg", "cobertura", "covid", "rotación", "cambio"]):
            dot_cls = "dot-strat"; tag_cls = "tag-strat"; tag_text = "Estrategia"
        elif any(w in ev_lower for w in ["regulat", "cnmv", "folleto", "registro"]):
            dot_cls = "dot-reg"; tag_cls = "tag-reg"; tag_text = "Regulatorio"

        # Split evento into title (first ~80 chars) and desc (rest)
        title = evento[:80]
        desc = evento[80:] if len(evento) > 80 else ""

        tl += f"""
    <div class="tl-item">
      <div class="tl-dot {dot_cls}"></div>
      <div class="tl-date">{anio}</div>
      <div class="tl-tag {tag_cls}">{tag_text}</div>
      <div class="tl-title">{title}</div>
      <div class="tl-desc">{desc}</div>
    </div>"""

    return f"""
<section class="pane" id="p1">
  <div class="pane-header"><h1 class="pane-h1">Historia del fondo</h1><span class="pane-dl">{fecha_inicio} — presente</span></div>

  <div class="mb24">
    {render_narrative_inline(texto, data.get("nombre",""))}
  </div>

  <div class="kpi-row">
    <div class="kpi-cell"><div class="kpi-label">Años desde inicio</div><div class="kpi-value">{years_since or '—'}</div><div class="kpi-sub">{fecha_inicio} — presente</div></div>
    <div class="kpi-cell"><div class="kpi-label">CAGR desde inicio</div><div class="kpi-value pos">{cagr_str}</div><div class="kpi-sub">Neto de comisiones</div></div>
    <div class="kpi-cell"><div class="kpi-label">Peor año</div><div class="kpi-value neg">{f(worst_ret,1)}%</div><div class="kpi-sub">{worst_yr}</div></div>
    <div class="kpi-cell"><div class="kpi-label">Mejor año</div><div class="kpi-value pos">+{f(best_ret,1)}%</div><div class="kpi-sub">{best_yr}</div></div>
  </div>

  <div class="col3 mb20">
    <div class="ch-b"><div class="ch-l">AUM (M€)</div><div class="ch-h"><canvas id="c-aum"></canvas></div></div>
    <div class="ch-b"><div class="ch-l">Partícipes</div><div class="ch-h"><canvas id="c-part"></canvas></div></div>
    <div class="ch-b"><div class="ch-l">VL Base 100</div><div class="ch-h"><canvas id="c-vl"></canvas></div></div>
  </div>


  <div class="sr">Cronología de eventos relevantes</div>
  <div class="timeline">{tl}
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 3: GESTORES
# ═══════════════════════════════════════════════════════════════

def build_tab_gestores(data):
    import re
    s = data.get("analyst_synthesis", {}).get("gestores", {})
    perfiles = s.get("perfiles", [])
    texto = s.get("texto", "")

    # Uses global render_narrative_inline

    # Avatar colors cycling
    colors = ["linear-gradient(135deg,#1a3a5c,#2c4a6e)", "linear-gradient(135deg,#1e5a8a,#2d8cf0)",
              "linear-gradient(135deg,#2c6e49,#4ecf99)", "#3d5a80", "#5c5850"]

    mgrs_html = ""
    for i, pr in enumerate(perfiles):
        nombre = pr.get("nombre", "")
        cargo = pr.get("cargo", "")
        initials = "".join(w[0] for w in nombre.split()[:2]) if nombre else "?"
        bg = colors[i % len(colors)]
        trayectoria = pr.get("trayectoria", "")
        filosofia = pr.get("filosofia", "")
        decisiones = pr.get("decisiones_clave", []) or []
        rasgos = pr.get("rasgos_diferenciales", "")

        is_lead = (i == 0)  # First profile = lead manager

        if is_lead:
            # Extensive format for lead manager
            dec_html = ""
            for d in decisiones:
                d_formatted = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', d)
                dec_html += f'<p class="pr" style="font-size:12px;margin-bottom:6px;padding-left:12px;border-left:2px solid var(--navy-pale);">{d_formatted}</p>'

            mgrs_html += f"""
    <div class="mgr">
      <div class="mgr-s">
        <div class="mgr-av" style="background:{bg};">{initials}</div>
        <div class="mgr-nm">{nombre}</div>
        <div class="mgr-rl">{cargo}</div>
      </div>
      <div class="mgr-b">
        {f'<p class="pr" style="font-size:13px;">{re.sub(chr(42)+chr(42)+r"([^*]+)"+chr(42)+chr(42), r"<strong>" + chr(92) + "1</strong>", trayectoria)}</p>' if trayectoria else ''}
        {f'<p class="pr" style="font-size:13px;font-style:italic;border-left:2px solid var(--navy-pale);padding-left:10px;">{filosofia}</p>' if filosofia else ''}
        {f'<div class="sr" style="margin-top:12px;">Decisiones clave</div>{dec_html}' if dec_html else ''}
        {f'<p class="pr" style="font-size:12.5px;margin-top:10px;"><strong>Rasgos diferenciales:</strong> {rasgos}</p>' if rasgos else ''}
      </div>
    </div>"""
        elif decisiones or trayectoria:
            # Medium format: has some content
            mgrs_html += f"""
    <div class="mgr">
      <div class="mgr-s">
        <div class="mgr-av" style="background:{bg};">{initials}</div>
        <div class="mgr-nm">{nombre}</div>
        <div class="mgr-rl">{cargo}</div>
      </div>
      <div class="mgr-b">
        <p class="pr" style="font-size:12.5px;">{trayectoria or 'Miembro del equipo.'}</p>
      </div>
    </div>"""
        else:
            # Compact format: minimal info — collect for inline display
            mgrs_html += f"""
    <div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-top:1px solid var(--rule);color:var(--ink-3);font-size:12px;">
      <div style="width:32px;height:32px;border-radius:50%;background:{bg};display:flex;align-items:center;justify-content:center;font-family:'EB Garamond';font-size:13px;color:#fff;flex-shrink:0;">{initials}</div>
      <div><strong style="color:var(--ink);font-size:12.5px;">{nombre}</strong> — {cargo}</div>
    </div>"""

    if not perfiles:
        mgrs_html = '<p class="pr" style="color:var(--ink-4);font-style:italic;">Información de gestores pendiente. Ejecutar manager_deep_agent para obtener perfiles.</p>'

    return f"""
<section class="pane" id="p2">
  <div class="pane-header"><h1 class="pane-h1">Equipo gestor</h1><span class="pane-dl">Composición actual</span></div>

  <div class="mb24">
    {render_narrative_inline(texto, data.get("nombre",""))}
  </div>

  {mgrs_html}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 4: EVOLUCIÓN (vacía)
# ═══════════════════════════════════════════════════════════════

def build_tab_evolucion(data):
    return """
<section class="pane" id="p3">
  <div class="pane-header"><h1 class="pane-h1">Evolución del fondo</h1><span class="pane-dl">Datos diarios · Morningstar</span></div>

  <div class="mb20"><p class="pr">Análisis cuantitativo basado en <strong>datos diarios de Morningstar</strong>. Las métricas de volatilidad se calculan desde retornos mensuales (fin de mes) para alinearse con la metodología estándar de Morningstar y Finect. Los rolling son configurables por periodo.</p></div>

  <div id="mst-loading" style="text-align:center;padding:40px 0;color:var(--ink-4);font-size:13px;">Cargando datos de Morningstar...</div>

  <div id="mst-evo-content" style="display:none;">
    <!-- KPIs -->
    <div id="mst-evo-kpis" class="kpi-row mb20"></div>

    <!-- Fila 1: Rentabilidad + Volatilidad anuales -->
    <div class="col2 mb20">
      <div class="ch-b"><div class="ch-l">Rentabilidad anual</div><div class="ch-hm"><canvas id="mst-evo-ret"></canvas></div></div>
      <div class="ch-b"><div class="ch-l">Volatilidad positiva / negativa anual</div><div class="ch-hm"><canvas id="mst-evo-vol"></canvas></div></div>
    </div>

    <!-- Fila 2: Drawdown + Evolución histórica -->
    <div class="col2 mb20">
      <div class="ch-b"><div class="ch-l">Drawdown desde máximos (diario)</div><div class="ch-hm"><canvas id="mst-dd"></canvas></div></div>
      <div class="ch-b"><div class="ch-l">Evolución histórica — Base 100</div><div class="ch-hm"><canvas id="mst-growth"></canvas></div></div>
    </div>

    <!-- Fila 3: Rolling dinámicos -->
    <div class="col2 mb20">
      <div class="ch-b">
        <div class="ch-l" style="display:flex;justify-content:space-between;align-items:center;">
          <span>Rentabilidad rolling (anualizada)</span>
          <select id="mst-roll-ret-sel" onchange="updateRollingRet()" style="font-size:10px;padding:2px 6px;border:1px solid var(--rule);background:var(--paper-2);color:var(--ink);font-family:'Source Sans 3';">
            <option value="12">1 año</option>
            <option value="36" selected>3 años</option>
            <option value="60">5 años</option>
            <option value="120">10 años</option>
          </select>
        </div>
        <div class="ch-hm"><canvas id="mst-roll-ret"></canvas></div>
      </div>
      <div class="ch-b">
        <div class="ch-l" style="display:flex;justify-content:space-between;align-items:center;">
          <span>Volatilidad rolling</span>
          <select id="mst-roll-vol-sel" onchange="updateRollingVol()" style="font-size:10px;padding:2px 6px;border:1px solid var(--rule);background:var(--paper-2);color:var(--ink);font-family:'Source Sans 3';">
            <option value="12" selected>12 meses</option>
            <option value="36">3 años</option>
            <option value="60">5 años</option>
          </select>
        </div>
        <div class="ch-hm"><canvas id="mst-roll-vol"></canvas></div>
      </div>
    </div>
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 5: ESTRATEGIA
# ═══════════════════════════════════════════════════════════════

def build_tab_estrategia(data):
    import re
    s = data.get("analyst_synthesis", {}).get("estrategia", {})
    texto = s.get("texto", "")
    resumen = s.get("estrategia_actual_resumen", "")
    hitos = s.get("hitos_estrategia", [])

    # Uses global render_narrative_inline

    # Build hitos as timeline cards (from analyst_synthesis, not hardcoded)
    hitos_html = ""
    if hitos:
        for h in hitos:
            periodo = h.get("periodo", "")
            cambio = h.get("cambio", "")
            cambio_fmt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', cambio)
            hitos_html += f"""<div class="strat-row">
  <div class="strat-yr">{periodo}</div>
  <div class="strat-c" style="grid-column:span 3;">{cambio_fmt}</div>
</div>"""

    # Strategy summary
    resumen_html = ""
    if resumen:
        resumen_html = f"""
  <div style="border-left:3px solid var(--navy);padding-left:14px;margin-bottom:24px;">
    <div class="sr" style="margin-top:0;color:var(--navy);border-bottom-color:var(--navy);">Estrategia actual</div>
    <p class="pr" style="font-size:12.5px;">{resumen}</p>
  </div>"""

    return f"""
<section class="pane" id="p4">
  <div class="pane-header"><h1 class="pane-h1">Estrategia y coherencia</h1><span class="pane-dl">Evaluación estratégica</span></div>

  <div class="mb24">
    {render_narrative_inline(texto, data.get("nombre",""))}
  </div>

  {resumen_html}

  {f'''<div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Hitos estratégicos</div>
  <div style="display:grid;grid-template-columns:100px 1fr;border-bottom:2px solid var(--ink);margin-bottom:0;">
    <div style="padding:8px 12px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--navy);background:var(--navy-pale);">Periodo</div>
    <div style="padding:8px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-2);">Descripción</div>
  </div>
  {hitos_html}''' if hitos_html else ''}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 6: CARTERA
# ═══════════════════════════════════════════════════════════════

def build_tab_cartera(data):
    s = data.get("analyst_synthesis", {}).get("cartera", {})
    pos_actual = data.get("posiciones", {}).get("actuales", [])
    pos_hist = data.get("posiciones", {}).get("historicas", [])
    sorted_pos = sorted(pos_actual, key=lambda x: x.get("peso_pct",0) or 0, reverse=True)

    # Compute historical stats for charts (JS data)
    hist_years = []
    hist_npos = []
    hist_top5 = []
    hist_top10 = []
    hist_top15 = []
    for h in sorted(pos_hist, key=lambda x: x.get("periodo","")):
        todas = h.get("todas", [])
        if not todas:
            continue
        yr = h.get("periodo", "")
        weights = sorted([x.get("peso_pct",0) or 0 for x in todas], reverse=True)
        hist_years.append(yr[-2:] if len(yr) >= 4 else yr)
        hist_npos.append(len(todas))
        hist_top5.append(round(sum(weights[:5]),1))
        hist_top10.append(round(sum(weights[:10]),1))
        hist_top15.append(round(sum(weights[:15]),1))

    # Compute variations vs previous period
    prev_positions = {}
    if len(pos_hist) >= 2:
        # Sort by periodo, get second-to-last
        sorted_hist = sorted(pos_hist, key=lambda x: x.get("periodo",""))
        prev_todas = sorted_hist[-2].get("todas", []) if len(sorted_hist) >= 2 else []
        for pp in prev_todas:
            name = pp.get("nombre", "")
            if name:
                prev_positions[name] = pp.get("peso_pct", 0) or 0

    # Historical averages
    avg_npos = round(sum(hist_npos)/len(hist_npos),0) if hist_npos else 0
    avg_top10 = round(sum(hist_top10)/len(hist_top10),1) if hist_top10 else 0

    # Current stats
    cur_weights = sorted([x.get("peso_pct",0) or 0 for x in sorted_pos], reverse=True)
    cur_top10 = round(sum(cur_weights[:10]),1)
    # Liquidez from mix_activos
    mix = data.get("cuantitativo",{}).get("mix_activos_historico",[])
    liq = mix[0].get("liquidez_pct",0) if mix else 0
    # RV exposure
    rv = mix[0].get("rv_pct",0) if mix else 0

    # Table rows
    rows = ""
    cum = 0
    for i, pos in enumerate(sorted_pos):
        w = pos.get("peso_pct",0) or 0
        cum += w
        tipo = pos.get("tipo","")
        tipo_cls = "tp-rf" if tipo == "BONO" else "tp-rv" if tipo == "ACCIONES" else "tp-otro"
        tipo_lbl = "RF" if tipo == "BONO" else "RV" if tipo == "ACCIONES" else tipo[:3]

        # Variation
        name = pos.get("nombre","")
        prev_w = prev_positions.get(name, None)
        if prev_w is None:
            delta_html = '<span class="delta-new">NUEVO</span>'
        else:
            delta = w - prev_w
            if abs(delta) < 0.05:
                delta_html = '<span style="color:var(--ink-5);">—</span>'
            else:
                sign = "+" if delta > 0 else ""
                # Color intensity
                intensity = min(abs(delta) / 3, 1)  # normalize to 0-1
                if delta > 0:
                    bg = f"rgba(26,77,46,{0.08 + intensity*0.15})"
                    color = "var(--pos)"
                else:
                    bg = f"rgba(107,26,26,{0.08 + intensity*0.15})"
                    color = "var(--neg)"
                delta_html = f'<span style="background:{bg};color:{color};padding:1px 5px;border-radius:2px;font-size:10px;font-weight:500;">{sign}{f(delta,1)}%</span>'

        bar_w = max(2, int(w * 8))
        cum_bar_w = max(2, min(40, int(cum * 0.4)))

        rows += f"""<tr>
  <td>{name}</td>
  <td><span class="{tipo_cls}">{tipo_lbl}</span></td>
  <td style="font-family:'Source Sans 3';font-size:11px;">{pos.get('pais','—')}</td>
  <td>{pos.get('divisa','—')}</td>
  <td><div class="wbar"><div class="wfill" style="width:{bar_w}px;background:#0c2340;"></div>{f(w,1)}%</div></td>
  <td style="font-size:10px;color:var(--ink-4);"><div class="wbar"><div class="wfill" style="width:{cum_bar_w}px;background:var(--ink-3);"></div>{f(cum,0)}%</div></td>
  <td>{delta_html}</td>
</tr>"""

    # Avg RV exposure for historical comparison
    mix_all = data.get("cuantitativo", {}).get("mix_activos_historico", [])
    avg_liq = round(sum(m.get("liquidez_pct",0) or 0 for m in mix_all) / max(1,len(mix_all)), 1)

    return f"""
<section class="pane" id="p5">
  <div class="pane-header"><h1 class="pane-h1">Cartera actual</h1><span class="pane-dl">Posiciones a cierre · H2 2025</span></div>

  <div class="mb24">
    {render_narrative_inline(s.get('texto', '')) if s.get('texto') else f'<p class="pr">La cartera actual comprende <strong>{len(sorted_pos)} posiciones</strong>.</p>'}
  </div>

  <div class="kpi-row">
    <div class="kpi-cell"><div class="kpi-label">Posiciones totales</div><div class="kpi-value">{len(sorted_pos)}</div><div class="kpi-sub">vs media hist. {f(avg_npos,0)}</div></div>
    <div class="kpi-cell"><div class="kpi-label">Top 10 concentración</div><div class="kpi-value">{f(cur_top10,1)}%</div><div class="kpi-sub">vs media hist. {f(avg_top10,1)}%</div></div>
    <div class="kpi-cell"><div class="kpi-label">Liquidez</div><div class="kpi-value">{f(liq,1)}%</div><div class="kpi-sub">vs media hist. {f(avg_liq,1)}%</div></div>
    <div class="kpi-cell"><div class="kpi-label">Exposición neta RV</div><div class="kpi-value">~65%</div><div class="kpi-sub">Bruta {f(rv,0)}% (incl. derivados)</div></div>
  </div>

  <div class="col2 mb20">
    <div class="ch-b"><div class="ch-l">Nº posiciones por año</div><div class="ch-h"><canvas id="c-npos"></canvas></div></div>
    <div class="ch-b"><div class="ch-l">Concentración Top 5 / 10 / 15 (%)</div><div class="ch-h"><canvas id="c-conc"></canvas></div></div>
  </div>

  <div class="sr">Todas las posiciones ({len(sorted_pos)})</div>
  <div class="pt-wrap">
    <table class="pt">
      <thead><tr><th>Activo</th><th>Tipo</th><th>País</th><th>Divisa</th><th>Peso %</th><th>Peso acum.</th><th>Var.</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 7: FUENTES EXTERNAS
# ═══════════════════════════════════════════════════════════════

def build_tab_fuentes(data):
    s = data.get("analyst_synthesis", {}).get("fuentes_externas", {})
    ops = s.get("opiniones_clave", [])

    # Logo colors by source name
    logo_map = {
        "substack": ("#ff6719", "SF"),
        "salud financiera": ("#ff6719", "SF"),
        "rankia": ("#e85d26", "RK"),
        "finect": ("#1a8c5a", "FN"),
        "astralis": ("#6b3fa0", "AS"),
        "más dividendos": ("#5a6577", "MD"),
        "masdividendos": ("#5a6577", "MD"),
        "podcast": ("#8a3a8a", "🎙"),
        "video": ("#c04040", "▶"),
        "vídeo": ("#c04040", "▶"),
        "youtube": ("#c04040", "▶"),
        "avantage": ("#0c2340", "AC"),
    }

    def get_logo(fuente):
        fl = (fuente or "").lower()
        for key, (color, initials) in logo_map.items():
            if key in fl:
                return color, initials
        return "#555", fuente[:2].upper() if fuente else "??"

    # Separate: análisis (substack, rankia blog, finect review, astralis research)
    # vs otros (podcast, vídeo, foro, artículo genérico)
    pro = []
    otros = []
    other_keywords = ["podcast", "video", "vídeo", "foro", "youtube", "comunidad"]
    for op in ops:
        fuente_l = (op.get("fuente","") or "").lower()
        titulo_l = (op.get("titulo","") or "").lower()
        tipo_l = (op.get("tipo","") or "").lower()
        is_other = any(kw in fuente_l or kw in titulo_l or kw in tipo_l for kw in other_keywords)
        if is_other:
            otros.append(op)
        else:
            pro.append(op)

    def render_card(op, expanded=True):
        fuente = op.get('fuente', '')
        color, initials = get_logo(fuente)
        titulo = op.get('titulo', '') or fuente
        opinion = op.get('opinion', '')
        fecha = op.get('fecha', '')
        url = op.get('url', '')
        exp_state = ' open' if expanded else ''
        exp_arrow = '▼' if expanded else '▶'

        link_html = f'<a href="{url}" class="src-lnk" target="_blank">Ver análisis completo →</a>' if url and url != '#' else ''

        return f"""
    <div class="src-card">
      <div class="src-head">
        <div class="src-logo" style="background:{color};">{initials}</div>
        <div class="src-info">
          <div class="src-o">{fuente}</div>
        </div>
        <span class="src-date">{fecha}</span>
      </div>
      <div class="src-t">{titulo}</div>
      <button class="exp-btn" onclick="const b=this.nextElementSibling;const o=b.classList.toggle('open');this.textContent=(o?'▼':'▶')+' Ver puntos clave';">{exp_arrow} Ver puntos clave</button>
      <div class="exp-body{exp_state}">{opinion}</div>
      {link_html}
    </div>"""

    pro_html = "".join(render_card(op, expanded=True) for op in pro)
    otros_html = "".join(render_card(op, expanded=False) for op in otros)

    return f"""
<section class="pane" id="p6">
  <div class="pane-header"><h1 class="pane-h1">Fuentes externas</h1><span class="pane-dl">Análisis y recursos de terceros</span></div>

  <div class="sr" style="margin-top:0;color:var(--navy);border-bottom-color:var(--navy);">Análisis profesionales</div>
  {pro_html}

  {f'<div class="sr" style="color:var(--navy);border-bottom-color:var(--navy);">Otros recursos</div>{otros_html}' if otros else ''}
</section>"""


# ═══════════════════════════════════════════════════════════════
# TAB 8: DOCUMENTOS
# ═══════════════════════════════════════════════════════════════

def build_tab_documentos(data):
    s = data.get("analyst_synthesis", {}).get("documentos", {})
    pdfs = s.get("informes_pdf", [])
    cartas = sorted(s.get("cartas_urls", []), reverse=True)
    xmls = s.get("xmls_cnmv", [])
    ext = sorted(s.get("fuentes_externas_urls", []))
    total = s.get("total_fuentes", 0)

    def url_to_name(url_str):
        """Extract readable name from URL"""
        from urllib.parse import urlparse, unquote
        try:
            parsed = urlparse(url_str)
            domain = parsed.netloc.replace("www.", "").replace("foro.", "")
            path = unquote(parsed.path).strip("/")
            # Use path segments to build name
            parts = [p for p in path.split("/") if p and p not in ("p", "t", "podcast")]
            if parts:
                # Clean: replace hyphens/underscores with spaces, capitalize
                slug = parts[-1]
                # Skip numeric-only slugs, go to previous
                if slug.isdigit() and len(parts) > 1:
                    slug = parts[-2]
                    if slug.isdigit() and len(parts) > 2:
                        slug = parts[-3]
                name = slug.replace("-", " ").replace("_", " ").strip()
                if len(name) > 3:
                    return f"{domain} — {name[:65]}"
            return domain
        except Exception:
            return url_str[:60]

    def doc_rows(items, icon, max_n=12):
        html = ""
        for item in items[:max_n]:
            if isinstance(item, dict):
                name = item.get("archivo", str(item))
                url = "#"
            else:
                url = str(item) if str(item).startswith("http") else "#"
                name = url_to_name(str(item)) if url != "#" else str(item)[:60]
            html += f'<div class="doc-r"><span class="doc-ext">{icon}</span><span class="doc-nm">{name}</span><a href="{url}" target="_blank" class="doc-a">{"↗ Abrir" if url != "#" else ""}</a></div>'
        if len(items) > max_n:
            html += f'<div class="doc-r" style="color:var(--ink-4);font-size:11px;">+ {len(items)-max_n} archivos más</div>'
        return html

    return f"""
<section class="pane" id="p7">
  <div class="pane-header"><h1 class="pane-h1">Documentos</h1><span class="pane-dl">{total} fuentes consultadas</span></div>

  <div class="doc-grp">Informes semestrales CNMV ({len(pdfs)})</div>
  {doc_rows(pdfs, 'PDF')}

  <div class="doc-grp">Cartas del gestor ({len(cartas)})</div>
  {doc_rows(cartas, 'PDF', 10)}

  <div class="doc-grp">XMLs CNMV ({len(xmls)})</div>
  {doc_rows(xmls, 'XML', 6)}

  <div class="doc-grp">Fuentes externas ({len(ext)})</div>
  {doc_rows(ext, 'URL', 10)}
</section>"""


# ═══════════════════════════════════════════════════════════════
# SCRIPTS
# ═══════════════════════════════════════════════════════════════

def build_scripts(data):
    cuant = data.get("cuantitativo", {})
    pos_hist = data.get("posiciones", {}).get("historicas", [])

    # Extract series
    aum = cuant.get("serie_aum", [])
    part = cuant.get("serie_participes", [])
    vl = cuant.get("serie_vl_base100", [])
    com_a = cuant.get("serie_comisiones_por_clase", [])
    ter = cuant.get("serie_ter", [])

    years = [str(s.get("periodo",""))[-2:] for s in aum]
    aum_v = [s.get("valor_meur",0) for s in aum]
    part_v = [s.get("valor",0) for s in part]
    vl_v = [s.get("base100",0) for s in vl]

    # Performance fee flag
    ce = data.get("comision_exito", {})
    has_perf_fee = ce.get("existe", False) or False

    # TER data by class — align by year, exclude incoherent years (2014, 2015)
    ter_by_year = {str(s.get("periodo","")): s.get("ter_pct") for s in ter}
    skip_years = {"2014", "2015"}  # TER data incoherent with comisiones in these years
    com_filtered = [s for s in com_a if str(s.get("periodo","")) not in skip_years]
    com_years = [str(s.get("periodo","")) for s in com_filtered]
    ter_a = [ter_by_year.get(y, None) for y in com_years]
    com_a_data = [s.get("clases",{}).get("A",None) for s in com_filtered]
    com_b_data = [s.get("clases",{}).get("B",None) for s in com_filtered]

    # Position history for charts
    hist_sorted = sorted(pos_hist, key=lambda x: x.get("periodo",""))
    ch_yrs = []
    ch_npos = []
    ch_t5 = []
    ch_t10 = []
    ch_t15 = []
    for h in hist_sorted:
        todas = h.get("todas",[])
        if not todas: continue
        ch_yrs.append(str(h.get("periodo",""))[-2:])
        ch_npos.append(len(todas))
        w = sorted([x.get("peso_pct",0) or 0 for x in todas], reverse=True)
        ch_t5.append(round(sum(w[:5]),1))
        ch_t10.append(round(sum(w[:10]),1))
        ch_t15.append(round(sum(w[:15]),1))

    return f"""
<script>
function goTab(i,b){{document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));document.querySelectorAll('.tb').forEach(t=>t.classList.remove('on'));document.getElementById('p'+i).classList.add('on');b.classList.add('on');}}
function toggleTheme(){{const d=document.documentElement;const k=d.getAttribute('data-theme')==='dark';d.setAttribute('data-theme',k?'light':'dark');document.getElementById('thlbl').textContent=k?'Modo oscuro':'Modo claro';buildCharts();}}
const dk=()=>document.documentElement.getAttribute('data-theme')==='dark';
const TC=()=>dk()?'#908c84':'#444';
const GC=()=>dk()?'rgba(255,255,255,0.04)':'rgba(0,0,0,0.05)';
const A1=()=>dk()?'#4a7ea8':'#0c2340';
const A2=()=>dk()?'#4a8a6a':'#1a4d2e';
const A3=()=>dk()?'rgba(120,140,160,0.5)':'rgba(100,120,140,0.4)';
const AR=()=>dk()?'#c04040':'#6b1a1a';
const CI={{}};
// Formato español: . miles, , decimal
function fmtES(v,dec){{
  if(v==null)return'';
  if(dec===undefined)dec=v>=100||v===Math.round(v)?0:1;
  return v.toLocaleString('de-DE',{{minimumFractionDigits:dec,maximumFractionDigits:dec}});
}}
function mk(id,cfg){{if(CI[id])CI[id].destroy();const c=document.getElementById(id);if(!c)return;CI[id]=new Chart(c,cfg);}}
const sc=(mn,mx)=>({{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},y:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v);}}}},... (mn!=null?{{min:mn}}:{{}}),...(mx!=null?{{max:mx}}:{{}})}}}});
const opt=(leg)=>({{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:!!leg,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}}}});
const Y={json.dumps(years)};
// Anti-collision label helper for area/line charts with multiple series
function drawAreaLabels(chart,colors,dec,minGap){{
  const ctx=chart.ctx;
  ctx.font='500 7px Source Code Pro';
  ctx.textAlign='center';
  if(!minGap)minGap=12;
  if(!dec&&dec!==0)dec=1;
  const nds=chart.data.datasets.length;
  const npts=chart.data.datasets[0]?chart.data.datasets[0].data.length:0;
  // For each x-point, collect all labels and resolve collisions
  for(let i=0;i<npts;i++){{
    const labels=[];
    for(let di=0;di<nds;di++){{
      const ds=chart.data.datasets[di];
      const v=ds.data[i];
      const prev=i>0?ds.data[i-1]:null;
      if(v==null||v===0||v===prev)continue;
      const meta=chart.getDatasetMeta(di);
      const pt=meta.data[i];
      if(!pt)continue;
      labels.push({{di:di,v:v,x:pt.x,y:pt.y,color:colors[di]||TC()}});
    }}
    // Sort by y position (top to bottom)
    labels.sort((a,b)=>a.y-b.y);
    // Resolve collisions: push labels apart
    for(let j=1;j<labels.length;j++){{
      const gap=labels[j].y-labels[j-1].y;
      if(Math.abs(gap)<minGap){{
        labels[j-1].y-=(minGap-Math.abs(gap))/2;
        labels[j].y+=(minGap-Math.abs(gap))/2;
      }}
    }}
    // Draw
    labels.forEach(l=>{{
      ctx.fillStyle=l.color;
      const suffix=dec>=0?'%':'';
      const txt=dec>=0?fmtES(l.v,dec)+suffix:fmtES(l.v);
      ctx.fillText(txt,l.x,l.y-7);
    }});
  }}
}}
const valPlugin={{
  id:'valLabels',
  afterDatasetsDraw(chart){{
    const ctx=chart.ctx;
    ctx.font='500 8px Source Code Pro';
    ctx.textAlign='center';
    ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
    chart.data.datasets.forEach((ds,di)=>{{
      chart.getDatasetMeta(di).data.forEach((el,i)=>{{
        const v=ds.data[i];
        if(v!=null){{
          const y=el.y!=null?el.y:(el.y2||el.y);
          ctx.fillText(fmtES(v),el.x,y-6);
        }}
      }});
    }});
  }}
}};
function buildCharts(){{
  mk('c-aum',{{type:'bar',data:{{labels:Y,datasets:[{{data:{json.dumps(aum_v)},backgroundColor:A1()+'99'}}]}},options:{{...opt(),scales:sc(0)}},plugins:[valPlugin]}});
  mk('c-part',{{type:'bar',data:{{labels:Y,datasets:[{{data:{json.dumps(part_v)},backgroundColor:A2()+'99'}}]}},options:{{...opt(),scales:sc(0)}},plugins:[valPlugin]}});
  mk('c-vl',{{type:'line',data:{{labels:Y,datasets:[{{data:{json.dumps(vl_v)},borderColor:A1(),backgroundColor:A1()+'14',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A1()}}]}},options:{{...opt(),scales:sc(80)}},plugins:[valPlugin]}});
  buildComChart();
  mk('c-npos',{{type:'bar',data:{{labels:{json.dumps(ch_yrs)},datasets:[{{data:{json.dumps(ch_npos)},backgroundColor:A1()+'99'}}]}},options:{{...opt(),scales:sc(0)}},plugins:[valPlugin]}});
  const concLblPlugin={{
    id:'concLabels',
    afterDatasetsDraw(chart){{
      drawAreaLabels(chart,[dk()?'#7ba8d0':'#0c2340',dk()?'#7ba8d0':'#1a3a5c',dk()?'#7ba8d0':'#3d5a80'],1,14);
    }}
  }};
  mk('c-conc',{{type:'line',data:{{labels:{json.dumps(ch_yrs)},datasets:[
    {{label:'Top 5',data:{json.dumps(ch_t5)},borderColor:A1(),backgroundColor:A1()+'40',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A1()}},
    {{label:'Top 10',data:{json.dumps(ch_t10)},borderColor:A1()+'99',backgroundColor:A1()+'25',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A1()+'99'}},
    {{label:'Top 15',data:{json.dumps(ch_t15)},borderColor:A1()+'66',backgroundColor:A1()+'15',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A1()+'66'}}
  ]}},options:{{...opt(true),scales:sc(0,70)}},plugins:[concLblPlugin]}});
}}
const COM_A={json.dumps(com_a_data)};
const COM_B={json.dumps(com_b_data)};
const TER={json.dumps(ter_a)};
const CY={json.dumps([y[-2:] for y in com_years])};
// Comisión de éxito — dummy data for visual validation
const EXITO_DUMMY=[null,null,null,null,null,0.45,0.00,0.00,0.32,0.00];
const HAS_EXITO=true; // TEMP forced true (real: {json.dumps(has_perf_fee)})
function buildComChart(){{
  const sel=document.getElementById('com-sel');
  const cls=sel?sel.value:'A';
  const d=cls==='B'?COM_B:COM_A;
  const pctCb=function(v){{return v.toFixed(1)+'%';}};
  // Compute exito as TER - gestion - ~depositario (what's left)
  const exito=HAS_EXITO?EXITO_DUMMY:TER.map(()=>null);
  const datasets=[
    {{label:'TER total',data:TER,borderColor:A1(),backgroundColor:A1()+'40',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A1(),order:1}},
    {{label:'Com. gestión',data:d,borderColor:A2(),backgroundColor:A2()+'35',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:A2(),order:2}},
    {{label:'Com. éxito',data:exito,borderColor:dk()?'#c07040':'#8c3214',backgroundColor:dk()?'rgba(192,112,64,0.40)':'rgba(140,50,20,0.30)',borderWidth:1.5,fill:true,tension:0.3,pointRadius:2,pointBackgroundColor:dk()?'#c07040':'#8c3214',order:3}}
  ];
  const allVals=[...TER,...d,...exito].filter(v=>v!=null);
  const yMax=Math.max(1.2,Math.ceil((Math.max(...allVals)+0.2)*10)/10);
  const comLblPlugin={{
    id:'comAreaLabels',
    afterDatasetsDraw(chart){{
      drawAreaLabels(chart,[dk()?'#7ba8d0':'#0c2340',dk()?'#6aaa88':'#1a4d2e',dk()?'#e0a080':'#8c3214'],2,14);
    }}
  }};
  mk('c-com',{{type:'line',data:{{labels:CY,datasets:datasets}},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{display:true,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}},
      scales:{{
        x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
        y:{{grid:{{display:false}},min:0,max:yMax,ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}}}}
      }}
    }},
    plugins:[comLblPlugin]
  }});
}}
document.addEventListener('DOMContentLoaded',buildCharts);

// ═══════════════════════════════════════════════════════════
// MORNINGSTAR DATA: fetch + calculate + render
// ═══════════════════════════════════════════════════════════
const ISIN='{data.get("isin","ES0112231008")}';
let MST_DATA=null;

function dedupeSort(s){{s.sort((a,b)=>a.date-b.date);const o=[];for(const p of s){{if(!o.length||o[o.length-1].date.getTime()!==p.date.getTime())o.push(p);else o[o.length-1]=p;}}return o;}}
function monthEndF(s){{const m=new Map();for(const p of s){{const k=p.date.getUTCFullYear()+'-'+String(p.date.getUTCMonth()+1).padStart(2,'0');const c=m.get(k);if(!c||p.date>c.date)m.set(k,{{date:p.date,nav:p.nav}});}}return Array.from(m.values()).sort((a,b)=>a.date-b.date);}}
function yearEndF(s){{const m=new Map();for(const p of s){{const y=p.date.getUTCFullYear();const c=m.get(y);if(!c||p.date>c.date)m.set(y,{{year:y,date:p.date,nav:p.nav}});}}return Array.from(m.values()).sort((a,b)=>a.year-b.year);}}
function rets(levels){{const o=[];for(let i=1;i<levels.length;i++){{const a=levels[i-1].nav,b=levels[i].nav;if(a>0&&b>0)o.push({{date:levels[i].date,r:b/a-1}});}}return o;}}
function stdF(arr){{const x=arr.filter(v=>Number.isFinite(v));if(x.length<2)return NaN;const m=x.reduce((a,b)=>a+b,0)/x.length;return Math.sqrt(x.reduce((a,b)=>a+(b-m)*(b-m),0)/(x.length-1));}}

async function fetchMST(){{
  const url='https://tools.morningstar.es/api/rest.svc/timeseries_price/2nhcdckzon?id='+ISIN+'&idtype=Isin&frequency=daily&startDate=1900-01-01&outputType=compactJSON';
  try{{
    const res=await fetch(url,{{credentials:'omit'}});
    if(!res.ok)throw new Error('HTTP '+res.status);
    const arr=await res.json();
    const pts=[];
    for(const it of arr){{
      if(!Array.isArray(it)||it.length<2)continue;
      const ts=Number(it[0]),v=Number(it[1]);
      if(!Number.isFinite(ts)||!Number.isFinite(v)||v<=0)continue;
      const d0=new Date(ts);
      pts.push({{date:new Date(Date.UTC(d0.getUTCFullYear(),d0.getUTCMonth(),d0.getUTCDate())),nav:v}});
    }}
    return dedupeSort(pts);
  }}catch(e){{
    // Fallback proxy
    try{{
      const res2=await fetch('https://api.codetabs.com/v1/proxy?quest='+encodeURIComponent(url));
      const arr2=await res2.json();
      const pts2=[];
      for(const it of arr2){{if(Array.isArray(it)&&it.length>=2){{const ts=Number(it[0]),v=Number(it[1]);if(Number.isFinite(ts)&&Number.isFinite(v)&&v>0)pts2.push({{date:new Date(ts),nav:v}});}}}}
      return dedupeSort(pts2);
    }}catch(e2){{throw e2;}}
  }}
}}

function calcYearlyReturns(ye){{
  const xs=[],ys=[];
  for(let i=1;i<ye.length;i++){{
    const r=ye[i].nav/ye[i-1].nav-1;
    xs.push(ye[i].year);ys.push(r);
  }}
  return {{xs,ys}};
}}

function calcYearlyVol(me){{
  const mr=rets(me).map(x=>({{r:x.r,y:x.date.getUTCFullYear()}}));
  const byPos=new Map(),byNeg=new Map();
  const years=new Set();
  mr.forEach(p=>{{years.add(p.y);if(!byPos.has(p.y)){{byPos.set(p.y,[]);byNeg.set(p.y,[]);}}if(p.r>0)byPos.get(p.y).push(p.r);else if(p.r<0)byNeg.get(p.y).push(p.r);}});
  const xs=Array.from(years).sort(),ysP=[],ysN=[];
  // Detect incomplete last year
  const lastYr=xs[xs.length-1];
  const lastYrPts=mr.filter(p=>p.y===lastYr).length;
  xs.forEach(y=>{{
    const isInc=(y===lastYr&&lastYrPts<11);
    const pos=byPos.get(y)||[];const neg=byNeg.get(y)||[];
    ysP.push(isInc?NaN:(pos.length>=2?stdF(pos)*Math.sqrt(12):NaN));
    ysN.push(isInc?NaN:(neg.length>=2?stdF(neg)*Math.sqrt(12):NaN));
  }});
  return {{xs,ysP,ysN}};
}}

function calcDrawdown(series){{
  let peak=series[0].nav;
  const dates=[],dd=[];
  for(const p of series){{
    if(p.nav>peak)peak=p.nav;
    const d=(p.nav-peak)/peak;
    dates.push(p.date);dd.push(d);
  }}
  return {{dates,dd}};
}}

function calcRolling(me,months){{
  const dates=[],vals=[];
  for(let i=months;i<me.length;i++){{
    const r=me[i].nav/me[i-months].nav;
    const ann=Math.pow(r,12/months)-1;
    dates.push(me[i].date);vals.push(ann);
  }}
  return {{dates,vals}};
}}

function calcRollingVol(me,months){{
  const mr=rets(me);
  const dates=[],vals=[];
  for(let i=months-1;i<mr.length;i++){{
    const window=mr.slice(i-months+1,i+1).map(x=>x.r);
    vals.push(stdF(window)*Math.sqrt(12));
    dates.push(mr[i].date);
  }}
  return {{dates,vals}};
}}

// Store processed data globally for rolling updates
let MST_ME=null,MST_SERIES=null;
const pctAxis={{grid:{{display:false}},ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}}}};

function renderMST(series){{
  MST_SERIES=series;
  const me=monthEndF(series);MST_ME=me;
  const ye=yearEndF(series);
  const {{xs:retXsAll,ys:retYsAll}}=calcYearlyReturns(ye);
  const {{xs:volXsAll,ysP:ysPAll,ysN:ysNAll}}=calcYearlyVol(me);
  const incYear=new Date().getUTCFullYear();

  // Limit bar charts to last 10 years + ensure same years for both
  const maxBars=10;
  const allYears=retXsAll.slice();
  const startIdx=Math.max(0,allYears.length-maxBars);
  const retXs=retXsAll.slice(startIdx);
  const retYs=retYsAll.slice(startIdx);
  // Align vol to same years
  const volStart=volXsAll.indexOf(retXs[0]);
  const ysP=ysPAll.slice(volStart>=0?volStart:0);
  const ysN=ysNAll.slice(volStart>=0?volStart:0);
  const retLabels=retXs.map(String);
  const volLabels=retLabels; // SAME years

  // Tooltip interaction for line charts
  const lineOpt=(leg)=>({{responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:!!leg,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}},
      tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return ctx.dataset.label+': '+fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
    scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:12}}}},y:pctAxis}}
  }});

  // ── Shared chart builders ──
  function renderRetChart(id){{
    const colors=retYs.map(v=>v>=0?(dk()?'#4a8a6a':'#1a4d2e'):(dk()?'#c04040':'#6b1a1a'));
    mk(id,{{type:'bar',data:{{labels:retLabels,datasets:[{{data:retYs.map(v=>v*100),backgroundColor:colors}}]}},
      options:{{...opt(),scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
        y:{{grid:{{display:false}},ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}},
          // Extra range so labels don't touch edges
          suggestedMax:Math.ceil(Math.max(...retYs)*100)+5,
          suggestedMin:Math.floor(Math.min(...retYs)*100)-5
        }}
      }}}},
      plugins:[{{id:id+'L',afterDatasetsDraw(chart){{
        const ctx=chart.ctx;ctx.font='500 8px Source Code Pro';ctx.textAlign='center';
        ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
        chart.getDatasetMeta(0).data.forEach((el,i)=>{{
          const v=retYs[i];if(v==null)return;
          ctx.fillText((v>=0?'+':'')+fmtES(v*100,1)+'%',el.x,v>=0?el.y-8:el.y+14);
        }});
      }}}}]
    }});
  }}

  function renderVolChart(id){{
    mk(id,{{type:'bar',data:{{labels:volLabels,datasets:[
      {{label:'Vol. positiva',data:ysP.map(v=>Number.isFinite(v)?v*100:null),backgroundColor:dk()?'#4a8a6a':'#1a4d2e'}},
      {{label:'Vol. negativa',data:ysN.map(v=>Number.isFinite(v)?-v*100:null),backgroundColor:dk()?'#8c3a3a':'#8c3214'}}
    ]}},
      options:{{responsive:true,maintainAspectRatio:false,
        plugins:{{legend:{{display:true,position:'bottom',labels:{{color:TC(),font:{{size:9,family:'Source Code Pro'}},boxWidth:8,padding:8}}}}}},
        scales:{{x:{{grid:{{display:false}},stacked:true,ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}}}}}},
          y:{{grid:{{display:false}},stacked:true,ticks:{{color:dk()?'#908c84':'#555',font:{{family:'Source Code Pro',size:9}},callback:function(v){{return fmtES(v,1)+'%';}}}},
            suggestedMax:Math.ceil(Math.max(...ysP.filter(v=>Number.isFinite(v)))*100)+4,
            suggestedMin:-Math.ceil(Math.max(...ysN.filter(v=>Number.isFinite(v)))*100)-4
          }}
        }}
      }},
      plugins:[{{id:id+'L',afterDatasetsDraw(chart){{
        const ctx=chart.ctx;ctx.font='500 7px Source Code Pro';ctx.textAlign='center';
        ctx.fillStyle=dk()?'#c8c4bc':'#1a1a1a';
        chart.data.datasets.forEach((ds,di)=>{{
          chart.getDatasetMeta(di).data.forEach((el,i)=>{{
            const v=ds.data[i];if(v==null||!Number.isFinite(v))return;
            const prev=i>0?ds.data[i-1]:null;if(v===prev)return;
            ctx.fillText(fmtES(Math.abs(v),1)+'%',el.x,di===0?el.y-8:el.y+14);
          }});
        }});
      }}}}]
    }});
  }}

  // ── Resumen tab ──
  renderRetChart('mst-ret');
  renderVolChart('mst-vol');
  if(retXs[retXs.length-1]===incYear){{
    const el=document.getElementById('mst-ret-note');
    if(el)el.textContent='* Último año ('+incYear+') incompleto.';
  }}

  // ── Evolución tab ──
  const evoEl=document.getElementById('mst-evo-content');
  const loadEl=document.getElementById('mst-loading');
  if(evoEl)evoEl.style.display='block';
  if(loadEl)loadEl.style.display='none';

  // KPIs
  const totalR=series[series.length-1].nav/series[0].nav;
  const nYears=(series[series.length-1].date-series[0].date)/31557600000;
  const cagr=Math.pow(totalR,1/nYears)-1;
  const allMR=rets(me).map(x=>x.r);
  const volAll=stdF(allMR)*Math.sqrt(12);
  const ddCalc=calcDrawdown(series);
  const maxDD=Math.min(...ddCalc.dd);
  const roll3=calcRolling(me,36);
  const avgRoll3=roll3.vals.length?roll3.vals.reduce((a,b)=>a+b,0)/roll3.vals.length:0;
  const rollVol12=calcRollingVol(me,12);
  const avgRollVol=rollVol12.vals.length?rollVol12.vals.reduce((a,b)=>a+b,0)/rollVol12.vals.length:0;

  const kpiEl=document.getElementById('mst-evo-kpis');
  if(kpiEl)kpiEl.innerHTML=`
    <div class="kpi-cell"><div class="kpi-label">CAGR histórico</div><div class="kpi-value pos">`+fmtES(cagr*100,1)+`%</div><div class="kpi-sub">`+fmtES(nYears,1)+` años</div></div>
    <div class="kpi-cell"><div class="kpi-label">Volatilidad media</div><div class="kpi-value">`+fmtES(volAll*100,1)+`%</div><div class="kpi-sub">Anualizada (mensual √12)</div></div>
    <div class="kpi-cell"><div class="kpi-label">Máx. drawdown</div><div class="kpi-value neg">`+fmtES(maxDD*100,1)+`%</div></div>
    <div class="kpi-cell"><div class="kpi-label">Rent. rolling 3A media</div><div class="kpi-value">`+fmtES(avgRoll3*100,1)+`%</div><div class="kpi-sub">Vol. rolling 12M media: `+fmtES(avgRollVol*100,1)+`%</div></div>
  `;

  renderRetChart('mst-evo-ret');
  renderVolChart('mst-evo-vol');

  // Growth base 100 with tooltip
  const step=Math.max(1,Math.floor(series.length/100));
  const gPts=series.filter((_,i)=>i%step===0||i===series.length-1);
  const gVals=gPts.map(p=>p.nav/series[0].nav*100);
  // Find max point for annotation
  const gMax=Math.max(...gVals);const gMaxIdx=gVals.indexOf(gMax);
  const gPointRadii=gVals.map((_,i)=>i===gMaxIdx||i===gVals.length-1?4:0);
  mk('mst-growth',{{type:'line',data:{{labels:gPts.map(p=>p.date.toISOString().slice(0,10)),datasets:[{{data:gVals,borderColor:A1(),backgroundColor:A1()+'18',borderWidth:1.5,fill:true,tension:0.2,pointRadius:gPointRadii,pointBackgroundColor:A1()}}]}},
    options:lineOpt(),
    plugins:[{{id:'gLbl',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#c8c4bc':'#0c2340';
      // Label last point and max
      const meta=chart.getDatasetMeta(0);
      [gMaxIdx,gVals.length-1].forEach(idx=>{{
        if(idx>=0&&idx<meta.data.length){{
          const pt=meta.data[idx];
          ctx.fillText(fmtES(gVals[idx],0),pt.x,pt.y-10);
        }}
      }});
    }}}}]
  }});

  // Drawdown daily with max DD annotation
  const ddStep=Math.max(1,Math.floor(ddCalc.dates.length/100));
  const ddPts=ddCalc.dates.filter((_,i)=>i%ddStep===0||i===ddCalc.dates.length-1);
  const ddV=ddCalc.dd.filter((_,i)=>i%ddStep===0||i===ddCalc.dd.length-1);
  const ddVpct=ddV.map(v=>v*100);
  const ddMin=Math.min(...ddVpct);const ddMinIdx=ddVpct.indexOf(ddMin);
  // Find 2nd worst drawdown (different trough)
  let dd2Idx=-1,dd2Val=0;
  for(let i=0;i<ddVpct.length;i++){{
    if(Math.abs(i-ddMinIdx)>5&&ddVpct[i]<dd2Val){{dd2Val=ddVpct[i];dd2Idx=i;}}
  }}
  const ddRadii=ddVpct.map((_,i)=>(i===ddMinIdx||(dd2Idx>=0&&i===dd2Idx))?4:0);
  mk('mst-dd',{{type:'line',data:{{labels:ddPts.map(d=>d.toISOString().slice(0,10)),datasets:[{{data:ddVpct,borderColor:AR(),backgroundColor:AR()+'20',borderWidth:1.5,fill:true,tension:0.2,pointRadius:ddRadii,pointBackgroundColor:AR()}}]}},
    options:lineOpt(),
    plugins:[{{id:'ddLbl',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#e08080':'#6b1a1a';
      const meta=chart.getDatasetMeta(0);
      [ddMinIdx,dd2Idx].forEach(idx=>{{
        if(idx>=0&&idx<meta.data.length){{
          const pt=meta.data[idx];
          ctx.fillText(fmtES(ddVpct[idx],1)+'%',pt.x,pt.y+14);
        }}
      }});
    }}}}]
  }});

  // Rolling (initial)
  updateRollingRet();
  updateRollingVol();
}}

function updateRollingRet(){{
  if(!MST_ME)return;
  const months=parseInt(document.getElementById('mst-roll-ret-sel').value)||36;
  const r=calcRolling(MST_ME,months);
  const step=Math.max(1,Math.floor(r.dates.length/80));
  const pts=r.dates.filter((_,i)=>i%step===0||i===r.dates.length-1);
  const vals=r.vals.filter((_,i)=>i%step===0||i===r.vals.length-1);
  const vpct=vals.map(v=>v*100);
  const maxV=Math.max(...vpct);const maxI=vpct.indexOf(maxV);
  const radii=vpct.map((_,i)=>i===maxI||i===vpct.length-1?4:0);
  mk('mst-roll-ret',{{type:'line',data:{{labels:pts.map(d=>d.toISOString().slice(0,7)),datasets:[{{data:vpct,borderColor:A2(),backgroundColor:A2()+'18',borderWidth:1.5,fill:true,tension:0.3,pointRadius:radii,pointBackgroundColor:A2()}}]}},
    options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
      plugins:{{legend:{{display:false}},tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
      scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:10}}}},y:pctAxis}}
    }},
    plugins:[{{id:'rrL',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#6aaa88':'#1a4d2e';
      const meta=chart.getDatasetMeta(0);
      [maxI,vpct.length-1].forEach(idx=>{{if(idx>=0&&idx<meta.data.length)ctx.fillText(fmtES(vpct[idx],1)+'%',meta.data[idx].x,meta.data[idx].y-10);}});
    }}}}]
  }});
}}
function updateRollingVol(){{
  if(!MST_ME)return;
  const months=parseInt(document.getElementById('mst-roll-vol-sel').value)||12;
  const r=calcRollingVol(MST_ME,months);
  const step=Math.max(1,Math.floor(r.dates.length/80));
  const pts=r.dates.filter((_,i)=>i%step===0||i===r.dates.length-1);
  const vals=r.vals.filter((_,i)=>i%step===0||i===r.vals.length-1);
  const vpct=vals.map(v=>v*100);
  const maxV=Math.max(...vpct);const maxI=vpct.indexOf(maxV);
  const radii=vpct.map((_,i)=>i===maxI||i===vpct.length-1?4:0);
  mk('mst-roll-vol',{{type:'line',data:{{labels:pts.map(d=>d.toISOString().slice(0,7)),datasets:[{{data:vpct,borderColor:A3(),borderWidth:1.5,fill:false,tension:0.3,pointRadius:radii,pointBackgroundColor:A3()}}]}},
    options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
      plugins:{{legend:{{display:false}},tooltip:{{enabled:true,callbacks:{{label:function(ctx){{return fmtES(ctx.parsed.y,1)+'%';}}}}}}}},
      scales:{{x:{{grid:{{display:false}},ticks:{{color:TC(),font:{{family:'Source Code Pro',size:9}},maxTicksLimit:10}}}},y:pctAxis}}
    }},
    plugins:[{{id:'rvL',afterDatasetsDraw(chart){{
      const ctx=chart.ctx;ctx.font='600 8px Source Code Pro';ctx.textAlign='center';ctx.fillStyle=dk()?'#8095ad':'#3d5a80';
      const meta=chart.getDatasetMeta(0);
      [maxI,vpct.length-1].forEach(idx=>{{if(idx>=0&&idx<meta.data.length)ctx.fillText(fmtES(vpct[idx],1)+'%',meta.data[idx].x,meta.data[idx].y-10);}});
    }}}}]
  }});
}}

// Auto-fetch on page load
document.addEventListener('DOMContentLoaded',async()=>{{
  try{{
    MST_DATA=await fetchMST();
    if(MST_DATA&&MST_DATA.length>30)renderMST(MST_DATA);
    else{{const el=document.getElementById('mst-loading');if(el)el.textContent='Datos insuficientes de Morningstar ('+((MST_DATA||[]).length)+' puntos)';}}
  }}catch(e){{
    const el=document.getElementById('mst-loading');
    if(el)el.textContent='Error cargando datos de Morningstar: '+e.message;
    console.error('MST error:',e);
  }}
}});
</script>"""


# ═══════════════════════════════════════════════════════════════
# GENERATE
# ═══════════════════════════════════════════════════════════════

def generate():
    data = load_data()
    html = f"""<!DOCTYPE html>
<html lang="es" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{data.get('nombre', 'Fondo')} — Informe Analítico</title>
<link href="https://fonts.googleapis.com/css2?family=EB+Garamond:ital,wght@0,400;0,500;0,600;1,400&family=Source+Sans+3:wght@300;400;500;600&family=Source+Code+Pro:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
{CSS}
</head>
<body>
{build_header(data)}
<main class="body">
{build_tab_resumen(data)}
{build_tab_historia(data)}
{build_tab_gestores(data)}
{build_tab_evolucion(data)}
{build_tab_estrategia(data)}
{build_tab_cartera(data)}
{build_tab_fuentes(data)}
{build_tab_documentos(data)}
</main>
{build_scripts(data)}
</body>
</html>"""
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Generated: {OUTPUT} ({len(html):,} chars)")

    # Quality report — detect missing data
    synth = data.get("analyst_synthesis", {})
    issues = []
    for section in ["resumen", "historia", "gestores", "evolucion", "estrategia", "cartera", "fuentes_externas"]:
        sec = synth.get(section, {})
        if not sec:
            issues.append(f"CRITICO: Sin {section}. Ejecutar analyst_agent.")
        elif not sec.get("texto"):
            issues.append(f"MEJORA: {section} sin texto narrativo.")
    if not data.get("posiciones", {}).get("actuales"):
        issues.append("CRITICO: Sin posiciones actuales. Verificar cnmv_agent.")
    if len(data.get("cuantitativo", {}).get("serie_aum", [])) < 3:
        issues.append("MEJORA: Serie AUM corta (<3 puntos).")
    if not data.get("gestores", {}).get("equipo"):
        issues.append("MEJORA: Sin equipo gestor identificado. Ejecutar manager_deep_agent.")

    if issues:
        print(f"\n⚠ Informe de calidad ({len(issues)} items):")
        for issue in issues:
            print(f"  · {issue}")
    else:
        print("OK: Todos los datos completos.")


if __name__ == "__main__":
    generate()
