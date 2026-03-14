"""Natural Language Analytics — QueryMind UI."""
from __future__ import annotations
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="QueryMind",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS via st.html() — bypasses Streamlit's markdown parser entirely.
# st.markdown() chokes on attribute selectors like [class*="css"] inside <style>
# and dumps the rest of the block as plain text.
# ─────────────────────────────────────────────────────────────────────────────
st.html("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>

/* ── Global font ── */
html, body, [class*="css"], * {
    font-family: 'Inter', -apple-system, sans-serif !important;
}
code, pre, .stCode *, [data-testid="stCode"] * {
    font-family: 'IBM Plex Mono', monospace !important;
}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, [data-testid="stHeader"],
[data-testid="stDecoration"], [data-testid="stToolbar"] {
    display: none !important;
}

/* ── Page background ── */
[data-testid="stAppViewContainer"] > .main { background: #F8FAFC !important; }
[data-testid="block-container"] {
    padding-top: 2rem !important;
    padding-bottom: 3rem !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #0F172A !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
}
section[data-testid="stSidebar"] > div { padding-top: 1.25rem !important; }

section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span:not([data-testid="stMarkdownContainer"] a span),
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] small { color: #94A3B8 !important; }

section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] strong { color: #F1F5F9 !important; }

section[data-testid="stSidebar"] input {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid rgba(255,255,255,0.12) !important;
    border-radius: 7px !important;
    color: #F1F5F9 !important;
}
section[data-testid="stSidebar"] input:focus {
    border-color: #2563EB !important;
    box-shadow: 0 0 0 3px rgba(37,99,235,0.3) !important;
    outline: none !important;
}
section[data-testid="stSidebar"] button {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid rgba(255,255,255,0.12) !important;
    border-radius: 7px !important;
    color: #CBD5E1 !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    transition: all 150ms ease !important;
}
section[data-testid="stSidebar"] button:hover {
    background: rgba(255,255,255,0.13) !important;
    color: #F1F5F9 !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.09) !important;
    border-radius: 7px !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] summary,
section[data-testid="stSidebar"] [data-testid="stExpander"] p { color: #94A3B8 !important; }
section[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.08) !important; }
section[data-testid="stSidebar"] [data-baseweb="radio"] label span { color: #94A3B8 !important; }

/* ── Text area (question input) ── */
.stTextArea textarea {
    font-size: 15px !important;
    line-height: 1.65 !important;
    border: 1.5px solid rgba(0,0,0,0.13) !important;
    border-radius: 10px !important;
    padding: 14px 18px !important;
    background: #FFFFFF !important;
    color: #0F172A !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.07) !important;
    resize: none !important;
    transition: border-color 150ms ease, box-shadow 150ms ease !important;
}
.stTextArea textarea:focus {
    border-color: #2563EB !important;
    box-shadow: 0 0 0 3px rgba(37,99,235,0.12), 0 1px 3px rgba(0,0,0,0.07) !important;
    outline: none !important;
}
.stTextArea textarea::placeholder { color: #94A3B8 !important; }
.stTextArea label { display: none !important; }

/* ── Primary button ── */
button[kind="primary"] {
    background: #2563EB !important;
    border: none !important;
    border-radius: 8px !important;
    color: #FFFFFF !important;
    font-size: 14px !important;
    font-weight: 600 !important;
    padding: 10px 24px !important;
    box-shadow: 0 1px 3px rgba(37,99,235,0.35) !important;
    transition: background 150ms ease, box-shadow 150ms ease, transform 100ms ease !important;
}
button[kind="primary"]:hover {
    background: #1D4ED8 !important;
    box-shadow: 0 4px 14px rgba(37,99,235,0.45) !important;
    transform: translateY(-1px) !important;
}
button[kind="primary"]:active { transform: translateY(0) !important; }

/* ── Secondary / chip buttons ── */
button[kind="secondary"] {
    background: #FFFFFF !important;
    border: 1px solid rgba(0,0,0,0.1) !important;
    border-radius: 20px !important;
    color: #64748B !important;
    font-size: 12.5px !important;
    font-weight: 500 !important;
    padding: 5px 14px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
    transition: all 150ms ease !important;
    white-space: nowrap !important;
}
button[kind="secondary"]:hover {
    border-color: #2563EB !important;
    color: #2563EB !important;
    background: rgba(37,99,235,0.06) !important;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: #FFFFFF !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 18px 22px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.04) !important;
}
[data-testid="stMetricLabel"] > div {
    color: #64748B !important;
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: 0.07em !important;
    text-transform: uppercase !important;
}
[data-testid="stMetricValue"] > div {
    color: #0F172A !important;
    font-size: 26px !important;
    font-weight: 600 !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid rgba(0,0,0,0.07) !important;
    border-radius: 10px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.07) !important;
    overflow: hidden !important;
}
[data-testid="stExpander"] summary {
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #0F172A !important;
    padding: 13px 16px !important;
}
[data-testid="stExpander"] summary:hover { background: rgba(0,0,0,0.015) !important; }
[data-testid="stExpander"] > div > div { padding: 2px 16px 14px !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius: 10px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08) !important;
    overflow: hidden !important;
    border: none !important;
}

/* ── Divider ── */
hr {
    border: none !important;
    border-top: 1px solid rgba(0,0,0,0.07) !important;
    margin: 1.25rem 0 !important;
}

/* ── Spinner color ── */
div[data-testid="stSpinner"] > div { border-top-color: #2563EB !important; }

</style>
""")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — all injected HTML uses hardcoded colors, no CSS vars
# ─────────────────────────────────────────────────────────────────────────────

def _badge(text: str, color: str, bg: str) -> str:
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:20px;'
        f'font-size:12px;font-weight:600;color:{color};background:{bg};'
        f'margin:2px 3px 2px 0;font-family:Inter,sans-serif">{text}</span>'
    )

def _rel_row(label: str, label_color: str, label_bg: str, code: str) -> str:
    return (
        f'<div style="display:flex;align-items:center;gap:8px;padding:7px 0;'
        f'border-bottom:1px solid rgba(0,0,0,0.06);flex-wrap:wrap">'
        f'{_badge(label, label_color, label_bg)}'
        f'<code style="font-family:\'IBM Plex Mono\',monospace;font-size:12px;'
        f'background:#F1F5F9;padding:2px 8px;border-radius:5px;color:#0F172A">{code}</code>'
        f'</div>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state.history = []
if "result" not in st.session_state:
    st.session_state.result = None


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<p style="font-size:17px;font-weight:700;color:#F1F5F9;'
        'letter-spacing:-0.01em;margin-bottom:1.5rem">⬡ QueryMind</p>',
        unsafe_allow_html=True,
    )

    st.markdown('<p style="font-size:10px;font-weight:700;letter-spacing:0.1em;'
                'text-transform:uppercase;color:#475569;margin:0 0 6px">Connection</p>',
                unsafe_allow_html=True)
    api_url = st.text_input("api_url", value=API_BASE, label_visibility="collapsed",
                             placeholder="API base URL")

    st.markdown('<p style="font-size:10px;font-weight:700;letter-spacing:0.1em;'
                'text-transform:uppercase;color:#475569;margin:12px 0 6px">Provider</p>',
                unsafe_allow_html=True)
    provider = st.radio("provider", ["openai", "anthropic"],
                        horizontal=True, label_visibility="collapsed")

    st.markdown('<p style="font-size:10px;font-weight:700;letter-spacing:0.1em;'
                'text-transform:uppercase;color:#475569;margin:12px 0 6px">Database</p>',
                unsafe_allow_html=True)
    db_override = st.text_input("db_override", label_visibility="collapsed",
                                 placeholder="Override DATABASE_URL (optional)")

    st.markdown("---")

    st.markdown('<p style="font-size:10px;font-weight:700;letter-spacing:0.1em;'
                'text-transform:uppercase;color:#475569;margin:0 0 6px">Schema</p>',
                unsafe_allow_html=True)
    if st.button("Inspect schema", use_container_width=True):
        try:
            r = requests.get(f"{api_url}/schema", timeout=10)
            r.raise_for_status()
            for tname, meta in r.json().get("tables", {}).items():
                with st.expander(tname):
                    for col in meta["columns"]:
                        pk = " · PK" if col["name"] in meta["primary_keys"] else ""
                        st.caption(f"`{col['name']}` — {col['type']}{pk}")
        except Exception as e:
            st.error(str(e))

    if st.session_state.history:
        st.markdown("---")
        st.markdown('<p style="font-size:10px;font-weight:700;letter-spacing:0.1em;'
                    'text-transform:uppercase;color:#475569;margin:0 0 6px">Recent</p>',
                    unsafe_allow_html=True)
        for item in reversed(st.session_state.history[-6:]):
            q = item["question"]
            short = q[:50] + "…" if len(q) > 50 else q
            st.markdown(
                f'<p style="font-size:12.5px;color:#64748B;padding:4px 0;'
                f'border-left:2px solid #1E293B;padding-left:8px;'
                f'margin:3px 0;line-height:1.4">{short}</p>',
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Main content
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    '<h1 style="font-size:26px;font-weight:700;color:#0F172A;'
    'letter-spacing:-0.03em;margin-bottom:4px">QueryMind</h1>',
    unsafe_allow_html=True,
)
st.markdown(
    '<p style="font-size:14px;color:#64748B;margin-bottom:1.5rem;line-height:1.6">'
    'Ask any question about your data in plain English. '
    'QueryMind resolves the schema, writes the SQL, validates it, and explains the result.'
    '</p>',
    unsafe_allow_html=True,
)

# Sample question chips
SAMPLES = [
    "Top 10 customers by total spend",
    "Monthly revenue for 2023",
    "Revenue by product category",
    "Best-selling products by units",
    "Orders from New York users",
    "Average order value, completed only",
]
chip_cols = st.columns(len(SAMPLES))
clicked = None
for i, (col, sample) in enumerate(zip(chip_cols, SAMPLES)):
    if col.button(sample, key=f"chip_{i}"):
        clicked = sample

st.markdown('<div style="margin-top:10px"></div>', unsafe_allow_html=True)

# Question input
question = st.text_area(
    "q",
    value=clicked or "",
    placeholder="e.g. Which product categories generate the most revenue from returning customers?",
    height=90,
    label_visibility="collapsed",
)

run_col, _ = st.columns([1, 7])
run = run_col.button("Run query →", type="primary", use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Query execution
# ─────────────────────────────────────────────────────────────────────────────
if run and question.strip():
    payload: dict = {"question": question.strip(), "provider": provider}
    if db_override.strip():
        payload["database_url"] = db_override.strip()

    with st.spinner("Running pipeline…"):
        try:
            resp = requests.post(f"{api_url}/ask", json=payload, timeout=90)
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as exc:
            try:
                detail = exc.response.json().get("detail", exc.response.text)
            except Exception:
                detail = exc.response.text if exc.response else str(exc)
            st.error(f"**API {exc.response.status_code}** — {detail}")
            st.stop()
        except Exception as exc:
            st.error(f"Could not reach the API: {exc}")
            st.stop()

    st.session_state.result = data
    st.session_state.history.append({"question": question.strip()})
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────
data = st.session_state.result
if not data:
    st.stop()

st.markdown("<hr>", unsafe_allow_html=True)

# Answer card
summary = data.get("summary", "—")
st.markdown(
    f'<div style="background:#FFFFFF;border-radius:10px;padding:18px 22px;'
    f'box-shadow:0 1px 3px rgba(0,0,0,0.08);border-left:3px solid #2563EB;'
    f'font-size:15px;line-height:1.7;color:#0F172A;margin-bottom:1.25rem;'
    f'font-family:Inter,sans-serif">{summary}</div>',
    unsafe_allow_html=True,
)

# Metrics
trace = data.get("trace", {})
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows returned",   f"{data.get('row_count', 0):,}")
c2.metric("Query time",      f"{data.get('execution_time_ms', 0):.1f} ms")
c3.metric("Tables resolved", len(trace.get("tables_selected", [])))
c4.metric("Joins detected",  len(trace.get("relationships_used", [])))

st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

# Chart + data table
rows = data.get("rows", [])
chart_cfg = data.get("chart")

if rows:
    df = pd.DataFrame(rows)

    if chart_cfg:
        left, right = st.columns([1.15, 1])
    else:
        left, right = st.columns([1, 0.001])

    with left:
        if chart_cfg:
            ct    = chart_cfg.get("type", "bar")
            x_col = chart_cfg.get("x")
            y_col = chart_cfg.get("y")
            title = chart_cfg.get("title", "")
            palette = ["#2563EB","#7C3AED","#059669","#D97706","#DC2626","#0891B2"]
            try:
                if ct == "bar":
                    fig = px.bar(df, x=x_col, y=y_col, title=title,
                                 color_discrete_sequence=palette)
                elif ct == "line":
                    fig = px.line(df, x=x_col, y=y_col, title=title,
                                  markers=True, color_discrete_sequence=palette)
                elif ct == "scatter":
                    fig = px.scatter(df, x=x_col, y=y_col, title=title,
                                     color_discrete_sequence=palette)
                elif ct == "pie":
                    fig = px.pie(df, names=x_col, values=y_col, title=title,
                                 color_discrete_sequence=palette)
                else:
                    fig = px.bar(df, x=x_col, y=y_col, title=title,
                                 color_discrete_sequence=palette)

                fig.update_layout(
                    paper_bgcolor="white", plot_bgcolor="white",
                    font_family="Inter, sans-serif", font_color="#0F172A",
                    title_font_size=13, title_font_color="#64748B",
                    margin=dict(t=32, b=12, l=0, r=0), height=320,
                    xaxis=dict(showgrid=False, zeroline=False,
                               tickfont=dict(size=11, color="#64748B")),
                    yaxis=dict(gridcolor="#F1F5F9", zeroline=False,
                               tickfont=dict(size=11, color="#64748B")),
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as exc:
                st.warning(f"Chart error: {exc}")

    with right:
        st.markdown(
            '<p style="font-size:11px;font-weight:700;letter-spacing:0.07em;'
            'text-transform:uppercase;color:#94A3B8;margin-bottom:6px">Result data</p>',
            unsafe_allow_html=True,
        )
        st.dataframe(df, use_container_width=True, height=310)
else:
    st.info("No rows returned for this query.")

st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

# SQL + Pipeline trace
sql_col, trace_col = st.columns(2)

with sql_col:
    with st.expander("Generated SQL", expanded=True):
        st.code(data.get("sql", ""), language="sql")

with trace_col:
    with st.expander("Pipeline reasoning", expanded=True):
        tables = trace.get("tables_selected", [])
        rels   = trace.get("relationships_used", [])
        issues = trace.get("schema_issues", [])

        # Tables resolved
        st.markdown(
            '<p style="font-size:11px;font-weight:700;letter-spacing:0.07em;'
            'text-transform:uppercase;color:#94A3B8;margin:4px 0 6px">Tables resolved</p>',
            unsafe_allow_html=True,
        )
        if tables:
            st.markdown(
                "".join(_badge(t, "#1D4ED8", "rgba(37,99,235,0.10)") for t in tables),
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<span style="color:#94A3B8;font-size:13px">—</span>',
                        unsafe_allow_html=True)

        # Relationships
        st.markdown(
            '<p style="font-size:11px;font-weight:700;letter-spacing:0.07em;'
            'text-transform:uppercase;color:#94A3B8;margin:12px 0 4px">Relationships used</p>',
            unsafe_allow_html=True,
        )
        if rels:
            rows_html = ""
            for r in rels:
                is_fk = "FK" in r
                label = "declared FK" if is_fk else "inferred"
                lc, lb = ("#065F46", "rgba(5,150,105,0.10)") if is_fk else ("#92400E", "rgba(217,119,6,0.10)")
                clean = r.replace(" FK→ ", " → ").replace(" ~→ ", " → ")
                rows_html += _rel_row(label, lc, lb, clean)
            st.markdown(rows_html, unsafe_allow_html=True)
        else:
            st.markdown('<span style="color:#94A3B8;font-size:13px">No joins needed</span>',
                        unsafe_allow_html=True)

        # Schema issues
        if issues:
            st.markdown(
                '<p style="font-size:11px;font-weight:700;letter-spacing:0.07em;'
                'text-transform:uppercase;color:#94A3B8;margin:12px 0 4px">Schema warnings</p>',
                unsafe_allow_html=True,
            )
            for issue in issues:
                st.markdown(
                    _rel_row("warn", "#92400E", "rgba(217,119,6,0.10)", issue),
                    unsafe_allow_html=True,
                )
