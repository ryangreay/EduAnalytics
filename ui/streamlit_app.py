import os
import sys
from pathlib import Path
import streamlit as st
import json
import re
import plotly.graph_objects as go
from sqlalchemy import text
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.agent import create_sql_agent

load_dotenv(PROJECT_ROOT / ".env")

st.set_page_config(page_title="CAASPP SQL Chat", layout="wide")
st.title("CAASPP ELA/Math AI Assistant")

st.markdown("""
Ask questions about California education data and I'll help you find answers using SQL queries and entity lookups. Specificity helps! Example: "Top 10 counties by Asian proficiency for ELA for all grades in 2025 and how many students tested" is better than "Top 10 counties by Asian proficiency".
""")

PG_URL = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}?sslmode={os.getenv('POSTGRES_SSLMODE')}"
)

# Initialize agent
@st.cache_resource
def get_agent():
    agent, sql_toolkit, system_instructions = create_sql_agent(
        pg_url=PG_URL,
        whitelist_path=str(PROJECT_ROOT / "app" / "schema_whitelist.json")
    )
    return agent, sql_toolkit, system_instructions

try:
    agent, sql_toolkit, system_instructions = get_agent()
except Exception as e:
    st.error(f"Failed to initialize agent: {e}")
    st.stop()

# Diagnostics - cached to avoid repeated DB hits
@st.cache_data(show_spinner=False)
def diagnose(_sql_toolkit):
    info = {}
    # Connection check
    try:
        with _sql_toolkit.engine.begin() as con:
            con.execute(text("SELECT 1"))
        info["can_connect"] = True
    except Exception as e:
        info["can_connect"] = False
        info["connect_error"] = str(e)

    # Tables/schema info
    try:
        info["table_info"] = _sql_toolkit.get_table_info()
    except Exception as e:
        info["table_info"] = f"Error fetching table info: {e}"

    # Row count in fact table
    try:
        with _sql_toolkit.engine.begin() as con:
            res = con.execute(text("SELECT COUNT(*) FROM analytics.fact_scores"))
            info["fact_scores_count"] = res.scalar()
    except Exception as e:
        info["fact_scores_error"] = str(e)

    # Latest year
    try:
        with _sql_toolkit.engine.begin() as con:
            res = con.execute(text("SELECT MAX(year_key) FROM analytics.dim_year"))
            info["max_year"] = res.scalar()
    except Exception as e:
        info["year_error"] = str(e)

    return info

diagnostics = diagnose(sql_toolkit)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Debug toggle (stored in session state)
if "chart_debug" not in st.session_state:
    st.session_state.chart_debug = False

# Display chat history
for message in st.session_state.messages:
    if message["role"] == "user":
        with st.chat_message("user"):
            st.markdown(message["content"])
    elif message["role"] == "assistant":
        with st.chat_message("assistant"):
            st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about Math/ELA (e.g., 'Show top districts by Math proficiency for Hispanic students in grade 5')"):
    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Show thinking indicator
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # Reset error count for new question
                sql_toolkit.reset_error_count()
                
                # Convert history to LangChain messages
                messages = []
                for msg in st.session_state.messages[:-1]:  # Exclude current message
                    if msg["role"] == "user":
                        messages.append(HumanMessage(content=msg["content"]))
                    elif msg["role"] == "assistant":
                        messages.append(AIMessage(content=msg["content"]))
                
                # Add current question
                messages.append(HumanMessage(content=prompt))
                
                # Stream the agent's response
                response_placeholder = st.empty()
                
                # Run agent with streaming and full history
                full_response = ""
                for step in agent.stream(
                    {"messages": messages},
                    stream_mode="values"
                ):
                    last_message = step["messages"][-1]
                    
                    # If it's an AI message with content, show it
                    if isinstance(last_message, AIMessage) and hasattr(last_message, 'content') and last_message.content:
                        full_response = last_message.content
                        response_placeholder.markdown(full_response)
                
                # Try to parse chart spec from final response and render a chart
                def _extract_chart_spec(text: str):
                    debug = {"found": False, "candidates": [], "selected": None, "errors": []}
                    if not text:
                        return None, text, debug
                    patterns = [
                        r"```chart\s*\n([\s\S]*?)\n?```",
                        r"```json\s*\n([\s\S]*?)\n?```",
                        r"```\s*\n([\s\S]*?)\n?```",
                    ]
                    matches = []
                    for p in patterns:
                        for m in re.finditer(p, text):
                            block = m.group(1)
                            start, end = m.span()
                            matches.append({"pattern": p, "block": block, "span": (start, end)})
                    debug["candidates"] = [{"pattern": m["pattern"], "span": m["span"], "preview": m["block"][:200]} for m in matches]
                    cleaned_text = text
                    selected_spec = None
                    selected_span = None
                    for idx, m in enumerate(matches):
                        try:
                            spec = json.loads(m["block"])
                            if isinstance(spec, dict) and "chart_type" in spec:
                                selected_spec = spec
                                selected_span = m["span"]
                                debug["selected"] = {"index": idx, "pattern": m["pattern"], "chart_type": spec.get("chart_type")}
                                break
                        except Exception as e:
                            debug["errors"].append(str(e))
                            continue
                    if selected_spec:
                        debug["found"] = True
                        # remove only the selected span from text
                        s, e = selected_span
                        cleaned_text = (text[:s] + text[e:]).strip()
                        return selected_spec, cleaned_text, debug
                    return None, text, debug

                def _render_chart(spec: dict):
                    if not isinstance(spec, dict):
                        return False
                    chart_type = spec.get("chart_type", "table")
                    title = spec.get("title")
                    if title:
                        st.subheader(title)
                    data = spec.get("data", []) or []
                    label_format = spec.get("label_format", "number")

                    def fmt_value(v):
                        if v is None:
                            return ""
                        if label_format == "percent":
                            # accept 0-1 or 0-100
                            try:
                                f = float(v)
                                if f <= 1.0:
                                    f *= 100.0
                                return f"{f:.1f}%"
                            except Exception:
                                return str(v)
                        return f"{v:,}" if isinstance(v, (int, float)) else str(v)

                    if chart_type == "value":
                        val = spec.get("value")
                        st.metric(label=title or "Value", value=fmt_value(val))
                        return True

                    if chart_type == "donut":
                        val = spec.get("value")
                        if val is None and data:
                            # Attempt to read from y field
                            y_field = spec.get("y")
                            if isinstance(data, list) and data and y_field in data[0]:
                                val = data[0].get(y_field)
                            # If still None, try to find any percentage/proficiency field
                            elif isinstance(data, list) and data:
                                for key in data[0].keys():
                                    if any(x in key.lower() for x in ["pct", "prof", "percent", "proficiency"]):
                                        val = data[0].get(key)
                                        break
                        try:
                            v = float(val)
                            if v <= 1.0:
                                v *= 100.0
                            v = max(0.0, min(100.0, v))
                            fig = go.Figure(data=[go.Pie(values=[v, 100.0 - v], labels=["Value", "Remaining"], hole=0.6)])
                            st.plotly_chart(fig, use_container_width=True)
                            st.caption(fmt_value(v))
                            return True
                        except Exception:
                            return False

                    if chart_type in ("bar", "stacked_bar"):
                        x_field = spec.get("x")
                        y_field = spec.get("y")
                        series_field = spec.get("series")
                        if not data or not x_field or not y_field:
                            return False
                        fig = go.Figure()
                        if isinstance(y_field, list) and not series_field:
                            # multiple y columns → one trace per y
                            x_vals = [row.get(x_field) for row in data]
                            for yk in y_field:
                                y_vals = [row.get(yk) for row in data]
                                fig.add_bar(name=str(yk), x=x_vals, y=y_vals)
                        elif series_field:
                            # long form with series column
                            # group by series values
                            series_values = []
                            for row in data:
                                sv = row.get(series_field)
                                if sv not in series_values:
                                    series_values.append(sv)
                            x_values_unique = []
                            for row in data:
                                xv = row.get(x_field)
                                if xv not in x_values_unique:
                                    x_values_unique.append(xv)
                            for sv in series_values:
                                y_vals = []
                                for xv in x_values_unique:
                                    match = next((r for r in data if r.get(series_field) == sv and r.get(x_field) == xv), None)
                                    y_vals.append(match.get(y_field) if match else 0)
                                fig.add_bar(name=str(sv), x=x_values_unique, y=y_vals)
                            if chart_type == "stacked_bar":
                                fig.update_layout(barmode="stack")
                        else:
                            # simple single series bar
                            x_vals = [row.get(x_field) for row in data]
                            y_vals = [row.get(y_field) for row in data]
                            fig.add_bar(x=x_vals, y=y_vals)
                        st.plotly_chart(fig, use_container_width=True)
                        return True

                    if chart_type == "pie":
                        label_field = spec.get("label_field")
                        value_field = spec.get("value_field")
                        if not data or not label_field or not value_field:
                            return False
                        labels = [row.get(label_field) for row in data]
                        values = [row.get(value_field) for row in data]
                        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0)])
                        st.plotly_chart(fig, use_container_width=True)
                        return True

                    if chart_type == "table":
                        st.dataframe(data)
                        return True

                    return False

                spec, cleaned_text, chart_debug = _extract_chart_spec(full_response)
                if spec:
                    # Replace the streamed markdown with cleaned text sans the chart block
                    response_placeholder.markdown(cleaned_text)
                    _render_chart(spec)
                else:
                    # No chart spec; show text only
                    pass

                # Optional debug panel
                if st.session_state.chart_debug:
                    with st.expander("Chart Debug"):
                        st.write({k: v for k, v in chart_debug.items() if k != "candidates"})
                        if chart_debug.get("candidates"):
                            st.caption("Candidates (previews):")
                            for i, c in enumerate(chart_debug["candidates"]):
                                st.code(c.get("preview", ""))

                # Add assistant response to history
                st.session_state.messages.append({"role": "assistant", "content": full_response})
                
            except Exception as e:
                error_msg = f"An error occurred: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

# Sidebar with examples and info
with st.sidebar:
    st.header("💡 Example Questions")
    st.checkbox("Enable Chart Debug", key="chart_debug")
    
    examples = [
        "Show top 10 districts by Math proficiency in the latest year",
        "What is the average ELA proficiency for Hispanic students in grade 5?",
        "Compare Math scores for English learners vs. all students in 2025",
        "Which schools in Los Angeles Unified have the highest proficiency?",
        "Show ELA trends for socioeconomically disadvantaged students over the last 2 years",
    ]
    
    # Render examples as styled, non-interactive chips
    st.markdown(
        """
        <style>
        .example-grid { display: grid; grid-template-columns: 1fr; gap: 0.5rem; }
        .example-chip {
            padding: 0.5rem 0.75rem;
            background: #f6f8fa;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            font-size: 0.95rem;
            line-height: 1.3;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='example-grid'>" +
        "".join([f"<div class='example-chip'>{e}</div>" for e in examples]) +
        "</div>",
        unsafe_allow_html=True,
    )
    
    st.divider()
    
    st.header("📊 About the Data")
    st.markdown("""
    This assistant analyzes California Assessment of Student Performance and Progress (CAASPP) data, including:
    - **Years**: 2024 and 2025
    - **Subjects**: Math, ELA (English Language Arts)
    - **Grades**: 3-8, 11
    - **Metrics**: Proficiency rates, mean scale scores, student counts
    - **Breakdowns**: By county, district, school, student subgroup
    """)

    st.divider()
    st.subheader("🩺 Diagnostics")
    if diagnostics.get("can_connect"):
        st.success("Database connection: OK")
    else:
        st.error("Database connection: FAILED")
        err = diagnostics.get("connect_error")
        if err:
            st.caption(err)

    fact_count = diagnostics.get("fact_scores_count")
    if fact_count is not None:
        st.write(f"Rows in analytics.fact_scores: {fact_count:,}")
    elif diagnostics.get("fact_scores_error"):
        st.caption(f"fact_scores error: {diagnostics['fact_scores_error']}")

    max_year = diagnostics.get("max_year")
    if max_year is not None:
        st.write(f"Latest year (dim_year): {max_year}")
    elif diagnostics.get("year_error"):
        st.caption(f"year error: {diagnostics['year_error']}")

    with st.expander("Show table info"):
        ti = diagnostics.get("table_info")
        if isinstance(ti, str):
            st.text(ti)
        else:
            st.write(ti)
    
    with st.expander("Last SQL attempt"):
        st.caption("Shows the most recent SQL the agent tried to run and any error returned.")
        try:
            last_q = getattr(sql_toolkit, "last_query_text", None)
            last_e = getattr(sql_toolkit, "last_error_text", None)
            if last_q:
                st.code(last_q, language="sql")
            if last_e:
                st.caption(last_e)
            if not last_q and not last_e:
                st.write("No SQL attempts yet in this session.")
        except Exception as _:
            st.write("Unable to read last SQL attempt.")
    with st.expander("Agent System Instructions"):
        try:
            st.code(system_instructions)
        except Exception:
            st.caption("Unable to load system instructions.")
    
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
