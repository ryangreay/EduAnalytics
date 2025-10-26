import os
import sys
from pathlib import Path
import streamlit as st
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
    agent, sql_toolkit = create_sql_agent(
        pg_url=PG_URL,
        whitelist_path=str(PROJECT_ROOT / "app" / "schema_whitelist.json")
    )
    return agent, sql_toolkit

try:
    agent, sql_toolkit = get_agent()
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
                
                # Add assistant response to history
                st.session_state.messages.append({"role": "assistant", "content": full_response})
                
            except Exception as e:
                error_msg = f"An error occurred: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

# Sidebar with examples and info
with st.sidebar:
    st.header("💡 Example Questions")
    
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
    
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
