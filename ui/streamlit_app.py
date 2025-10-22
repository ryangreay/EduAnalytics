import os
import streamlit as st
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage

from app.agent import create_sql_agent

load_dotenv()

st.set_page_config(page_title="CAASPP SQL Chat", layout="wide")
st.title("🎓 CAASPP ELA/Math AI Assistant")

st.markdown("""
Ask questions about California education data and I'll help you find answers using SQL queries and entity lookups.
""")

PG_URL = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

# Initialize agent
@st.cache_resource
def get_agent():
    agent, sql_toolkit = create_sql_agent(
        pg_url=PG_URL,
        whitelist_path="app/schema_whitelist.json"
    )
    return agent, sql_toolkit

try:
    agent, sql_toolkit = get_agent()
except Exception as e:
    st.error(f"Failed to initialize agent: {e}")
    st.stop()

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
        "Compare Math scores for English learners vs. all students in 2023",
        "Which schools in Los Angeles Unified have the highest proficiency?",
        "Show ELA trends for socioeconomically disadvantaged students over the last 3 years",
    ]
    
    for example in examples:
        if st.button(example, key=example):
            st.session_state.messages.append({"role": "user", "content": example})
            st.rerun()
    
    st.divider()
    
    st.header("📊 About the Data")
    st.markdown("""
    This assistant analyzes California Assessment of Student Performance and Progress (CAASPP) data, including:
    - **Subjects**: Math, ELA (English Language Arts)
    - **Grades**: 3-8, 11
    - **Metrics**: Proficiency rates, mean scale scores, student counts
    - **Breakdowns**: By county, district, school, student subgroup
    """)
    
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
