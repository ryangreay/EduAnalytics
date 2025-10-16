import os, pandas as pd, plotly.express as px, streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

from app.tools_entity import EntityResolver
from app.tools_sql import SQLTool
from app.chart_rules import pick_chart
from app.agent import build_graph

load_dotenv()

st.set_page_config(page_title="CAASPP SQL Chat", layout="wide")
st.title("CAASPP ELA/Math – Conversational SQL")

PG_URL = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

if "history" not in st.session_state:
    st.session_state.history = []  # chat memory

resolver = EntityResolver(index_dir="./data/faiss")
sqltool = SQLTool(pg_url=PG_URL, whitelist_path="app/schema_whitelist.json")
app = build_graph(resolver, sqltool, pick_chart)

# Display chat
for m in st.session_state.history:
    if m["role"] == "user":
        st.chat_message("user").markdown(m["content"])
    else:
        st.chat_message("assistant").markdown(m["content"])

prompt = st.chat_input("Ask about Math/ELA (e.g., Show top districts by Math proficiency in the latest year)")
if prompt:
    st.session_state.history.append({"role":"user","content":prompt})
    result = app.invoke({"history": st.session_state.history, "question": prompt})
    # Render
    with st.chat_message("assistant"):
        st.markdown(result["narration"])
        rows = result["rows"]
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)
            ch = result["chart"]
            if ch["type"] == "bar":
                fig = px.bar(df, x=ch["x"], y=ch["y"])
                st.plotly_chart(fig, use_container_width=True)
            elif ch["type"] == "donut":
                col = ch["y"]
                d2 = pd.DataFrame({col:[df.iloc[0][col]], "label":[col]})
                fig = px.pie(d2, values=col, names="label", hole=.6)
                st.plotly_chart(fig, use_container_width=True)
    st.session_state.history.append({"role":"assistant","content":result["narration"]})
