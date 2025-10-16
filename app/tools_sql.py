import json, os
from sqlalchemy import create_engine, text
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase

from .prompts import SYSTEM_SQL, USER_SQL_SUFFIX

class SQLTool:
    def __init__(self, pg_url: str, whitelist_path: str):
        self.engine = create_engine(pg_url, pool_pre_ping=True)
        with open(whitelist_path,"r") as f:
            self.schema = json.load(f)
        self.db = SQLDatabase.from_uri(pg_url, include_tables=list(self.schema["tables"].keys()))
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    def generate_sql(self, history: str, question: str) -> str:
        prompt = SYSTEM_SQL + USER_SQL_SUFFIX.format(
            schema=json.dumps(self.schema, indent=2),
            history=history,
            question=question
        )
        resp = self.llm.invoke(prompt)
        return resp.content.strip()

    def run_sql(self, sql: str):
        # naive guard
        lower = sql.lower()
        if any(x in lower for x in ["insert","update","delete","drop","alter"]):
            return [], "/* blocked non-SELECT */ SELECT 1 WHERE 1=0"
        with self.engine.begin() as con:
            rows = con.execute(text(sql)).fetchall()
            return [dict(r._mapping) for r in rows], sql
