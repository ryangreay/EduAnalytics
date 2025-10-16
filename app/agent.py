from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END

class S(TypedDict):
    history: List[Dict[str,str]]  # [{"role":"user"/"assistant","content": "..."}]
    question: str
    entities: List[Dict[str,Any]]
    sql: str
    rows: List[Dict[str,Any]]
    chart: Dict[str,Any]
    narration: str

def node_resolve(state: S, resolver) -> S:
    q = state["question"]
    hits = resolver.search(q, k=5)
    state["entities"] = hits
    return state

def node_sql(state: S, sqltool) -> S:
    # Compose a short history string
    hist = []
    for m in state["history"][-8:]:
        role = m["role"]
        hist.append(f"{role.upper()}: {m['content']}")
    history_text = "\n".join(hist)

    # Add soft hint from best entity match
    q = state["question"]
    if state["entities"]:
        e = state["entities"][0]
        label = " | ".join([str(e.get("county_name","")), str(e.get("district_name","")), str(e.get("school_name",""))]).strip(" |")
        q = f"{q}\n(Hint entity: {label})"

    sql = sqltool.generate_sql(history_text, q)
    state["sql"] = sql
    return state

def node_run(state: S, sqltool) -> S:
    rows, sql = sqltool.run_sql(state["sql"])
    state["rows"] = rows
    state["sql"] = sql
    return state

def node_chart(state: S, pick_chart_fn) -> S:
    state["chart"] = pick_chart_fn(state["rows"], state["question"])
    return state

def node_narrate(state: S) -> S:
    rc = len(state["rows"])
    state["narration"] = f"Returned {rc} row(s)."
    return state

def build_graph(resolver, sqltool, pick_chart_fn):
    g = StateGraph(S)
    g.add_node("resolve", lambda s: node_resolve(s, resolver))
    g.add_node("gen_sql", lambda s: node_sql(s, sqltool))
    g.add_node("run_sql", lambda s: node_run(s, sqltool))
    g.add_node("chart", lambda s: node_chart(s, pick_chart_fn))
    g.add_node("narrate", node_narrate)
    g.set_entry_point("resolve")
    g.add_edge("resolve","gen_sql")
    g.add_edge("gen_sql","run_sql")
    g.add_edge("run_sql","chart")
    g.add_edge("chart","narrate")
    g.add_edge("narrate", END)
    return g.compile()
