from typing import Annotated, Sequence
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import create_react_agent

import json, os
from .prompts import REACT_SYSTEM_PROMPT
from .tools_sql import SQLToolkit
from .tools_entity import EntityResolver

from .prompts import REACT_SYSTEM_PROMPT, REACT_SYSTEM_CHARTS_PROMPT


class AgentState(TypedDict):
    """State for the ReAct agent"""
    messages: Annotated[Sequence[BaseMessage], add_messages]
    sql_error_count: int  # Track consecutive SQL errors


def create_sql_agent(pg_url: str, whitelist_path: str, charts_enabled: bool = True):
    """Create a ReAct agent for SQL question answering with entity lookup
    
    Args:
        pg_url: PostgreSQL connection URL
        whitelist_path: Path to schema whitelist JSON
        charts_enabled: If True, include charting instructions in system prompt
    """
    
    # Initialize components
    sql_toolkit = SQLToolkit(pg_url, whitelist_path)
    entity_resolver = EntityResolver()
    
    # Get tools with error tracking
    tools = sql_toolkit.get_tools_with_retry_limit(max_attempts=3)
    
    # Add entity resolver tool if enabled
    if entity_resolver.enabled:
        tools.append(entity_resolver.as_tool())
    
    # Create LLM
    llm = ChatOpenAI(model=os.getenv("LLM_MODEL"), temperature=0)
    
    # Create ReAct agent; fold system prompt into state_modifier (current API doesn't take `prompt`)
    # Load SQL examples if available and append to system instructions
    examples_path = os.path.join(os.path.dirname(__file__), "sql_examples.json")
    examples_text = ""
    try:
        if os.path.exists(examples_path):
            with open(examples_path, "r", encoding="utf-8") as f:
                ex = json.load(f)
            if isinstance(ex, list) and len(ex) > 0:
                blocks = []
                for item in ex:
                    q = item.get("question", "").strip()
                    notes = item.get("notes", "").strip()
                    sql = item.get("sql", "").strip()
                    chart_spec = item.get("chart_spec_example", "").strip()
                    if not q or not sql:
                        continue
                    block = f"Q: {q}\n" + (f"Notes: {notes}\n" if notes else "") + f"SQL:\n{sql}"
                    # Only include chart specs if charts are enabled
                    if chart_spec and charts_enabled:
                        block += f"\n\nChart Spec:\n{chart_spec}"
                    blocks.append(block)
                if blocks:
                    example_label = "Examples (Q→SQL→Chart):" if charts_enabled else "Examples (Q→SQL):"
                    examples_text = f"\n\n{example_label}\n" + "\n\n".join(blocks)
    except Exception:
        examples_text = ""
    
    combined_system_instructions = (
        f"{REACT_SYSTEM_PROMPT}\n\n"
        f"{REACT_SYSTEM_CHARTS_PROMPT if charts_enabled else ""}\n\n"
        "You are analyzing California education data. Stay focused on answering the user's question." 
        f"{examples_text}"
    )
    agent = create_react_agent(
        llm,
        tools,
        state_modifier=combined_system_instructions,
    )
    
    return agent, sql_toolkit, combined_system_instructions


def run_agent_query(agent, question: str, history: list = None):
    """
    Run a query through the agent
    
    Args:
        agent: The LangGraph agent
        question: User's question
        history: Optional list of previous messages [{"role": "user"/"assistant", "content": "..."}]
    
    Returns:
        dict with 'response' (agent's final answer) and 'messages' (full message history)
    """
    # Convert history to messages
    messages = []
    if history:
        for msg in history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(AIMessage(content=msg["content"]))
    
    # Add current question
    messages.append(HumanMessage(content=question))
    
    # Run agent
    result = agent.invoke({"messages": messages})
    
    # Extract final response
    final_message = result["messages"][-1]
    response = final_message.content if hasattr(final_message, 'content') else str(final_message)
    
    return {
        "response": response,
        "messages": result["messages"]
    }
