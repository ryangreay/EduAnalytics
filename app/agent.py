from typing import Annotated, Sequence
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import create_react_agent

from .prompts import REACT_SYSTEM_PROMPT
from .tools_sql import SQLToolkit
from .tools_entity import EntityResolver


class AgentState(TypedDict):
    """State for the ReAct agent"""
    messages: Annotated[Sequence[BaseMessage], add_messages]
    sql_error_count: int  # Track consecutive SQL errors


def create_sql_agent(pg_url: str, whitelist_path: str):
    """Create a ReAct agent for SQL question answering with entity lookup"""
    
    # Initialize components
    sql_toolkit = SQLToolkit(pg_url, whitelist_path)
    entity_resolver = EntityResolver()
    
    # Get tools with error tracking
    tools = sql_toolkit.get_tools_with_retry_limit(max_attempts=3)
    
    # Add entity resolver tool if enabled
    if entity_resolver.enabled:
        tools.append(entity_resolver.as_tool())
    
    # Create LLM
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    
    # Create ReAct agent with iteration limit
    agent = create_react_agent(
        llm,
        tools,
        prompt=REACT_SYSTEM_PROMPT,
        state_modifier="You are analyzing California education data. Stay focused on answering the user's question."
    )
    
    return agent, sql_toolkit


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
