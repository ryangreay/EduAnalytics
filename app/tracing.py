import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.callbacks.base import BaseCallbackHandler


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RequestTraceCallback(BaseCallbackHandler):
    """LangChain callback handler that captures LLM and tool telemetry."""

    def __init__(self, request_trace: "RequestTrace"):
        self.request_trace = request_trace
        self._llm_started_at: Dict[str, float] = {}

    def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any) -> None:
        run_id = str(kwargs.get("run_id"))
        self._llm_started_at[run_id] = time.perf_counter()
        self.request_trace.llm_calls += 1

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        run_id = str(kwargs.get("run_id"))
        started = self._llm_started_at.pop(run_id, None)
        if started is not None:
            self.request_trace.llm_latency_ms += round((time.perf_counter() - started) * 1000, 2)

        usage = {}
        if hasattr(response, "llm_output") and isinstance(response.llm_output, dict):
            usage = response.llm_output.get("token_usage") or {}

        prompt_tokens = usage.get("prompt_tokens", 0) or 0
        completion_tokens = usage.get("completion_tokens", 0) or 0
        total_tokens = usage.get("total_tokens", 0) or (prompt_tokens + completion_tokens)

        self.request_trace.prompt_tokens += int(prompt_tokens)
        self.request_trace.completion_tokens += int(completion_tokens)
        self.request_trace.total_tokens += int(total_tokens)


class RequestTrace:
    def __init__(self, question: str, session_id: Optional[str] = None, user_id: Optional[str] = None):
        self.trace_id = str(uuid.uuid4())
        self.session_id = session_id
        self.user_id = user_id
        self.question = question
        self.started_at = _utc_now_iso()
        self._started_counter = time.perf_counter()

        self.answer: Optional[str] = None
        self.latency_ms: Optional[float] = None

        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.total_tokens = 0
        self.llm_calls = 0
        self.llm_latency_ms = 0.0

        self.tool_calls: List[Dict[str, Any]] = []
        self.generated_sql: List[str] = []
        self._active_tool_index: Optional[int] = None

        self.callback_handler = RequestTraceCallback(self)

    def record_tool_start(self, name: str, tool_input: Any) -> None:
        self.tool_calls.append(
            {
                "name": name,
                "input": str(tool_input)[:4000] if tool_input is not None else None,
                "output_preview": None,
                "success": None,
                "latency_ms": None,
                "error": None,
                "started_at": _utc_now_iso(),
            }
        )
        self._active_tool_index = len(self.tool_calls) - 1

    def record_tool_end(
        self,
        tool_output: Any,
        success: bool,
        latency_ms: Optional[float] = None,
        error: Optional[str] = None,
    ) -> None:
        if self._active_tool_index is None:
            return
        call = self.tool_calls[self._active_tool_index]
        call["output_preview"] = str(tool_output)[:4000] if tool_output is not None else None
        call["success"] = bool(success)
        call["latency_ms"] = latency_ms
        call["error"] = error
        call["ended_at"] = _utc_now_iso()
        self._active_tool_index = None

    def record_sql(self, sql: str) -> None:
        if sql:
            self.generated_sql.append(sql)

    def finalize(self, answer: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self.answer = answer
        self.latency_ms = round((time.perf_counter() - self._started_counter) * 1000, 2)
        payload: Dict[str, Any] = {
            "trace_id": self.trace_id,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "question": self.question,
            "answer": self.answer,
            "started_at": self.started_at,
            "ended_at": _utc_now_iso(),
            "latency_ms": self.latency_ms,
            "llm": {
                "calls": self.llm_calls,
                "latency_ms": self.llm_latency_ms,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.total_tokens,
            },
            "tool_calls": self.tool_calls,
            "tool_calls_count": len(self.tool_calls),
            "generated_sql": self.generated_sql,
        }
        if extra:
            payload.update(extra)
        return payload


class AgentTraceLogger:
    """Persist JSONL traces for each user question."""

    def __init__(self, trace_file_path: Optional[str] = None):
        default_path = Path("logs") / "agent_traces.jsonl"
        path = Path(trace_file_path or os.getenv("AGENT_TRACE_PATH", str(default_path)))
        if not path.is_absolute():
            path = Path.cwd() / path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.trace_file_path = path

    def start_request(
        self,
        question: str,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> RequestTrace:
        return RequestTrace(question=question, session_id=session_id, user_id=user_id)

    def write(self, payload: Dict[str, Any]) -> None:
        with self.trace_file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
