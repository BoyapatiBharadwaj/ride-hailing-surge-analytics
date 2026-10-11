"""GenAI Ops assistant: Groq chat-completions with tool calling over the fixed, read-only Gold tools.

    python -m genai.assistant "Which zones are surging right now?"

The model never sees SQL and never writes it: it chooses a tool + arguments, we run the tool, and feed
the JSON result back. The system prompt forbids numbers that did not come from a tool result.
"""
import json
import sys
from typing import Any, Dict, List, Tuple

from common.config import env
from genai.tools import TOOL_SPECS, run_tool

MAX_ROUNDS = 6

SYSTEM_PROMPT = """You are the Ops assistant for a ride-hailing surge-pricing platform in New York City.
Answer operations questions (surge, demand, driver utilisation, rider wait time, forecasts, daily KPIs).

Rules:
- Use the provided tools for every factual or numeric claim. NEVER invent or estimate numbers.
- If a tool returns no data or an error, say so plainly and suggest what is missing (for example: the stream has not produced data yet).
- Zone names can match several taxi zones; mention which zones you used.
- Surge multipliers are shown like 1.5x. Utilisation target is 78%, rider ETA target is 4.5 minutes.
- Keep answers short and concrete: lead with the answer, then 1-3 supporting numbers.
- All timestamps in tool results are already New York local time: show them exactly as given (never convert them) and say "NYC time".
- For questions about all zones, zone counts or the lowest values, use top_zones (order "lowest" for the lowest) and its
  summary_all_zones, which covers every zone; the listed rows are only the top or bottom N.
- Write money as "USD 1,234" (no dollar sign). If a tool result has a "note", mention it.
- You are read-only. You cannot change prices or any data; if asked to, explain that you can only report.
"""


def _client():
    from groq import Groq
    key = env("GROQ_API_KEY")
    if not key or "FILL" in key:
        raise RuntimeError("GROQ_API_KEY is not set in .env (get a free key at https://console.groq.com/keys)")
    return Groq(api_key=key)


_PREFER = ("gpt-oss-120b", "llama-3.3", "llama-4", "gpt-oss-20b", "qwen", "llama-3.1-70b", "llama-3.1-8b", "llama")
_SKIP = ("whisper", "guard", "tts", "playai", "distil", "embed", "allam")


def pick_model(client, current: str) -> str:
    """The configured model may be retired; choose an available chat model that supports tool calling."""
    ids = [m.id for m in client.models.list().data]
    if current in ids:
        return current
    cands = [i for i in ids if not any(k in i.lower() for k in _SKIP)]
    for pref in _PREFER:
        for i in cands:
            if pref in i.lower():
                return i
    return cands[0] if cands else current


def ask(question: str, history: List[Dict[str, Any]] = None) -> Tuple[str, List[Dict[str, Any]]]:
    """Returns (answer, tool_trace). `history` is a list of prior {role, content} chat turns."""
    client = _client()
    model = env("GROQ_MODEL", "openai/gpt-oss-120b")
    messages: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += [m for m in (history or []) if m.get("role") in ("user", "assistant")]
    messages.append({"role": "user", "content": question})
    trace: List[Dict[str, Any]] = []

    for _ in range(MAX_ROUNDS):
        try:
            resp = client.chat.completions.create(model=model, messages=messages, tools=TOOL_SPECS,
                                                  tool_choice="auto", temperature=0.1, max_tokens=700)
        except Exception as exc:  # noqa: BLE001
            if "model" in str(exc).lower() and ("404" in str(exc) or "not exist" in str(exc) or "decommission" in str(exc)):
                new = pick_model(client, model)
                if new == model:
                    raise
                model = new
                continue
            raise
        msg = resp.choices[0].message
        calls = msg.tool_calls or []
        if not calls:
            return (msg.content or "").strip(), trace
        messages.append({"role": "assistant", "content": msg.content or "",
                         "tool_calls": [{"id": c.id, "type": "function",
                                         "function": {"name": c.function.name, "arguments": c.function.arguments}}
                                        for c in calls]})
        for c in calls:
            try:
                args = json.loads(c.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            result = run_tool(c.function.name, args)
            trace.append({"tool": c.function.name, "args": args, "result": result})
            messages.append({"role": "tool", "tool_call_id": c.id, "name": c.function.name,
                             "content": json.dumps(result, default=str)[:6000]})
    return "I could not finish answering within the tool-call limit. Please ask a narrower question.", trace


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "Which 5 zones have the highest surge right now?"
    answer, tr = ask(q)
    for t in tr:
        print(f"[tool] {t['tool']}({t['args']})")
    print(answer)
