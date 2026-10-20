import DOMPurify from "dompurify";
import { marked } from "marked";
import { useEffect, useRef, useState } from "react";
import { askAssistant } from "../api.js";

const EXAMPLES = [
  "Which zones are surging right now?",
  "What is the surge at JFK Airport?",
  "How many zones have surge right now?",
  "Forecast demand for Midtown",
  "Give me the KPI summary for the latest day",
  "What was driver utilisation today?",
];

const html = (md) => ({ __html: DOMPurify.sanitize(marked.parse(md || "", { breaks: true })) });

export default function ChatPanel({ backend }) {
  const [msgs, setMsgs] = useState([]);
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const end = useRef(null);

  useEffect(() => { end.current?.scrollIntoView({ behavior: "smooth", block: "end" }); }, [msgs, busy]);

  async function send(q) {
    const question = (q ?? text).trim();
    if (!question || busy) return;
    const history = msgs.map(({ role, content }) => ({ role, content }));
    setMsgs((m) => [...m, { role: "user", content: question }]);
    setText("");
    setBusy(true);
    try {
      const r = await askAssistant(question, history);
      setMsgs((m) => [...m, { role: "assistant", content: r.answer, trace: r.trace || [] }]);
    } catch (e) {
      setMsgs((m) => [...m, { role: "assistant", content: `⚠️ ${e.message || e}`, trace: [] }]);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="chat">
      <div className="chat-head">
        <h2>💬 Ops assistant</h2>
        <div className="src">Answers from the Gold tables{backend ? ` (${backend})` : ""} through 5 read-only tools</div>
      </div>

      <div className="chat-body">
        {msgs.length === 0 && (
          <div className="examples">
            <div className="muted">Ask about surge, demand, drivers, ETA, revenue or the forecast. Try:</div>
            {EXAMPLES.map((e) => (
              <button key={e} className="chip" onClick={() => send(e)} disabled={busy}>{e}</button>
            ))}
          </div>
        )}
        {msgs.map((m, i) => (
          <div key={i} className={`msg ${m.role}`}>
            <div className="bubble">
              {m.role === "assistant" ? <div className="md" dangerouslySetInnerHTML={html(m.content)} /> : m.content}
              {(m.trace || []).map((t, j) => (
                <details key={j} className="trace">
                  <summary>tool: {t.tool}</summary>
                  <pre>{JSON.stringify({ args: t.args, result: t.result }, null, 2)}</pre>
                </details>
              ))}
            </div>
          </div>
        ))}
        {busy && (
          <div className="msg assistant">
            <div className="bubble typing"><span /><span /><span /></div>
          </div>
        )}
        <div ref={end} />
      </div>

      <form className="chat-input" onSubmit={(e) => { e.preventDefault(); send(); }}>
        <input value={text} onChange={(e) => setText(e.target.value)} placeholder="e.g. Which zones are surging right now?"
               disabled={busy} aria-label="Your question" />
        <button type="submit" disabled={busy || !text.trim()}>Ask</button>
      </form>
      {msgs.length > 0 && (
        <button className="link" onClick={() => setMsgs([])} disabled={busy}>Clear chat</button>
      )}
    </div>
  );
}
