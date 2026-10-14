import { useEffect, useRef, useState } from "react";

// Small client for the Python server (genai/ops_server.py).
async function getJSON(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return r.json();
}

export const fetchLive = () => getJSON("/api/live");
export const fetchGold = () => getJSON("/api/gold");

export async function askAssistant(question, history) {
  const r = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, history }),
  });
  if (!r.ok) throw new Error(`chat: HTTP ${r.status}`);
  return r.json();
}

// polling hook: calls `fn` now and every `ms`, keeps the last good value and the last error

export function usePoll(fn, ms) {
  const [state, setState] = useState({ data: null, error: null, loading: true });
  const timer = useRef(null);
  useEffect(() => {
    let alive = true;
    const run = async () => {
      try {
        const data = await fn();
        if (alive) setState({ data, error: null, loading: false });
      } catch (e) {
        if (alive) setState((s) => ({ ...s, error: String(e.message || e), loading: false }));
      } finally {
        if (alive) timer.current = setTimeout(run, ms);
      }
    };
    run();
    return () => {
      alive = false;
      clearTimeout(timer.current);
    };
  }, [fn, ms]);
  return state;
}

export const fmt = (v, digits = 0, suffix = "") =>
  v === null || v === undefined || Number.isNaN(v)
    ? "–"
    : `${Number(v).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits })}${suffix}`;
export const pct = (v, digits = 0) => (v === null || v === undefined ? "–" : `${(v * 100).toFixed(digits)}%`);
