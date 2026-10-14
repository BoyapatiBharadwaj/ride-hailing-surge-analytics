import { useState } from "react";
import { fetchGold, fetchLive, fmt, pct, usePoll } from "./api.js";
import ChatPanel from "./components/ChatPanel.jsx";
import DriversTab from "./components/DriversTab.jsx";
import SurgeTab from "./components/SurgeTab.jsx";
import TodayTab from "./components/TodayTab.jsx";
import TripsTab from "./components/TripsTab.jsx";

const LIVE_MS = 15000;
const GOLD_MS = 120000;
const TARGET_ETA = 4.5;

function Card({ label, value, sub, tone }) {
  return (
    <div className="card">
      <div className="card-label">{label}</div>
      <div className="card-value">{value}</div>
      <div className={`card-sub ${tone || ""}`}>{sub || " "}</div>
    </div>
  );
}

const TABS = [
  { id: "trips", label: "🚕 Live trips" },
  { id: "drivers", label: "🧑‍✈️ Drivers" },
  { id: "surge", label: "🔥 Surge" },
  { id: "today", label: "📊 Today & forecast" },
];

export default function App() {
  const live = usePoll(fetchLive, LIVE_MS);
  const gold = usePoll(fetchGold, GOLD_MS);
  const [tab, setTab] = useState("trips");
  const d = live.data;
  const ok = d && d.ok;
  const t = ok ? d.trips : null;
  const dr = ok ? d.drivers : null;
  const s = ok ? d.surge : null;

  return (
    <div className="page">
      <header className="topbar">
        <div className="brand">
          <span className="logo">🚖</span>
          <div>
            <h1>Ride-Hailing Surge Ops</h1>
            <div className="subtitle">New York City · live operations and GenAI assistant</div>
          </div>
        </div>
        <div className="status">
          <span className={`dot ${ok ? "on" : "off"}`} />
          {ok ? (
            <span>
              Live from Kafka · {d.read_at_ny} New York ({d.read_at_utc} UTC) · every {LIVE_MS / 1000} s
            </span>
          ) : (
            <span>{live.loading ? "Connecting to the stream…" : "Stream not reachable"}</span>
          )}
        </div>
      </header>

      <div className="layout">
        <main className="main">
          {d && !d.ok && (
            <div className="alert">
              Live stream not reachable ({d.error}). Is the stack running? Check with <code>make status</code>.
            </div>
          )}
          {live.error && !d && <div className="alert">Server not reachable: {live.error}</div>}

          <section className="cards">
            <Card label="Ride requests · 15 min" value={t ? fmt(t.requests) : "–"} sub="riders asking for a car" />
            <Card label="Matched to a driver" value={t ? fmt(t.matched) : "–"}
                  sub={t && t.match_rate !== null ? `${pct(t.match_rate)} of requests` : ""} />
            <Card label="No driver available" value={t ? fmt(t.no_driver) : "–"} sub="unmatched requests"
                  tone={t && t.no_driver ? "bad" : ""} />
            <Card label="Avg rider ETA" value={t && t.avg_eta_min !== null ? `${fmt(t.avg_eta_min, 2)} min` : "–"}
                  sub={`target < ${TARGET_ETA} min`}
                  tone={t && t.avg_eta_min !== null ? (t.avg_eta_min < TARGET_ETA ? "good" : "bad") : ""} />
            <Card label="Drivers online" value={dr ? fmt(dr.online) : "–"}
                  sub={dr ? `${fmt(dr.available)} free · ${fmt(dr.on_trip)} on a trip` : ""} />
            <Card label="Zones surging" value={s ? fmt(s.surging) : "–"}
                  sub={s && s.max !== null ? `highest ${fmt(s.max, 1)}x` : ""} />
          </section>

          <nav className="tabs">
            {TABS.map((x) => (
              <button key={x.id} className={tab === x.id ? "tab active" : "tab"} onClick={() => setTab(x.id)}>
                {x.label}
              </button>
            ))}
          </nav>

          <section className="panel">
            {tab === "trips" && <TripsTab trips={t} />}
            {tab === "drivers" && <DriversTab drivers={dr} />}
            {tab === "surge" && <SurgeTab surge={s} />}
            {tab === "today" && <TodayTab gold={gold} />}
          </section>
        </main>

        <aside className="side">
          <ChatPanel backend={gold.data ? gold.data.backend : null} />
        </aside>
      </div>
    </div>
  );
}
