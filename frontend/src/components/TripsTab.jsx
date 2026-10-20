import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, pct } from "../api.js";

const COLOURS = { requests: "#2457c5", matched: "#1f8a5b", "no driver": "#c8611a" };

export default function TripsTab({ trips }) {
  if (!trips) return <Empty />;
  return (
    <>
      <h3>Requests per minute · last 30 minutes (New York time)</h3>
      <div className="chart">
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={trips.per_minute} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="#eef0f4" vertical={false} />
            <XAxis dataKey="minute" tick={{ fontSize: 11, fill: "#5b6474" }} interval="preserveStartEnd" minTickGap={24} />
            <YAxis tick={{ fontSize: 11, fill: "#5b6474" }} allowDecimals={false} width={36} />
            <Tooltip />
            <Legend verticalAlign="top" height={28} iconType="plainline" />
            {Object.entries(COLOURS).map(([k, c]) => (
              <Line key={k} type="monotone" dataKey={k} stroke={c} strokeWidth={2} dot={false} isAnimationActive={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <p className="src">requests = riders asking for a car (Kafka trip_requests) · matched / no driver = dispatch results (trip_matches)</p>

      <div className="mini">
        <Mini label="Revenue · 15 min" value={`USD ${fmt(trips.revenue, 2)}`} />
        <Mini label="Revenue per km" value={trips.revenue_per_km !== null ? `USD ${fmt(trips.revenue_per_km, 2)}` : "–"} />
        <Mini label="Trips with surge > 1.0x" value={fmt(trips.surge_trips)} />
        <Mini label="Matched share" value={pct(trips.match_rate)} />
      </div>

      <h3>Latest trips</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time (NYC)</th><th>Status</th><th>Pickup</th><th>Drop-off</th>
              <th className="num">Driver</th><th className="num">ETA min</th><th className="num">Fare USD</th>
              <th className="num">km</th><th className="num">Surge</th>
            </tr>
          </thead>
          <tbody>
            {trips.feed.map((r, i) => {
              const ok = r.status === "MATCHED";
              return (
                <tr key={i}>
                  <td className="mono">{r["time (NYC)"] || "–"}</td>
                  <td><span className={ok ? "pill good" : "pill bad"}>{ok ? "matched" : "no driver"}</span></td>
                  <td>{r.pickup}</td>
                  <td>{r["drop-off"]}</td>
                  <td className="num">{ok && r.driver != null ? r.driver : "–"}</td>
                  <td className="num">{ok && r["ETA min"] != null ? fmt(r["ETA min"], 1) : "–"}</td>
                  <td className="num">{ok && r["fare USD"] != null ? fmt(r["fare USD"], 2) : "–"}</td>
                  <td className="num">{r.km != null ? fmt(r.km, 1) : "–"}</td>
                  <td className="num">{r.surge != null ? `${fmt(r.surge, 1)}x` : "–"}</td>
                </tr>
              );
            })}
            {trips.feed.length === 0 && (
              <tr><td colSpan={9} className="muted">No trips in the last 30 minutes yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function Mini({ label, value, sub }) {
  return (
    <div className="mini-item">
      <div className="card-label">{label}</div>
      <div className="mini-value">{value}</div>
      {sub && <div className="card-sub">{sub}</div>}
    </div>
  );
}

export function Empty({ text }) {
  return <div className="muted pad">{text || "Waiting for live data from Kafka…"}</div>;
}
