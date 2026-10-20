import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, pct } from "../api.js";
import { Empty, Mini } from "./TripsTab.jsx";

export default function TodayTab({ gold }) {
  if (gold.loading && !gold.data) return <Empty text="Loading the Gold tables…" />;
  const g = gold.data || {};
  const k = g.kpi || {};
  const tr = k.trips || {};
  const sg = k.surge || {};
  const ut = k.utilisation || {};
  const fc = g.forecast || [];
  return (
    <>
      {g.kpi_error || !g.kpi ? (
        <div className="info">Gold KPIs not available yet ({g.kpi_error || gold.error || "no data"}) – the hourly DAG builds them in Databricks.</div>
      ) : (
        <>
          <h3>Latest day in Gold ({k.date}){k.note ? ` – ${k.note}` : ""}</h3>
          <div className="mini">
            <Mini label="Requests" value={fmt(tr.requests)} />
            <Mini label="Completed" value={fmt(tr.completed_trips)} />
            <Mini label="Cancelled / no driver" value={fmt(tr.cancelled_trips)} />
            <Mini label="Avg rider ETA" value={tr.avg_rider_eta_min != null ? `${fmt(tr.avg_rider_eta_min, 2)} min` : "–"} sub="target < 4.5 min" />
          </div>
          <div className="mini">
            <Mini label="Driver utilisation" value={pct(ut.avg_utilisation, 1)} sub="target 78 %" />
            <Mini label="Revenue per km" value={tr.revenue_per_km != null ? `USD ${fmt(tr.revenue_per_km, 2)}` : "–"} />
            <Mini label="Average surge" value={sg.avg_multiplier != null ? `${fmt(sg.avg_multiplier, 2)}x` : "–"} />
            <Mini label="Max surge" value={sg.max_multiplier != null ? `${fmt(sg.max_multiplier, 1)}x` : "–"} />
          </div>
        </>
      )}

      <h3>Demand forecast – next 30 minutes (top zones)</h3>
      {g.forecast_error || fc.length === 0 ? (
        <div className="info">No live forecast yet ({g.forecast_error || "the hourly DAG scores the next 30 minutes"}).</div>
      ) : (
        <>
          <div className="chart">
            <ResponsiveContainer width="100%" height={30 * fc.length + 40}>
              <BarChart data={fc} layout="vertical" margin={{ top: 4, right: 24, left: 8, bottom: 4 }}>
                <CartesianGrid stroke="#eef0f4" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11, fill: "#5b6474" }} />
                <YAxis type="category" dataKey="zone" width={250} tick={{ fontSize: 11, fill: "#1d2433" }} />
                <Tooltip formatter={(v) => [fmt(v, 1), "expected requests"]} />
                <Bar dataKey="predicted_demand" fill="#6b47b8" radius={[0, 4, 4, 0]} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <p className="src">
            Expected ride requests in the next 30-minute slot · model {fc[0].model_version} · slot starting{" "}
            {String(fc[0].forecast_ts).slice(11, 16)} NYC
          </p>
        </>
      )}
      <p className="src">Source: Gold tables – {g.backend || "…"} · refreshed hourly by Airflow (or now with <code>make hourly</code>)</p>
    </>
  );
}
