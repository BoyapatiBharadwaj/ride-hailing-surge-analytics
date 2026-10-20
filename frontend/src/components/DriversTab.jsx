import { CircleMarker, Tooltip } from "react-leaflet";
import { fmt, pct } from "../api.js";
import NycMap from "./NycMap.jsx";
import { Empty, Mini } from "./TripsTab.jsx";

const FREE = "#1f8a5b";
const BUSY = "#c8611a";

export default function DriversTab({ drivers }) {
  if (!drivers) return <Empty />;
  const u = drivers.utilisation_now;
  return (
    <>
      <div className="mini">
        <Mini label="Online (pinged in 90 s)" value={fmt(drivers.online)} />
        <Mini label="Available" value={fmt(drivers.available)} />
        <Mini label="On a trip" value={fmt(drivers.on_trip)} />
        <Mini label="Busy right now" value={pct(u)} sub="target 78 % over a day" />
        <Mini label="Scala producer drivers" value={fmt(drivers.scala_online)} sub="ids 701–800" />
      </div>
      <NycMap>
        {drivers.points.map(([lat, lon, busy, id, zone]) => (
          <CircleMarker key={id} center={[lat, lon]} radius={3.5}
                        pathOptions={{ color: busy ? BUSY : FREE, fillColor: busy ? BUSY : FREE, fillOpacity: 0.85, weight: 0 }}>
            <Tooltip>driver {id} · {busy ? "on a trip" : "available"}<br />{zone}</Tooltip>
          </CircleMarker>
        ))}
      </NycMap>
      <p className="src">
        <span className="legend" style={{ background: FREE }} /> available ·{" "}
        <span className="legend" style={{ background: BUSY }} /> on a trip · source: Kafka driver_gps_pings
        (drivers 1–700 Python simulator, 701–800 Scala producer while <code>make scala-producer</code> runs)
      </p>
      <h3>Most free drivers by zone</h3>
      <div className="table-wrap">
        <table>
          <thead><tr><th>Zone</th><th className="num">Free drivers</th></tr></thead>
          <tbody>
            {drivers.top_free_zones.map((r) => (
              <tr key={r.zone}><td>{r.zone}</td><td className="num">{r.free}</td></tr>
            ))}
            {drivers.top_free_zones.length === 0 && <tr><td colSpan={2} className="muted">No free drivers right now.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}
