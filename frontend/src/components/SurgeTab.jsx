import { CircleMarker, Tooltip } from "react-leaflet";
import { fmt } from "../api.js";
import NycMap from "./NycMap.jsx";
import { Empty, Mini } from "./TripsTab.jsx";

export const surgeColour = (m) => (m <= 1.0 ? "#9aa3b2" : m < 1.5 ? "#f59e0b" : m < 2.0 ? "#ea580c" : "#be123c");

export default function SurgeTab({ surge }) {
  if (!surge) return <Empty />;
  const zones = surge.zones.filter((z) => z.lat !== null && z.lon !== null);
  return (
    <>
      <div className="mini">
        <Mini label="Zones surging (> 1.0x)" value={fmt(surge.surging)} />
        <Mini label="Highest multiplier" value={surge.max !== null ? `${fmt(surge.max, 1)}x` : "–"} />
        <Mini label="Average multiplier" value={surge.avg !== null ? `${fmt(surge.avg, 2)}x` : "–"} />
        <Mini label="Weather now"
              value={surge.temp_c !== null ? `${fmt(surge.temp_c, 0)} °C · ${fmt(surge.rain_mm || 0, 1)} mm` : "–"} />
      </div>
      <NycMap>
        {[...zones].sort((a, b) => a.multiplier - b.multiplier).map((z) => (
          <CircleMarker key={z.zone_id} center={[z.lat, z.lon]} radius={z.multiplier <= 1 ? 3 : 4 + (z.multiplier - 1) * 9}
                        pathOptions={{ color: "#fff", weight: 1, fillColor: surgeColour(z.multiplier),
                                       fillOpacity: z.multiplier <= 1 ? 0.55 : 0.85 }}>
            <Tooltip>
              <b>{z.zone}</b><br />surge {fmt(z.multiplier, 1)}x · demand {z.demand ?? "–"} · supply {z.supply ?? "–"}
            </Tooltip>
          </CircleMarker>
        ))}
      </NycMap>
      <p className="src">
        Size and colour = multiplier ({["1.0x", "1.2x", "1.5x", "2.0x+"].map((l, i) => (
          <span key={l}><span className="legend" style={{ background: surgeColour([1, 1.2, 1.5, 2][i]) }} /> {l} </span>
        ))}) · source: Kafka surge_updates{surge.last_update_ny ? `, last update ${surge.last_update_ny} NYC` : ""}
      </p>
      <h3>Top surging zones</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr><th>Zone</th><th className="num">Surge</th><th className="num">Demand</th><th className="num">Supply</th><th className="num">Ratio</th></tr>
          </thead>
          <tbody>
            {surge.zones.slice(0, 15).map((z) => (
              <tr key={z.zone_id}>
                <td>{z.zone}</td>
                <td className="num"><span className="pill" style={{ background: surgeColour(z.multiplier), color: "#fff" }}>{fmt(z.multiplier, 1)}x</span></td>
                <td className="num">{z.demand ?? "–"}</td>
                <td className="num">{z.supply ?? "–"}</td>
                <td className="num">{z.ratio != null ? fmt(z.ratio, 2) : "–"}</td>
              </tr>
            ))}
            {surge.zones.length === 0 && <tr><td colSpan={5} className="muted">No surge updates in the last 150 seconds.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}
