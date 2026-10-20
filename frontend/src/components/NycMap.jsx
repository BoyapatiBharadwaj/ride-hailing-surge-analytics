import { MapContainer, TileLayer } from "react-leaflet";

// Leaflet map of New York with the free Carto light basemap (needs internet in the browser; the dots work without it).
export default function NycMap({ children, height = 430 }) {
  return (
    <div className="map" style={{ height }}>
      <MapContainer center={[40.715, -73.94]} zoom={10} minZoom={9} maxZoom={16} preferCanvas scrollWheelZoom
                    style={{ height: "100%", width: "100%" }}>
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
          subdomains="abcd"
        />
        {children}
      </MapContainer>
    </div>
  );
}
