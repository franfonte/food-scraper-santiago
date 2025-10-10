import math

def generate_zones(R_km=4.0):
    min_lat, max_lat = -33.70, -33.30
    min_lon, max_lon = -70.85, -70.45
    lat_center = -33.45

    s_km = R_km * math.sqrt(2)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * math.cos(math.radians(abs(lat_center)))

    dlat = s_km / km_per_deg_lat
    dlon = s_km / km_per_deg_lon

    zones = []
    lat = min_lat + dlat / 2
    while lat <= max_lat:
        lon = min_lon + dlon / 2
        while lon <= max_lon:
            zones.append({
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "radius_m": int(R_km * 1000)
            })
            lon += dlon
        lat += dlat
    return zones
