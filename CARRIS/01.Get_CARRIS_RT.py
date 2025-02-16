import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

def fetch_realtime_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

def parse_data_to_gdf(data):
    entities = data.get("entity", [])
    records = []

    for entity in entities:
        vehicle_data = entity.get("vehicle", {})
        trip_data = vehicle_data.get("trip", {})
        position_data = vehicle_data.get("position", {})  # Correctly accessing position
        vehicle_info = vehicle_data.get("vehicle", {})

        # Ensure values exist
        latitude = position_data.get("latitude")
        longitude = position_data.get("longitude")

        record = {
            "id": entity.get("id", ""),
            "trip_id": trip_data.get("tripId", ""),
            "route_id": trip_data.get("routeId", ""),
            "direction_id": trip_data.get("directionId", ""),
            "latitude": latitude,
            "longitude": longitude,
            "current_stop_sequence": vehicle_data.get("currentStopSequence"),  # FIX: Now correctly from vehicle
            "current_status": vehicle_data.get("currentStatus"),  # FIX: Now correctly from vehicle
            "timestamp": vehicle_data.get("timestamp"),  # FIX: Now correctly from vehicle
            "stop_id": vehicle_data.get("stopId"),  # FIX: Now correctly from vehicle
            "vehicle_id": vehicle_info.get("id", ""),
            "license_plate": vehicle_info.get("licensePlate", ""),
            "geometry": Point(longitude, latitude) if latitude and longitude else None
        }
        records.append(record)

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return gdf

if __name__ == "__main__":
    url = "https://rt.jdcp.workers.dev/"
    data = fetch_realtime_data(url)
    gdf = parse_data_to_gdf(data)
    
    
gdf["timestamp"] = pd.to_datetime(gdf["timestamp"], unit="s")



# Exportar
from datetime import datetime
gdf.to_csv(datetime.now().strftime('data_sources/data_transformed/carris_gtfsrt-%Y-%m-%d-%H-%M-%S.csv'), encoding='utf8', index=False)