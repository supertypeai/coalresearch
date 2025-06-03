import json
from db.models import MiningSite

q = MiningSite.select()

data = []
for row in q:
    location_json = json.loads(row.location)
    location_json['id'] = row.id
    location_json['name'] = row.name
    location_json['company'] = row.company_id.name

    data.append(location_json)

# data = [
#     {"id": 38, "province": "Kalimantan Tengah", "city": None, "latitude": None, "longitude": None},
#     {"id": 39, "province": "Kalimantan Timur", "city": "Kutai Kartanegara", "latitude": 0.534906, "longitude": 116.107206},
#     # ... add the rest
# ]

features = []

for entry in data:

    if entry["latitude"] is not None and entry["longitude"] is not None:
        feature = {
            "type": "Feature",
            "properties": {
                "id": entry["id"],
                "name": entry["name"],
                "company": entry["company"],
                "province": entry["province"],
                "city": entry["city"],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [entry["longitude"], entry["latitude"]],
            }
        }
        features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open("listed_mining_sites.geojson", "w") as f:
    json.dump(geojson, f, indent=2)
