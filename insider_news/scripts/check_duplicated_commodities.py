import json
from insider_news.processor import get_connection
from typing import Any

def cast_value(value: Any, target_type: Any) -> Any:
    if value is None:
        return None
    if target_type == "json":
        return json.loads(value) if isinstance(value, str) else value
    try:
        return target_type(value)
    except (TypeError, ValueError):
        return value

conn = get_connection()

cols = [
    {
        "name": "id",
        "type": int
    }, 
    {
        "name": "commodity_type",
        "type": "json"
    }
]
col_names = [c["name"] for c in cols]

query = f"SELECT {', '.join(col_names)} FROM mining_news;"

cur = conn.cursor()
records = cur.execute(query).fetchall()
records = [
    {
        c["name"]: cast_value(rec[i], c["type"])
        for i, c in enumerate(cols)
    }
    for rec in records
]
conn.close()

def has_duplicates(x: list):
    return len(x) != len(set(x))

duplicated_com_type = []
for rec in records:
    duplicated = has_duplicates(rec['commodity_type'])
    # print(rec['commodity_type'])
    if duplicated:
        duplicated_com_type.append(rec)

for d in duplicated_com_type:
    print(d)