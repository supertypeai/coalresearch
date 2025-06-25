import requests
import pandas as pd
import sqlite3
import json

# URLs for LBMA gold AM and silver daily prices
URLS = {
    "Emas": "https://prices.lbma.org.uk/json/gold_am.json",
    "Perak": "https://prices.lbma.org.uk/json/silver.json",
}


def fetch_price_data(url: str) -> pd.DataFrame:
    """
    Fetch daily price JSON from LBMA and return DataFrame with columns:
    date, high
    """
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Normalize JSON into DataFrame
    df = pd.json_normalize(data)
    # Extract high from 'v' list [high, low, avg]
    df["high"] = df["v"].apply(lambda x: x[0])
    df.drop(columns=["v", "is_cms_locked"], inplace=True)

    # Convert date column
    df["date"] = pd.to_datetime(df["d"], format="%Y-%m-%d")
    df.drop(columns=["d"], inplace=True)

    # Exclude invalid days
    df = df[df["high"] > 0]

    return df


def compute_monthly_high(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given daily DataFrame with date, high, compute for each month:
      - monthly_high: max daily high
    """
    df.set_index("date", inplace=True)
    monthly = df["high"].resample("M").max().reset_index()
    monthly["month"] = monthly["date"].dt.to_period("M").astype(str)
    return monthly[["month", "high"]].rename(columns={"high": "monthly_high"})


def upsert_to_db(data: dict, db_path: str = "db.sqlite"):
    """
    Upsert monthly high data for each commodity into SQLite table 'commodity'
    data: {'name': DataFrame(month, monthly_high)}
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS commodity (
            commodity_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            price TEXT
        )
    """
    )

    for name, df in data.items():
        # Build JSON list of {month: value}
        price_list = [
            {row["month"]: f"{row['monthly_high']}"} for _, row in df.iterrows()
        ]
        price_json = json.dumps(price_list)

        # Upsert using SQLite ON CONFLICT
        cur.execute(
            """
            INSERT INTO commodity (name, price)
            VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET price=excluded.price
            """,
            (name, price_json),
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    all_data = {}
    for name, url in URLS.items():
        df = fetch_price_data(url)
        monthly_high = compute_monthly_high(df)
        all_data[name] = monthly_high
        print(f"Monthly high for {name}:")
        print(monthly_high)

    # Upsert into SQLite database
    upsert_to_db(all_data)
    print("Data upserted into db.sqlite (table 'commodity').")
