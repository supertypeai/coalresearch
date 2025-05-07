import requests
import pandas as pd

# 1) Start a session so cookies persist across requests
session = requests.Session()

# 2) Spoof a realistic User‑Agent
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# 3) Hit the page so it sets its cookies (including CSRF)
home_url = "https://www.minerba.esdm.go.id/harga_acuan"
resp = session.get(home_url)
resp.raise_for_status()

# 4) Extract the CSRF token from the cookies
csrf_token = session.cookies.get("csrf_cookie_name")
if not csrf_token:
    raise RuntimeError("CSRF cookie not found!")

# 5) Prepare your form data (adjust the date range as needed)
data = {
    "csrf_test_name": csrf_token,
    "bulan_awal": "01/2024",
    "bulan_akhir": "05/2025",
}

# 6) POST back to the same endpoint to get the table
post_resp = session.post(
    home_url,
    data=data,
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": home_url,
    },
)
post_resp.raise_for_status()

# 7) Parse the returned HTML into a DataFrame
#    pandas.read_html will find all <table> elements;
#    here we grab the first one.
dfs = pd.read_html(post_resp.text)
df = dfs[0]

# 8) Inspect a sample
print(df.head())
