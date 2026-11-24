"""Peewee migrations -- 005_normalize_names.py.

One name per thing across the mining tables.

export_destination.country: BPS spells the same country differently between
releases and commodities, which splits a country's series in two (Korea's coal
exports are "Republic Of Korea" for 2013-2023 and "Korea. Republic Of" for 2024).
The sheet keeps BPS's spelling; `tables/export_destination.py` maps it on sync
with the same table as below.

commodity_price.name: the Minerba scraper stored "Aluminum" while company,
mining_site and the other tables use "Aluminium" (the spelling in their CHECK
constraints). `tables/commodity_price.py` now keeps "Aluminium".

commodity_price is synced to Turso only for the last 60 days, so the renamed
price rows need a one-off full sync:
    python turso/sync_v2.py --replace --specific commodity_price
"""

COUNTRY_NAMES = {
    "Hongkong": "Hong Kong",
    "Korea. Republic Of": "South Korea",
    "Republic Of Korea": "South Korea",
    "Chinese Taipei": "Taiwan",
    "Germany. Fed. Rep. Of": "Germany",
}


def migrate(migrator, database, *, fake=False):
    for old, new in COUNTRY_NAMES.items():
        database.execute_sql(
            "UPDATE export_destination SET country = ? WHERE country = ?", (new, old)
        )
    database.execute_sql(
        "UPDATE commodity_price SET name = 'Aluminium' WHERE name = 'Aluminum'"
    )


def rollback(migrator, database, *, fake=False):
    # Country names are not restored: several BPS spellings map to one name.
    # Re-syncing the sheet without the mapping brings them back.
    database.execute_sql(
        "UPDATE commodity_price SET name = 'Aluminum' WHERE name = 'Aluminium'"
    )
