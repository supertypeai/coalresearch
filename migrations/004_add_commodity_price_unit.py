"""Peewee migrations -- 004_add_commodity_price_unit.py.

Generalizes `commodity_price` so each row says what its price is per.

`price_usd_per_ton` is misnamed: gold and silver are quoted in USD per troy
ounce, and iron, manganese, titanium and chromium are quoted per dry metric
tonne unit. It is kept unchanged (same raw values) so existing clients don't
break, and is deprecated in favour of:

    price       REAL  the same number under an honest name
    currency    TEXT  e.g. 'USD'
    price_unit  TEXT  what one price is per: 't', 'dmt', 'dmtu', 'troy_oz', ...
    source      TEXT  'LBMA' or 'Minerba'

LBMA rows (Gold, Silver) are fully backfilled here. Minerba rows get
`price_unit` from the commodity label on the next scrape
(`tables/commodity_price.py`).
"""


def migrate(migrator, database, *, fake=False):
    for col in ("price REAL", "currency TEXT", "price_unit TEXT", "source TEXT"):
        database.execute_sql(f"ALTER TABLE commodity_price ADD COLUMN {col}")
    database.execute_sql(
        "UPDATE commodity_price SET price = price_usd_per_ton, currency = 'USD'"
    )
    database.execute_sql(
        """
        UPDATE commodity_price
        SET source = CASE WHEN name IN ('Gold', 'Silver') THEN 'LBMA' ELSE 'Minerba' END,
            price_unit = CASE WHEN name IN ('Gold', 'Silver') THEN 'troy_oz' END
        """
    )


def rollback(migrator, database, *, fake=False):
    for col in ("source", "price_unit", "currency", "price"):
        database.execute_sql(f"ALTER TABLE commodity_price DROP COLUMN {col}")
