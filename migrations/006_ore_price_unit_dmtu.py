"""Peewee migrations -- 006_ore_price_unit_dmtu.py.

Iron, manganese, chromium and titanium got the placeholder price_unit 't'
because Minerba was unreachable when 004 added the column, so their real unit
was never scraped. Minerba quotes these ores per dry metric tonne unit (iron
1.50 USD/dmtu ≈ 93 USD/t at 62% Fe), so 't' is wrong. Only placeholder rows are
changed; a unit scraped from Minerba is left alone.

`tables/commodity_price.py` uses the same placeholder (FALLBACK_PRICE_UNITS).

commodity_price is synced to Turso only for the last 60 days, and these series
end on 15 Feb 2026, so run the same UPDATE on Turso directly.
"""

ORE_NAMES = ("Iron", "Manganese", "Chromium", "Titanium")


def migrate(migrator, database, *, fake=False):
    database.execute_sql(
        f"UPDATE commodity_price SET price_unit = 'dmtu' "
        f"WHERE price_unit = 't' AND name IN ({', '.join('?' * len(ORE_NAMES))})",
        ORE_NAMES,
    )


def rollback(migrator, database, *, fake=False):
    database.execute_sql(
        f"UPDATE commodity_price SET price_unit = 't' "
        f"WHERE price_unit = 'dmtu' AND name IN ({', '.join('?' * len(ORE_NAMES))})",
        ORE_NAMES,
    )
