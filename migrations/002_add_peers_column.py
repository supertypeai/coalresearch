"""Peewee migrations -- 002_add_peers_column.py.

Adds the `peers` JSON column to the `company` table.
"""


def migrate(migrator, database, *, fake=False):
    """Add the peers column."""
    database.execute_sql(
        "ALTER TABLE company ADD COLUMN peers TEXT CHECK(json_valid(peers))"
    )


def rollback(migrator, database, *, fake=False):
    """Remove the peers column."""
    database.execute_sql("ALTER TABLE company DROP COLUMN peers")
