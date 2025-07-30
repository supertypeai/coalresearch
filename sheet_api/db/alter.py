import sqlite3

from sheet_api.db.models import (
    db,
    Company,
    MiningSite,
    TotalCommoditiesProduction,
    ResourcesAndReserves,
    ExportDestination,
    GlobalCommodityData,
)

db_dir = 'db.sqlite'
TBL_MAP = {
    'company': Company,
    'mining_site': MiningSite,
    'total_commodities_production': TotalCommoditiesProduction,
    'resources_and_reserves': ResourcesAndReserves,
    'export_destination': ExportDestination,
    'global_commodity_data': GlobalCommodityData
}

def logCreationScript(table_name: str):
    con = sqlite3.connect(db_dir)
    cur = con.cursor()

    # Get the raw CREATE TABLE statement
    cur.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    result = cur.fetchone()
    
    if result is None:
        print(f"Table '{table_name}' not found.")
        return

    create_sql = result[0]

    # Basic pretty formatting
    formatted_sql = (
        create_sql
        .replace("(", "(\n    ")
        .replace(", ", ",\n    ")
        .replace("CHECK", "\n    CHECK")  # improve CHECK visibility
        .replace(")", "\n)")
    )

    print(formatted_sql)

    con.close()


def alterTable(tbl_name: str):
    con = sqlite3.connect(db_dir)
    cur = con.cursor()

    # 1.
    cur.execute("PRAGMA foreign_keys = OFF;")
    
    # 2.
    cur.execute(f"ALTER TABLE {tbl_name} RENAME TO {tbl_name}_old;") 

    # 3. Define model
    Model = TBL_MAP[tbl_name]

    # 4. Use peewee to create new table    
    db.connect()
    db.create_tables([Model])

    # 5. Copy data from old to new
    fields = [field.column_name for field in Model._meta.sorted_fields]
    columns_str = ", ".join(fields)

    cur.execute(f"""
        INSERT INTO {tbl_name} ({columns_str})
        SELECT {columns_str}
        FROM {tbl_name}_old;
    """)

    # 6.
    cur.execute(f"DROP TABLE {tbl_name}_old;")

    # 7.
    cur.execute("PRAGMA foreign_keys = ON;")

    # 8.
    con.commit()
    con.close()    

    # 9.
    logCreationScript(tbl_name)
    print("Alteration done.")

def recreateTable(tbl_name: str):
    con = sqlite3.connect(db_dir)
    cur = con.cursor()

    # 1
    cur.execute("PRAGMA foreign_keys = OFF;")
    
    # 2
    cur.execute(f"DROP TABLE {tbl_name};")
    
    # 3
    Model = TBL_MAP[tbl_name]
    db.connect()
    db.create_tables([Model])

    # 4
    cur.execute("PRAGMA foreign_keys = ON;")
    con.commit()
    con.close()    

    logCreationScript(tbl_name)
    print(f"Table {tbl_name} recreated.")

if __name__ == "__main__":
    table = "global_commodity_data"
    # alterTable(table)
    recreateTable(table)
