import sqlite3

from models import (
    db,
    Company,
)

db_dir = 'db.sqlite'

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


def alterCompany():
    con = sqlite3.connect(db_dir)
    cur = con.cursor()

    # 1.
    cur.execute("PRAGMA foreign_keys = OFF;")
    # 2.
    cur.execute("ALTER TABLE company RENAME TO company_old;") 

    # 3. Use peewee to create new company table
    db.connect()
    db.create_tables([Company])

    # 4. Copy data from old to new
    fields = [field.column_name for field in Company._meta.sorted_fields]
    columns_str = ", ".join(fields)

    cur.execute(f"""
        INSERT INTO company ({columns_str})
        SELECT {columns_str}
        FROM company_old;
    """)

    # 5.
    cur.execute("DROP TABLE company_old;")

    # 6.
    cur.execute("PRAGMA foreign_keys = ON;")

    # 7.
    con.commit()
    con.close()    

    # 8.
    logCreationScript("company")

if __name__ == "__main__":
    alterCompany()
    print("Alteration done.")