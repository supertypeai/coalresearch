"""
Example usage:
    python migrate.py
"""

from peewee_migrate import Router
from db.models import db

# Ensure connection
db.connect(reuse_if_open=True)

router = Router(db, migrate_dir='migrations')
print("Checking for pending migrations...")
todo = router.todo
if todo:
    print(f"Applying {len(todo)} pending migration(s): {', '.join(todo)}")
    router.run()
    print("All migrations applied successfully.")
else:
    print("Database is up to date. No pending migrations.")

db.close()