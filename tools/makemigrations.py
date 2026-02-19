"""
Example usage:
    python makemigrations.py
    python makemigrations.py my_migration_name
"""

import sys
import datetime
from peewee_migrate import Router
import peewee
import db.models as models

# Use the database defined in models
db = models.db

# Ensure connection
db.connect(reuse_if_open=True)

# Extract all Peewee models defined globally in db.models
all_models = [
    model for model in vars(models).values()
    if isinstance(model, type) and issubclass(model, peewee.Model) and model != peewee.Model
    and getattr(model, '_meta', None) is not None
]

router = Router(db, migrate_dir='migrations')

# Set a dynamic migration name if none is provided as a command-line argument
if len(sys.argv) > 1:
    migration_name = sys.argv[1]
else:
    # Use a descriptive timestamp to avoid conflicts
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    migration_name = f"auto_{timestamp}"

# Create a migration only if needed
migration = router.create(migration_name, auto=all_models)
if migration:
    print(f"Migration file created for '{migration_name}': {migration}.py")
else:
    print("No changes detected in Peewee models. Database is up to date.")

db.close()
