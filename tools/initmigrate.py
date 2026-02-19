from peewee_migrate import Router
from db import models

models.db.connect(reuse_if_open=True)
router = Router(models.db)
router.run(fake=True)
models.db.close()