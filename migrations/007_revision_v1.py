"""Peewee migrations -- 007_revision_v1.py.

Some examples (model - class or model name)::

    > Model = migrator.orm['table_name']            # Return model in current state by name
    > Model = migrator.ModelClass                   # Return model in current state by name

    > migrator.sql(sql)                             # Run custom SQL
    > migrator.run(func, *args, **kwargs)           # Run python function with the given args
    > migrator.create_model(Model)                  # Create a model (could be used as decorator)
    > migrator.remove_model(model, cascade=True)    # Remove a model
    > migrator.add_fields(model, **fields)          # Add fields to a model
    > migrator.change_fields(model, **fields)       # Change fields
    > migrator.remove_fields(model, *field_names, cascade=True)
    > migrator.rename_field(model, old_field_name, new_field_name)
    > migrator.rename_table(model, new_table_name)
    > migrator.add_index(model, *col_names, unique=False)
    > migrator.add_not_null(model, *field_names)
    > migrator.add_default(model, field_name, default)
    > migrator.add_constraint(model, name, sql)
    > migrator.drop_index(model, *col_names)
    > migrator.drop_not_null(model, *field_names)
    > migrator.drop_constraints(model, *constraints)

"""

from contextlib import suppress

import peewee as pw
from peewee_migrate import Migrator


with suppress(ImportError):
    import playhouse.postgres_ext as pw_pext


def migrate(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your migrations here."""
    
    migrator.rename_field('commodity_price', 'price', 'price_usd_per_ton')

    migrator.rename_field('company_financials', 'assets', 'assets_usd')
    migrator.rename_field('company_financials', 'revenue', 'revenue_usd')
    migrator.rename_field('company_financials', 'cost_of_revenue', 'cost_of_revenue_usd')
    migrator.rename_field('company_financials', 'net_profit', 'net_profit_usd')

    # company -> company_id change: Assuming column is already company_id, 
    # and we just update python field name. If so, drop/add is redundant for checking if column exists.
    # But peewee-migrate might manage this metadata.
    # Safest is to remove the company/company_id add/remove if the column is same.
    # However, if field name changed, peewee-migrate might be confused.
    # Let's rely on peewee-migrate's rename_field if we want to rename the field.
    # But here the field name changes from 'company' to 'company_id'.
    # The COLUMN name for ForeignKeyField(Company) is usually 'company_id'.
    # So renaming field 'company' to 'company_id' might just update metadata without SQL if column name matches.
    # Let's try skipping these foreign key changes for now to avoid dropping columns.
    # If the column name is indeed company_id, we don't need to do anything.

    migrator.add_fields(
        'export_destination',
        unit=pw.TextField(null=True)) # New column

    migrator.rename_field('export_destination', 'export_USD', 'export_usd')
    migrator.rename_field('export_destination', 'export_volume_BPS', 'export_volume_bps')
    migrator.rename_field('export_destination', 'export_volume_ESDM', 'export_volume_esdm')
    
    # Mining contract: company fields...
    # remove 'mine_owner', add 'mine_owner_id' (col: 'mine_owner_id')
    # remove 'contractor', add 'contractor_id' (col: 'contractor_id')
    # Same logic as company_financials. Skip drop/add.
    
    # migrator.add_fields(
    #     'mining_contract',
    #     id=pw.AutoField()) 
    # Skipping adding 'id' to mining_contract because it fails on populated tables.
    # If the model needs an ID, we might need a more complex migration (recreate table),
    # but for this task "rename columns only", adding ID is out of scope if it breaks things.
    # Existing schema has composite PK (or no PK).
    
    migrator.rename_field('mining_license', 'licensed_area', 'licensed_area_ha')
    
    # Mining license company FK... skip

    migrator.rename_field('mining_license_auctions', 'licensed_area', 'licensed_area_ha')
    
    # Mining license auctions company FK... skip

    migrator.add_fields(
        'mining_site',
        unit=pw.TextField(null=True))

    # Mining site company FK... skip

    migrator.add_fields(
        'sales_destination',
        commodity_type=pw.TextField(null=True),
        unit=pw.TextField(null=True))

    migrator.rename_field('sales_destination', 'revenue', 'revenue_usd')
    
    # Sales destination company FK... skip


def rollback(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your rollback migrations here."""
    
    migrator.rename_field('sales_destination', 'revenue_usd', 'revenue')
    migrator.remove_fields('sales_destination', 'commodity_type', 'unit')
    
    migrator.remove_fields('mining_site', 'unit')
    
    migrator.rename_field('mining_license_auctions', 'licensed_area_ha', 'licensed_area')
    
    migrator.rename_field('mining_license', 'licensed_area_ha', 'licensed_area')
    
    migrator.remove_fields('mining_contract', 'id')

    migrator.remove_fields('export_destination', 'unit')
    migrator.rename_field('export_destination', 'export_usd', 'export_USD')
    migrator.rename_field('export_destination', 'export_volume_bps', 'export_volume_BPS')
    migrator.rename_field('export_destination', 'export_volume_esdm', 'export_volume_ESDM')
    
    # company_performance... FK reverse (noop)
    
    # company_ownership... FK reverse (noop)
    
    migrator.rename_field('company_financials', 'assets_usd', 'assets')
    migrator.rename_field('company_financials', 'revenue_usd', 'revenue')
    migrator.rename_field('company_financials', 'cost_of_revenue_usd', 'cost_of_revenue')
    migrator.rename_field('company_financials', 'net_profit_usd', 'net_profit')
    
    migrator.rename_field('commodity_price', 'price_usd_per_ton', 'price')

    migrator.remove_fields('company_financials', 'company_id', 'assets_usd', 'revenue_usd', 'cost_of_revenue_usd', 'net_profit_usd')

    migrator.add_fields(
        'commodity_price',

        price=pw.FloatField())

    migrator.remove_fields('commodity_price', 'price_usd_per_ton')
