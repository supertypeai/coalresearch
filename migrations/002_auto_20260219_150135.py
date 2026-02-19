"""Peewee migrations -- 002_auto_20260219_150135.py.

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
    
    migrator.change_fields('commodity_price', price=pw.FloatField())

    migrator.change_fields('commodity_price', date=pw.DateField())

    migrator.add_fields(
        'company',

        commodity_type=pw.TextField(null=True))

    migrator.remove_fields('company', 'commodity')

    migrator.change_fields('company', phone_number=pw.TextField(null=True))

    migrator.change_fields('company_financials', assets=pw.FloatField())

    migrator.change_fields('company_financials', revenue=pw.FloatField())

    migrator.change_fields('company_financials', cost_of_revenue=pw.FloatField())

    migrator.change_fields('company_financials', net_profit=pw.FloatField())

    migrator.change_fields('company_ownership', percentage_ownership=pw.FloatField())

    migrator.change_fields('export_destination', export_USD=pw.FloatField(null=True))

    migrator.change_fields('export_destination', export_volume_BPS=pw.FloatField(null=True))

    migrator.change_fields('export_destination', export_volume_ESDM=pw.FloatField(null=True))

    migrator.change_fields('mining_license', licensed_area=pw.FloatField())

    migrator.change_fields('mining_license_auctions', city=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'city')

    migrator.change_fields('mining_license_auctions', province=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'province')

    migrator.change_fields('mining_license_auctions', company_name=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'company_name')

    migrator.change_fields('mining_license_auctions', date_winner=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'date_winner')

    migrator.change_fields('mining_license_auctions', permit_area=pw.FloatField())

    migrator.change_fields('mining_license_auctions', license_number=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'license_number')

    migrator.change_fields('mining_license_auctions', permit_type=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'permit_type')

    migrator.change_fields('mining_license_auctions', kdi=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'kdi')

    migrator.change_fields('mining_license_auctions', code_wiup=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'code_wiup')

    migrator.change_fields('mining_license_auctions', auction_status=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'auction_status')

    migrator.change_fields('mining_license_auctions', created_at=pw.TextField())

    migrator.change_fields('mining_license_auctions', last_modified=pw.TextField())

    migrator.change_fields('mining_license_auctions', participant_count=pw.IntegerField())

    migrator.add_not_null('mining_license_auctions', 'participant_count')

    migrator.change_fields('mining_license_auctions', phases=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'phases')

    migrator.change_fields('mining_license_auctions', participants=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'participants')

    migrator.change_fields('mining_license_auctions', winner=pw.TextField())

    migrator.add_not_null('mining_license_auctions', 'winner')

    migrator.change_fields('mining_site', production_volume=pw.FloatField(null=True))

    migrator.change_fields('mining_site', overburden_removal_volume=pw.FloatField(null=True))

    migrator.change_fields('mining_site', strip_ratio=pw.FloatField(null=True))

    migrator.change_fields('mining_site', resources_reserves=pw.TextField())

    migrator.add_not_null('mining_site', 'resources_reserves')

    migrator.change_fields('mining_site', location=pw.TextField())

    migrator.add_not_null('mining_site', 'location')

    migrator.change_fields('sales_destination', revenue=pw.FloatField(null=True))

    migrator.change_fields('sales_destination', percentage_of_total_revenue=pw.FloatField(null=True))

    migrator.change_fields('sales_destination', volume=pw.FloatField(null=True))

    migrator.change_fields('sales_destination', percentage_of_sales_volume=pw.FloatField(null=True))

    migrator.change_fields('total_commodities_production', production_volume=pw.FloatField())


def rollback(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your rollback migrations here."""
    
    migrator.change_fields('total_commodities_production', production_volume=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('sales_destination', revenue=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('sales_destination', percentage_of_total_revenue=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('sales_destination', volume=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('sales_destination', percentage_of_sales_volume=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('mining_site', production_volume=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('mining_site', overburden_removal_volume=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('mining_site', strip_ratio=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('mining_site', resources_reserves=pw.TextField(null=True))

    migrator.drop_not_null('mining_site', 'resources_reserves')

    migrator.change_fields('mining_site', location=pw.TextField(null=True))

    migrator.drop_not_null('mining_site', 'location')

    migrator.change_fields('mining_license_auctions', city=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'city')

    migrator.change_fields('mining_license_auctions', province=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'province')

    migrator.change_fields('mining_license_auctions', company_name=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'company_name')

    migrator.change_fields('mining_license_auctions', date_winner=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'date_winner')

    migrator.change_fields('mining_license_auctions', permit_area=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('mining_license_auctions', license_number=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'license_number')

    migrator.change_fields('mining_license_auctions', permit_type=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'permit_type')

    migrator.change_fields('mining_license_auctions', kdi=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'kdi')

    migrator.change_fields('mining_license_auctions', code_wiup=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'code_wiup')

    migrator.change_fields('mining_license_auctions', auction_status=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'auction_status')

    migrator.change_fields('mining_license_auctions', created_at=pw.DateTimeField(null=True))

    migrator.change_fields('mining_license_auctions', last_modified=pw.DateTimeField(null=True))

    migrator.change_fields('mining_license_auctions', participant_count=pw.IntegerField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'participant_count')

    migrator.change_fields('mining_license_auctions', phases=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'phases')

    migrator.change_fields('mining_license_auctions', participants=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'participants')

    migrator.change_fields('mining_license_auctions', winner=pw.TextField(null=True))

    migrator.drop_not_null('mining_license_auctions', 'winner')

    migrator.change_fields('mining_license', licensed_area=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('export_destination', export_USD=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('export_destination', export_volume_BPS=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('export_destination', export_volume_ESDM=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, null=True, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('company_ownership', percentage_ownership=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('company_financials', assets=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('company_financials', revenue=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('company_financials', cost_of_revenue=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('company_financials', net_profit=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.add_fields(
        'company',

        commodity=pw.TextField(null=True))

    migrator.remove_fields('company', 'commodity_type')

    migrator.change_fields('company', phone_number=pw.IntegerField(null=True))

    migrator.change_fields('commodity_price', price=pw.DecimalField(auto_round=False, decimal_places=5, max_digits=10, rounding=ROUND_HALF_EVEN))

    migrator.change_fields('commodity_price', date=pw.TextField())
