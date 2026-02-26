"""Peewee migrations -- 001_auto_20260226_222932.py.

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
    
    @migrator.create_model
    class CommodityPrice(pw.Model):
        name = pw.TextField()
        price_usd_per_ton = pw.FloatField()
        date = pw.DateField()

        class Meta:
            table_name = "commodity_price"
            primary_key = pw.CompositeKey('name', 'date')

    @migrator.create_model
    class Company(pw.Model):
        id = pw.IntegerField(primary_key=True)
        name = pw.TextField()
        slug = pw.TextField(unique=True)
        idx_ticker = pw.TextField(null=True)
        operation_province = pw.TextField(null=True)
        operation_district = pw.TextField(null=True)
        representative_address = pw.TextField(null=True)
        company_type = pw.TextField(null=True)
        key_operation = pw.TextField()
        activities = pw.TextField(null=True)
        website = pw.TextField(null=True)
        phone_number = pw.TextField(null=True)
        email = pw.TextField(null=True)
        mining_license = pw.TextField(null=True)
        mining_contract = pw.TextField(null=True)
        commodity_type = pw.TextField(null=True)

        class Meta:
            table_name = "company"

    @migrator.create_model
    class CompanyFinancials(pw.Model):
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        idx_ticker = pw.TextField()
        name = pw.TextField()
        year = pw.IntegerField()
        assets_usd = pw.FloatField()
        revenue_usd = pw.FloatField()
        revenue_breakdown = pw.TextField()
        cost_of_revenue_usd = pw.FloatField()
        cost_of_revenue_breakdown = pw.TextField()
        net_profit_usd = pw.FloatField()
        slug = pw.TextField()

        class Meta:
            table_name = "company_financials"
            primary_key = pw.CompositeKey('idx_ticker', 'year')

    @migrator.create_model
    class CompanyOwnership(pw.Model):
        parent_company_id = pw.ForeignKeyField(column_name='parent_company_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        percentage_ownership = pw.FloatField()

        class Meta:
            table_name = "company_ownership"
            primary_key = pw.CompositeKey('parent_company_id', 'company_id')

    @migrator.create_model
    class CompanyPerformance(pw.Model):
        id = pw.AutoField()
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        year = pw.IntegerField()
        commodity_type = pw.TextField()
        commodity_sub_type = pw.TextField(null=True)
        commodity_stats = pw.TextField()
        slug = pw.TextField()

        class Meta:
            table_name = "company_performance"

    @migrator.create_model
    class ExportDestination(pw.Model):
        id = pw.AutoField()
        country = pw.TextField()
        year = pw.IntegerField()
        commodity_type = pw.TextField()
        export_usd = pw.FloatField(null=True)
        export_volume_bps = pw.FloatField(null=True)
        export_volume_esdm = pw.FloatField(null=True)
        volume_unit = pw.TextField(null=True)

        class Meta:
            table_name = "export_destination"

    @migrator.create_model
    class GlobalCommodityData(pw.Model):
        id = pw.AutoField()
        country = pw.TextField()
        resources_reserves = pw.TextField(null=True)
        resources_reserves_unit = pw.TextField(null=True)
        resources_reserves_share = pw.TextField(null=True)
        export_import_usd = pw.TextField(null=True)
        production_volume = pw.TextField(null=True)
        production_volume_unit = pw.TextField(null=True)
        production_share = pw.TextField(null=True)
        commodity_type = pw.TextField()

        class Meta:
            table_name = "global_commodity_data"

    @migrator.create_model
    class MiningContract(pw.Model):
        id = pw.AutoField()
        mine_owner_id = pw.ForeignKeyField(column_name='mine_owner_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        contractor_id = pw.ForeignKeyField(column_name='contractor_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        contract_period_end = pw.DateField(null=True)

        class Meta:
            table_name = "mining_contract"

    @migrator.create_model
    class MiningLicense(pw.Model):
        id = pw.AutoField()
        license_type = pw.TextField()
        license_number = pw.TextField()
        wiup_code = pw.TextField()
        province = pw.TextField()
        city = pw.TextField()
        license_effective_date = pw.DateField()
        license_expiry_date = pw.DateField()
        activity = pw.TextField()
        licensed_area_ha = pw.FloatField()
        cnc = pw.TextField(null=True)
        generation = pw.TextField(null=True)
        location = pw.TextField()
        commodity_type = pw.TextField()
        company_name = pw.TextField()
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], null=True, on_delete='NO ACTION', on_update='NO ACTION')
        geometry = pw.TextField(null=True)

        class Meta:
            table_name = "mining_license"

    @migrator.create_model
    class MiningLicenseAuction(pw.Model):
        id = pw.AutoField()
        commodity_type = pw.TextField()
        city = pw.TextField()
        province = pw.TextField()
        company_name = pw.TextField()
        winner_date = pw.TextField()
        licensed_area_ha = pw.FloatField()
        license_number = pw.TextField(unique=True)
        area_type = pw.TextField()
        kdi = pw.TextField()
        wiup_code = pw.TextField()
        auction_status = pw.TextField()
        created_at = pw.TextField()
        last_modified = pw.TextField()
        participant_count = pw.IntegerField()
        phases = pw.TextField()
        participants = pw.TextField()
        winner = pw.TextField()
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], null=True, on_delete='NO ACTION', on_update='NO ACTION')

        class Meta:
            table_name = "mining_license_auctions"

    @migrator.create_model
    class MiningNews(pw.Model):
        id = pw.AutoField()
        title = pw.TextField()
        body = pw.TextField()
        source = pw.TextField()
        timestamp = pw.DateTimeField()
        commodity_type = pw.TextField()
        created_at = pw.DateTimeField()

        class Meta:
            table_name = "mining_news"

    @migrator.create_model
    class MiningSite(pw.Model):
        id = pw.AutoField()
        name = pw.TextField()
        project_name = pw.TextField(null=True)
        year = pw.IntegerField()
        commodity_type = pw.TextField()
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], on_delete='NO ACTION', on_update='NO ACTION')
        unit = pw.TextField()
        production_volume = pw.FloatField(null=True)
        overburden_removal_volume = pw.FloatField(null=True)
        strip_ratio = pw.FloatField(null=True)
        resources_reserves = pw.TextField()
        location = pw.TextField()

        class Meta:
            table_name = "mining_site"

    @migrator.create_model
    class ResourcesAndReserves(pw.Model):
        id = pw.AutoField()
        province = pw.TextField()
        year = pw.IntegerField()
        commodity_type = pw.TextField()
        resources_reserves = pw.TextField()

        class Meta:
            table_name = "resources_and_reserves"

    @migrator.create_model
    class SalesDestination(pw.Model):
        id = pw.AutoField()
        company_id = pw.ForeignKeyField(column_name='company_id', field='id', model=migrator.orm['company'], null=True, on_delete='NO ACTION', on_update='NO ACTION')
        country = pw.TextField()
        idx_ticker = pw.TextField()
        year = pw.IntegerField()
        revenue_usd = pw.FloatField(null=True)
        percentage_of_total_revenue = pw.FloatField(null=True)
        volume = pw.FloatField(null=True)
        percentage_of_sales_volume = pw.FloatField(null=True)
        commodity_type = pw.TextField()
        unit = pw.TextField()

        class Meta:
            table_name = "sales_destination"

    @migrator.create_model
    class TotalCommoditiesProduction(pw.Model):
        id = pw.AutoField()
        commodity_type = pw.TextField()
        production_volume = pw.FloatField()
        unit = pw.TextField()
        year = pw.IntegerField()

        class Meta:
            table_name = "total_commodities_production"


def rollback(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your rollback migrations here."""
    
    migrator.remove_model('total_commodities_production')

    migrator.remove_model('sales_destination')

    migrator.remove_model('resources_and_reserves')

    migrator.remove_model('mining_site')

    migrator.remove_model('mining_news')

    migrator.remove_model('mining_license_auctions')

    migrator.remove_model('mining_license')

    migrator.remove_model('mining_contract')

    migrator.remove_model('global_commodity_data')

    migrator.remove_model('export_destination')

    migrator.remove_model('company_performance')

    migrator.remove_model('company_ownership')

    migrator.remove_model('company_financials')

    migrator.remove_model('company')

    migrator.remove_model('commodity_price')
