from peewee import (
    Model,
    SqliteDatabase,
    TextField,
    IntegerField,
    DecimalField,
    ForeignKeyField,
    Check,
    CompositeKey,
)

province_constraints = (
    "Aceh",
    "Bali",
    "Kepulauan Bangka Belitung",
    "Banten",
    "Bengkulu",
    "Gorontalo",
    "Papua Barat",
    "Jakarta",
    "Jambi",
    "Jawa Barat",
    "Jawa Tengah",
    "Jawa Timur",
    "Kalimantan Barat",
    "Kalimantan Selatan",
    "Kalimantan Tengah",
    "Kalimantan Timur",
    "Kalimantan Utara",
    "Kepulauan Riau",
    "Lampung",
    "Maluku Utara",
    "Maluku",
    "Nusa Tenggara Barat",
    "Nusa Tenggara Timur",
    "Papua",
    "Riau",
    "Sulawesi Barat",
    "Sulawesi Selatan",
    "Sulawesi Tengah",
    "Sulawesi Tenggara",
    "Sulawesi Utara",
    "Sumatera Barat",
    "Sumatera Selatan",
    "Sumatera Utara",
    "Yogyakarta",
)
company_type_constraints = (
    "Holding",
    "Mine Owner",
    "Consultant",
    "Contractor",
    "Trader",
)
key_operation_constraints = (
    "Mining",
    "Mining Services",
    "Equipment Rental",
    "Logistic Management",
    "Overburden Removal & Hauling",
    "Dredging",
    "Coal Trading",
    "Barging, Port & Transshipment",
    "Investment",
)
commodity_type_constraints = (
    "Coal",
    "Aluminium",
    "Gold",
    "Zinc and Lead",
    "Oil",
    "Nickel",
    "Copper",
    "Silver",
)
mineral_type_constraints = (
    "Coal",
    "Aluminium",
    "Gold",
    "Zinc and Lead",
    "Oil",
    "Nickel",
)
mining_operation_status_constraints = ("production", "development", "inactive")

# %%
db = SqliteDatabase("db.sqlite")


# %%
class Company(Model):
    id = IntegerField(primary_key=True)
    name = TextField()
    idx_ticker = TextField(null=True)
    operation_province = TextField(
        null=True,
        constraints=[Check(f"operation_province IN {province_constraints}")],
    )
    operation_kabkot = TextField(null=True)
    representative_address = TextField(null=True)
    company_type = TextField(
        null=True, constraints=[Check(f"company_type IN {company_type_constraints}")]
    )
    key_operation = TextField(
        constraints=[Check(f"key_operation IN {key_operation_constraints}")]
    )
    activities = TextField(null=True, constraints=[Check("json_valid(activities)")])
    website = TextField(null=True)
    phone_number = IntegerField(null=True)
    email = TextField(null=True)
    mining_license = TextField(
        null=True, constraints=[Check("json_valid(mining_license)")]
    )

    class Meta:
        database = db
        table_name = "company"


class GlobalCommodityData(Model):
    id = IntegerField(primary_key=True)
    country = TextField()
    resources_reserves = TextField(
        null=True, constraints=[Check("json_valid(resources_reserves)")]
    )
    export_import = TextField(
        null=True, constraints=[Check("json_valid(export_import)")]
    )
    production_volume = TextField(
        null=True, constraints=[Check("json_valid(production_volume)")]
    )
    commodity_type = TextField(
        null=True,
        constraints=[Check(f"commodity_type IN {commodity_type_constraints}")],
    )

    class Meta:
        database = db
        table_name = "global_commodity_data"


class CompanyOwnership(Model):
    parent_company_id = ForeignKeyField(Company, column_name="parent_company_id")
    company_id = ForeignKeyField(Company, column_name="company_id")
    percentage_ownership = DecimalField()

    class Meta:
        database = db
        table_name = "company_ownership"
        primary_key = CompositeKey("parent_company_id", "company_id")


class CompanyPerformance(Model):
    id = IntegerField(primary_key=True)
    company_id = ForeignKeyField(Company, column_name="company_id")
    year = IntegerField()
    commodity_type = TextField(
        null=True,
        constraints=[Check(f"commodity_type IN {commodity_type_constraints}")],
    )
    commodity_sub_type = TextField(null=True)
    commodity_stats = TextField(constraints=[Check("json_valid(commodity_stats)")])

    class Meta:
        database = db
        table_name = "company_performance"


class MiningSite(Model):
    id = IntegerField(primary_key=True)
    name = TextField()
    project_name = TextField(null=True)
    year = IntegerField()
    mineral_type = TextField(
        null=True, constraints=[Check(f"mineral_type IN {mineral_type_constraints}")]
    )
    company_id = ForeignKeyField(Company, column_name="company_id")
    production_volume = DecimalField(null=True)
    overburden_removal_volume = DecimalField(null=True)
    strip_ratio = DecimalField(null=True)
    resources_reserves = TextField(
        null=True, constraints=[Check("json_valid(resources_reserves)")]
    )
    location = TextField(null=True, constraints=[Check("json_valid(location)")])

    class Meta:
        database = db
        table_name = "mining_site"


class ResourcesAndReserves(Model):
    id = IntegerField(primary_key=True)
    province = TextField(constraints=[Check(f"province IN {province_constraints}")])
    year = IntegerField()
    commodity_type = TextField(
        null=True,
        constraints=[Check(f"commodity_type IN {commodity_type_constraints}")],
    )
    exploration_target_1 = DecimalField(null=True)
    total_inventory_1 = DecimalField(null=True)
    resources_inferred = DecimalField(null=True)
    resources_indicated = DecimalField(null=True)
    resources_measured = DecimalField(null=True)
    resources_total = DecimalField(null=True)
    verified_resources_2 = DecimalField(null=True)
    reserves_1 = DecimalField(null=True)
    verified_reserves_2 = DecimalField(null=True)

    class Meta:
        database = db
        table_name = "resources_and_reserves"


class TotalCommoditiesProduction(Model):
    id = IntegerField(primary_key=True)
    commodity_type = TextField(
        null=True,
        constraints=[Check(f"commodity_type IN {commodity_type_constraints}")],
    )
    production_volume = DecimalField(null=True)
    unit = TextField(null=True)
    year = IntegerField()

    class Meta:
        database = db
        table_name = "total_commodities_production"


class ExportDestination(Model):
    id = IntegerField(primary_key=True)
    country = TextField()
    year = IntegerField()
    commodity_type = TextField(
        null=True,
        constraints=[Check(f"commodity_type IN {commodity_type_constraints}")],
    )
    export_USD = DecimalField(null=True)
    export_volume_BPS = DecimalField(null=True)
    export_volume_ESDM = DecimalField(null=True)

    class Meta:
        database = db
        table_name = "export_destination"


class MiningContract(Model):
    mine_owner_id = ForeignKeyField(Company, column_name="mine_owner_id")
    contractor_id = ForeignKeyField(Company, column_name="contractor_id")
    contract_period_end = TextField(null=True)

    class Meta:
        database = db
        table_name = "mining_contract"
        primary_key = CompositeKey("mine_owner_id", "contractor_id")
