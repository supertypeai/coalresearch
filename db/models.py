from peewee import (
    Model,
    SqliteDatabase,
    TextField,
    IntegerField,
    FloatField,
    ForeignKeyField,
    Check,
    CompositeKey,
    DateTimeField,
    DateField,
)

province_constraints = (
    "Aceh",
    "Bali",
    "Kepulauan Bangka Belitung",
    "Banten",
    "Bengkulu",
    "Gorontalo",
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
    "Papua Barat",
    "Papua Barat Daya",
    "Papua Pegunungan",
    "Papua Selatan",
    "Papua Tengah",
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
    "Manufacturer",
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
    "Mineral Refining",
    "Construction",
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
    "Tin",
    "Cobalt",
)
mineral_type_constraints = (
    "Coal",
    "Aluminium",
    "Gold",
    "Zinc and Lead",
    "Oil",
    "Nickel",
    "Copper",
    "Silver",
)
mining_operation_status_constraints = ("production", "development", "inactive")

db = SqliteDatabase("db.sqlite")


class Company(Model):
    id = IntegerField(primary_key=True)
    name = TextField()  # NOT NULL by default
    slug = TextField(unique=True)
    idx_ticker = TextField(null=True)
    operation_province = TextField(
        null=True,
        constraints=[Check(f"operation_province IN {province_constraints}")],
    )
    operation_district = TextField(null=True)
    representative_address = TextField(null=True)
    company_type = TextField(
        null=True, constraints=[Check(f"company_type IN {company_type_constraints}")]
    )
    key_operation = TextField(
        constraints=[Check(f"key_operation IN {key_operation_constraints}")]
    )
    activities = TextField(null=True, constraints=[Check("json_valid(activities)")])
    website = TextField(null=True)
    phone_number = TextField(null=True) # Changed from Integer to Text
    email = TextField(null=True)
    mining_license = TextField(
        null=True, constraints=[Check("json_valid(mining_license)")]
    )
    mining_contract = TextField(
        null=True, constraints=[Check("json_valid(mining_contract)")]
    )
    commodity_type = TextField(null=True, constraints=[Check("json_valid(commodity_type)")])

    class Meta:
        database = db
        table_name = "company"

class MiningContract(Model):
    mine_owner = ForeignKeyField(
        Company,
        backref="contracts_as_mine_owner",
        column_name="mine_owner_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    contractor = ForeignKeyField(
        Company,
        backref="contracts_as_contractor",
        column_name="contractor_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    contract_period_end = DateField(null=True)

    class Meta:
        database = db
        table_name = "mining_contract"
        primary_key = CompositeKey("mine_owner", "contractor")


class CompanyOwnership(Model):
    parent_company_id = ForeignKeyField(
        Company,
        backref="child_ownerships",
        column_name="parent_company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    company_id = ForeignKeyField(
        Company,
        backref="parent_ownerships",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    percentage_ownership = FloatField()

    class Meta:
        database = db
        table_name = "company_ownership"
        primary_key = CompositeKey("parent_company", "company")


class CompanyPerformance(Model):
    company_id = ForeignKeyField(
        Company,
        backref="performance_records",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    year = IntegerField()
    commodity_type = TextField(constraints=[Check(f"commodity_type IN {commodity_type_constraints}")])
    commodity_sub_type = TextField(null=True)
    commodity_stats = TextField(constraints=[Check("json_valid(commodity_stats)")])
    slug = TextField()

    class Meta:
        database = db
        table_name = "company_performance"


class ExportDestination(Model):
    country = TextField()
    year = IntegerField()
    commodity_type = TextField(constraints=[Check(f"commodity_type IN {commodity_type_constraints}")])
    export_USD = FloatField(null=True)
    export_volume_BPS = FloatField(null=True)
    export_volume_ESDM = FloatField(null=True)

    class Meta:
        database = db
        table_name = "export_destination"


class ResourcesAndReserves(Model):
    province = TextField(constraints=[Check(f"province IN {province_constraints}")])
    year = IntegerField()
    commodity_type = TextField(constraints=[Check(f"commodity_type IN {commodity_type_constraints}")])
    resources_reserves = TextField( constraints=[Check("json_valid(resources_reserves)")])

    class Meta:
        database = db
        table_name = "resources_and_reserves"


class TotalCommoditiesProduction(Model):
    commodity_type = TextField(constraints=[Check(f"commodity_type IN {commodity_type_constraints}")])
    production_volume = FloatField()
    unit = TextField()
    year = IntegerField()

    class Meta:
        database = db
        table_name = "total_commodities_production"


class CommodityPrice(Model):
    name = TextField()
    price = FloatField()
    date = DateField()

    class Meta:
        database = db
        table_name = "commodity_price"
        primary_key = CompositeKey("name", "date")


class GlobalCommodityData(Model):
    country = TextField()
    resources_reserves = TextField(
        null=True, constraints=[Check("json_valid(resources_reserves)")]
    )
    resources_reserves_share = TextField(
        null=True, constraints=[Check("json_valid(resources_reserves_share)")]
    )
    export_import = TextField(
        null=True, constraints=[Check("json_valid(export_import)")]
    )
    production_volume = TextField(
        null=True, constraints=[Check("json_valid(production_volume)")]
    )
    production_share = TextField(
        null=True, constraints=[Check("json_valid(production_share)")]
    )
    commodity_type = TextField()

    class Meta:
        database = db
        table_name = "global_commodity_data"


class MiningLicense(Model):
    license_type = TextField()
    license_number = TextField()
    wiup_code = TextField()
    province = TextField()
    city = TextField()
    license_effective_date = DateField()
    license_expiry_date = DateField()
    activity = TextField()
    licensed_area = FloatField()
    location = TextField()
    commodity_type = TextField()
    company_name = TextField()
    company_id = ForeignKeyField(
        Company,
        backref="licenses",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )

    class Meta:
        database = db
        table_name = "mining_license"


class MiningNews(Model):
    title = TextField()
    body = TextField()
    source = TextField()
    timestamp = DateTimeField()
    commodity_type = TextField()
    created_at = DateTimeField()

    class Meta:
        database = db
        table_name = "mining_news"


class SalesDestination(Model):
    company_id = ForeignKeyField(
        Company,
        backref="sales_destinations",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    country = TextField()
    idx_ticker = TextField()
    year = IntegerField()
    revenue = FloatField(null=True)
    percentage_of_total_revenue = FloatField(null=True)
    volume = FloatField(null=True)
    percentage_of_sales_volume = FloatField(null=True)

    class Meta:
        database = db
        table_name = "sales_destination"


class CompanyFinancials(Model):
    company_id = ForeignKeyField(
        Company,
        backref="financials",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    idx_ticker = TextField()
    name = TextField()
    year = IntegerField()
    assets = FloatField()
    revenue = FloatField()
    revenue_breakdown = TextField(constraints=[Check("json_valid(revenue_breakdown)")])
    cost_of_revenue = FloatField()
    cost_of_revenue_breakdown = TextField(constraints=[Check("json_valid(cost_of_revenue_breakdown)")])
    net_profit = FloatField()
    slug = TextField()

    class Meta:
        database = db
        table_name = "company_financials"
        primary_key = CompositeKey("idx_ticker", "year")


class MiningSite(Model):
    name = TextField()
    project_name = TextField(null=True)
    year = IntegerField()
    commodity_type = TextField(
        constraints=[Check(f"commodity_type IN {mineral_type_constraints}")]
    )
    company_id = ForeignKeyField(
        Company,
        backref="mining_sites",
        on_delete="NO ACTION",
        on_update="NO ACTION",
        column_name="company_id"
    )
    production_volume = FloatField(null=True)
    overburden_removal_volume = FloatField(null=True)
    strip_ratio = FloatField(null=True)
    resources_reserves = TextField(constraints=[Check("json_valid(resources_reserves)")])
    location = TextField(constraints=[Check("json_valid(location)")])

    class Meta:
        database = db
        table_name = "mining_site"


class MiningLicenseAuction(Model):
    commodity_type = TextField(
        constraints=[Check(f"commodity_type IN {mineral_type_constraints}")]
    )
    city = TextField()
    province = TextField()
    company_name = TextField()
    winner_date = TextField()
    licensed_area  = FloatField()
    license_number = TextField(unique=True)
    area_type = TextField()
    kdi = TextField()
    wiup_code = TextField()
    auction_status = TextField()
    created_at = TextField()
    last_modified = TextField()
    participant_count = IntegerField()
    phases = TextField(constraints=[Check("json_valid(phases)")])
    participants = TextField(constraints=[Check("json_valid(participants)")])
    winner = TextField()
    company_id = ForeignKeyField(
        Company,
        backref="license_auctions",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )

    class Meta:
        database = db
        table_name = "mining_license_auctions"
