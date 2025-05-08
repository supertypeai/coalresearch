from peewee import (
    Model,
    SqliteDatabase,
    CharField,
    TextField,
    IntegerField,
    ForeignKeyField,
    FloatField,
    Check,
    CompositeKey
)
operation_province_constraints = (
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
    "Yogyakarta"
)
company_type_constraints = (
    'Holding', 
    'Mine Owner', 
    'Consultant', 
    'Contractor', 
    'Trader', 
)
key_operation_constraints = (
    'Mining', 
    'Mining Services', 
    'Equipment Rental', 
    'Logistic Management', 
    'Overburden Removal & Hauling', 
    'Dredging', 
    'Coal Trading', 
    'Investment' 
)

mineral_type_constraints = (
    "Coal", 
    "Aluminium", 
    "Gold", 
    "Zinc and Lead", 
    "Oil", 
    "Nickel"
)

# %%
db = SqliteDatabase('coal-db.sqlite')
# %%
class Company(Model):
    id = IntegerField(primary_key=True)
    name = CharField()
    idx_ticker = CharField(null=True)
    operation_province = CharField(
        null=True,
        constraints=[
            Check(f"operation_province IN {operation_province_constraints}")
        ]
    )
    operation_kabkot = CharField(null=True)
    representative_address = TextField(null=True)
    company_type = CharField(
        null=True,
        constraints=[
            Check(f"company_type IN {company_type_constraints}")
        ]
    )
    key_operation = CharField(
        constraints=[
            Check(f"key_operation IN {key_operation_constraints}")
        ]
    )
    activities = TextField(
        null=True,
        constraints=[Check("json_valid(activities)")]
    )
    website = CharField(null=True)
    phone_number = IntegerField(null=True)
    email = CharField(null=True)

    class Meta:
        database = db
        table_name = 'company'

class CompanyOwnership(Model):
    parent = ForeignKeyField(
        Company,
        column_name='parent_company_id'
    )
    company = ForeignKeyField(
        Company,
        column_name='company_id'
    )
    percentage_ownership = FloatField(null=True)

    class Meta:
        database = db
        table_name = 'company_ownership'
        primary_key = CompositeKey('parent', 'company')

class CoalCompanyPerformance(Model):
    id = IntegerField(primary_key=True)
    company = ForeignKeyField(
        Company,
        column_name='company_id'
    )
    year = IntegerField(null=True)
    mineral_type = CharField(
        null=True,
        constraints=[
            Check(f"mineral_type IN {mineral_type_constraints}")
        ]
    )
    calorific_value = CharField(null=True)
    mining_operation_status = CharField(
        null=True,
        constraints=[
            Check("mining_operation_status IN ('production', 'development', 'inactive')")
        ]
    )
    mining_permit = CharField(
        null=True,
        constraints=[
            Check("mining_permit IN ('IUP', 'IUPK', 'PKP2B', 'IUPJ', 'WIUP')")
        ]
    )
    area = IntegerField(null=True)
    production_volume = FloatField(null=True)
    sales_volume = FloatField(null=True)
    overburden_removal_volume = FloatField(null=True)
    strip_ratio = FloatField(null=True)
    reserve = FloatField(null=True)
    resource = FloatField(null=True)
    mineral_sub_type = CharField(null=True)
    area_geometry = TextField(
        null=True,
        constraints=[Check("json_valid(area_geometry)")]
    )

    class Meta:
        database = db
        table_name = 'coal_company_performance'

class MiningSite(Model):
    id = IntegerField(primary_key=True)
    name = CharField()
    year = IntegerField(null=True)
    company = ForeignKeyField(
        Company,
        column_name='company_id'
    )
    calorific_value = CharField(null=True)
    production_volume = FloatField(null=True)
    overburden_removal_volume = FloatField(null=True)
    strip_ratio = FloatField(null=True)
    reserve = FloatField(null=True)
    resource = FloatField(null=True)
    province = CharField(
        null=True,
        constraints=[
            Check(f"province IN {operation_province_constraints}")
        ]
    )
    city = CharField(null=True)
    mineral_type = CharField(
        null=True,
        constraints=[
            Check(f"mineral_type IN {mineral_type_constraints}")
        ]
    )

    class Meta:
        database = db
        table_name = 'mining_site'
