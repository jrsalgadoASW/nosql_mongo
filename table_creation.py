from pymongo import MongoClient, errors as pymongoErrors

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]

# schemas

country_schema = {
    "CountryCod": {"type": "number", "required": True},
    "CountryName": {"type": "string"},
}

date_schema = {
    "DateYear": {"type": "number"},
    "DateMonth": {"type": "number"},
    "DateDay": {"type": "number"},
    "DateHour": {"type": "number"},
    "DateWeekDay": {"type": "number"},
    "DateWeekDayName": {"type": "string"},
    "DateMonthName": {"type": "string"},
    "Semester": {"type": "string"},
}

product_schema = {
    "Cod_Item": {"type": "string"},
    "Item": {"type": "string"},
    "Tipo_item": {"type": "string"},
    "Unidad_medida": {"type": "string"},
}


# Create tables

try:
    db = database["FactImportRegistry"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimCountry"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimDate"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimProduct"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimSia"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimTransactionType"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimStatus"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimCompany"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")
try:
    db = database["DimImporter"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")

database.create_collection("FactImportRegistry")
database.create_collection("DimCountry")
database.create_collection("DimDate")
database.create_collection("DimProduct")
database.create_collection("DimSia")
database.create_collection("DimTransactionType")
database.create_collection("DimStatus")
database.create_collection("DimCompany")
database.create_collection("DimImporter")

client.close()
