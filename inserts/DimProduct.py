from pymongo import MongoClient, errors as pymongoErrors, ASCENDING, DESCENDING

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")
pipeline = [
    {
        # Doc id = combination of countrycod and countryname, so they are unique.
        "$group": {
            "_id": {
                "cod_item": "$COD_ITEM",
                "item": "$ITEM",
                "tipo_item": "$TIPO_ITEM",
                "unidad_medida": "$UNIDAD_MEDIDA",
            }
        }
    },
    {
        # Project only relevant fields
        "$project": {
            "_id": 0,
            "Cod_item": "$_id.cod_item",
            "Item": "$_id.item",
            "Tipo_item": "$_id.tipo_item",
            "Unidad_medida": "$_id.unidad_medida",
        }
    },
]


try:
    db = database["DimProduct"]
    db.drop()
except pymongoErrors.CollectionInvalid:
    print("skipping drop")

dim_product = database.get_collection("DimProduct")


dim_product.insert_many(data_semilla.aggregate(pipeline=pipeline))
dim_product.create_index(
    [
        ("Cod_item", ASCENDING),
        ("Item", ASCENDING),
        ("Tipo_item", ASCENDING),
        ("Unidad_medida", ASCENDING),
    ],
    unique=True,
    name="PK_Product"
)


client.close()
