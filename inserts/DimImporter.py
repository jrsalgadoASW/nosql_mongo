from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")

pipeline = [
    {
        # Doc id = combination of countrycod and countryname, so they are unique.
        "$group": {
            "_id": {
                "nit_importador": "$NIT_IMPORTADOR",
                "nombre_importador": "$NOMBRE_IMPORTADOR",
            }
        }
    },
    {
        # Project only relevant fields
        "$project": {
            "_id": 0,
            "nit_importador": "$_id.nit_importador",
            "nombre_importador": "$_id.nombre_importador",
        }
    },
]


dim_importer = database.get_collection("DimImporter")
dim_importer.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
