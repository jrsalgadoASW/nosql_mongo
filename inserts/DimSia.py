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
                "Nit_sia": "$NIT_SIA",
                "Nombre_sia": "$NOMBRE_SIA",
            }
        }
    },
    {
        #Project only relevant fields
        "$project": {
            "_id":0,
            "Nit_sia": "$_id.Nit_sia",
            "Nombre_sia": "$_id.Nombre_sia",
        }
    },
]


dim_sia = database.get_collection("DimSia")
dim_sia.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
