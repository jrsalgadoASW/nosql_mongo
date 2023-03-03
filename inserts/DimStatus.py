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
                "cod_estado": "$CDESTADO",
                "ds_estado": "$DSESTADO",
            }
        }
    },
    {
        # Project only relevant fields
        "$project": {
            "_id": 0,
            "Cod_estado": "$_id.cod_estado",
            "Ds_estado": "$_id.ds_estado",
        }
    },
]


dim_status = database.get_collection("DimStatus")
dim_status.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
