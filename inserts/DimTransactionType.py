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
                "cod_transaccion": "$CDTRANSACCION",
                "ds_transaccion": "$DSTRANSACCION",
            }
        }
    },
    {
        #Project only relevant fields
        "$project": {
            "_id":0,
            "Cod_transaccion": "$_id.cod_transaccion",
            "Ds_transaccion": "$_id.ds_transaccion",
        }
    },
]


dim_transactionType = database.get_collection("DimTransactionType")
dim_transactionType.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
