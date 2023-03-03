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
                "cdcia_usuaria": "$CDCIA_USUARIA",
                "dscia_usuaria": "$DSCIA_USUARIA",
                "dstipo_actividad": "$DSTIPO_ACTIVIDAD",
                "nit_compania": "$NIT_COMPANIA",
            }
        }
    },
    {
        # Project only relevant fields
        "$project": {
            "_id": 0,
            "cdcia_usuaria": "$_id.cdcia_usuaria",
            "dscia_usuaria": "$_id.dscia_usuaria",
            "dstipo_actividad": "$_id.dstipo_actividad",
            "nit_compania": "$_id.nit_compania",
        }
    },
]


dim_company = database.get_collection("DimCompany")
dim_company.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
