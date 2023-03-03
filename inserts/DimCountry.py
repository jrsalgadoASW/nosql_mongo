from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")

pipeline = [
    {
        # Fist Stage: Group the values
        "$group": {
            "_id": None,
            "origin": {
                "$addToSet": {
                    "CountryCod": "$COD_PAIS_ORIGEN",
                    "CountryName": "$PAIS_ORIGEN",
                }
            },
            "procedence": {
                "$addToSet": {
                    "CountryCod": "$COD_PAIS_PROCEDENCIA",
                    "CountryName": "$PAIS_PROCEDENCIA",
                }
            },
            "flag": {
                "$addToSet": {
                    "CountryCod": "$COD_PAIS_BANDERA",
                    "CountryName": "$BANDERA",
                }
            },
            "purchase": {
                "$addToSet": {
                    "CountryCod": "$COD_PAIS_COMPRA",
                    "CountryName": "$PAIS_COMPRA",
                }
            },
            "destination": {
                "$addToSet": {
                    "CountryCod": "$COD_PAIS_DESTINO",
                    "CountryName": "$PAIS_DESTINO",
                }
            },
        },
    },
    {
        # SECOND STAGE: Concat all the groups
        "$project": {
            "_id": 0,
            "docs": {
                "$concatArrays": [
                    "$origin",
                    "$procedence",
                    "$flag",
                    "$purchase",
                    "$destination",
                ]
            },
        }
    },
    # Third Stage: get docs out of array
    {"$unwind": "$docs"},
    {
        # Fourth Stage: Doc id = combination of countrycod and countryname, so they are unique.
        "$group": {
            "_id": {
                "CountryCod": "$docs.CountryCod",
                "CountryName": "$docs.CountryName",
            }
        }
    },
    {
        # Fifth Stage: Doc Structures
        "$project": {
            "_id": 0,
            "CountryCod": "$_id.CountryCod",
            "CountryName": "$_id.CountryName",
        }
    },
]


dim_country = database.get_collection("DimCountry")
dim_country.insert_many(data_semilla.aggregate(pipeline=pipeline))


client.close()
