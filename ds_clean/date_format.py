from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")

date_labels = [
    "FECHA_REVISION",
    "FECHA_RECHAZO",
    "FECHA_EJECUCION",
    "FECHA_DIGITALIZACION",
    "FECHA_DEFINITIVO",
    "FECHA_APROBACION",
]
for label in date_labels:
    data_semilla.aggregate(
        [
            {
                "$addFields": {
                    "convertedDate": {
                        "$dateFromString": {
                            "dateString": "$"+label,
                            "format": "%d/%m/%Y %H:%M",
                        }
                    }
                }
            },
            {"$unset": "dateField"},
            {"$addFields": {label: "$convertedDate"}},
            {"$unset": "convertedDate"},
            {"$out": "DataSemilla"},
        ]
    )
