sia_condition = {
    "$lookup": {
        "from": "DimSia",
        "let": {
            "siaNit": {"$ifNull": ["$NIT_SIA", 0]},
            "siaName": {"$ifNull": ["$NOMBRE_SIA", ""]},
        },
        "pipeline": [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$NIT_SIA", "$$siaNit"]},
                            {"$eq": ["$NOMBRE_SIA", "$$siaName"]},
                        ]
                    }
                }
            }
        ],
        "as": "dSia",
    }
}
