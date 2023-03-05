importer_condition = {
    "$lookup": {
        "from": "DimImporter",
        "let": {"importerNit": "$NIT_IMPORTADOR", "importerName": "$NOMBRE_IMPORTADOR"},
        "pipeline": [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$NIT_IMPORTADOR", "$$importerNit"]},
                            {"$eq": ["$NOMBRE_IMPORTADOR", "$$importerName"]},
                        ]
                    }
                }
            }
        ],
        "as": "di",
    }
}
