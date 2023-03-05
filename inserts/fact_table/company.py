company_condition = {
    "$match": {
        "$expr": {
            "$and": [
                {"$eq": ["$$item", "$ITEM"]},
                {"$eq": ["$$tipo_item", "$TIPO_ITEM"]},
                {"$eq": ["$$unidad_medida", "$UNIDAD_MEDIDA"]},
                {"$eq": ["$$cod_item", "$COD_ITEM"]},
            ]
        }
    }
}
