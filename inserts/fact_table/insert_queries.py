def get_unwind(field_name: str):
    fecha_rechazo_unwind_stage = {
        "$unwind": "$" + field_name,
    }
    return fecha_rechazo_unwind_stage


cond = {
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


def get_lookup():
    fecha_rechazo_stage = {
            "$lookup": {
                "from": "DimProduct",
                "let": {
                    "item": "$ITEM",
                    "tipo_item": "$TIPO_ITEM",
                    "unidad_medida": "$UNIDAD_MEDIDA",
                    "cod_item": "$COD_ITEM",
                },
                "pipeline": [cond],
                "as": "DimProduct",
            }
    }
    
    return fecha_rechazo_stage
