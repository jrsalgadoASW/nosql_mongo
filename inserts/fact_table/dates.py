def get_date_conditions(col_name: str):
    date_condition = {
        "$match": {
            "$expr": {
                "$and": [
                    {
                        "$eq": [
                            {"$dayOfWeek": "$" + col_name},
                            "$DateDay",
                        ]
                    },
                    {
                        "$eq": [
                            {"$month": "$" + col_name},
                            "$DateMonth",
                        ]
                    },
                    {
                        "$eq": [
                            {"$year": "$" + col_name},
                            "$DateYear",
                        ]
                    },
                    {
                        "$eq": [
                            {"$hour": "$" + col_name},
                            "$DateHour",
                        ]
                    },
                ]
            },
        },
    }
    return date_condition


def get_date_unwind(field_name: str):
    fecha_rechazo_unwind_stage = {
        "$unwind": "$" + field_name,
    }
    return fecha_rechazo_unwind_stage


def get_date_lookup(table_name: str, field_name: str):
    fecha_rechazo_stage = {
        "$lookup": {
            "from": "DimDate",
            "pipeline": [get_date_conditions(table_name)],
            "as": field_name,
        },
    }
    return fecha_rechazo_stage


def get_date_projection(new_field_name: str, field_name: str):
    projection_pipeline = {"$project": {new_field_name: "$" + field_name + "._id"}}
    return projection_pipeline
