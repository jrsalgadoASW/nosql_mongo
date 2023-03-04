from .date_condition import date_conditions

fecha_rechazo_stage = {
    "$lookup": {
        "from": "DimDate",
        "pipeline": [date_conditions("FECHA_RECHAZO")],
        "as": "ddRechazo",
    },
}

fecha_rechazo_unwind_stage = {
    "$unwind": "$ddRechazo",
}
fecha_rechazo_pipeline_unwind = {"$unwind": "$fecha_rechazo_pipeline"}

fecha_rechazo_stage = {
    "$lookup": {
        "from": "DimDate",
        "pipeline": [date_conditions("FECHA_RECHAZO")],
        "as": "ddRechazo",
    },
}

projection_pipeline = {"$project": {"Fecha_Rechazo": "$ddRechazo._id"}}

fecha_rechazo_pipeline = [
    fecha_rechazo_stage,
    fecha_rechazo_unwind_stage,
    projection_pipeline,
]
