from .date_condition import date_conditions

fecha_aprobacion_stage = {
    "$lookup": {
        "from": "DimDate",
        "pipeline": [date_conditions("FECHA_APROBACION")],
        "as": "ddAprobacion",
    },
}

fecha_aprobacion_unwind_stage = {
    "$unwind": "$ddAprobacion",
}



projection_aprobacion = {"$project": {"Fecha_Aprobacion": "$ddAprobacion._id"}}

fecha_aprobacion_pipeline = [
    fecha_aprobacion_stage,
    fecha_aprobacion_unwind_stage,
    projection_aprobacion,
]
