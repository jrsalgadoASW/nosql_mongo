from pymongo import MongoClient
from fact_table.dates.fecha_rechazo import (
    fecha_rechazo_stage,
    fecha_rechazo_unwind_stage,
    projection_pipeline,
    fecha_rechazo_pipeline,
)
from fact_table.dates.fecha_aprobacion import (
    fecha_aprobacion_stage,
    fecha_aprobacion_unwind_stage,
    projection_aprobacion,
)

client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")
fact_test = database.get_collection("fact_test")

# FECHAS
# FECHAS LABELS
FechaRechazoID_Label = "FechaRechazoID"
FechaAprobacionID_Label = "FechaAprobacionID"
FechaDefinitivoID_Label = "FechaDefinitivoID"
FechaRevisionID_Label = "FechaRevisionID"
FechaDigitalizacionID_Label = "FechaDigitalizacionID"


ProductoID_Label = "ProductoID"

PaisOrigenID_Label = "PaisOrigenID"
PaisBanderaID_Label = "PaisBanderaID"
PaisDestinoID_Label = "PaisDestinoID"
PaisProcedenciaID_Label = "PaisProcedenciaID"
PaisCompraID_Label = "PaisCompraID"

EmpresaID_Label = "EmpresaID"
ImporterId_Label = "ImporterId"
SiaID_Label = "SiaID"
EstadoID_Label = "EstadoID"

TipoTransaccionID_Label = "TipoTransaccionID"

Cantidad_Label = "CANTIDAD"
Precio_Label = "PRECIO"
Peso_bruto_Label = "PESO_BRUTO"
Peso_neto_Label = "PESO_NETO"
Fletes_Label = "FLETES"
Fob_Label = "FOB"


def date_conditions(col_name: str):
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


# data_semilla_pipeline = [
#     fecha_rechazo_stage,
#     fecha_rechazo_unwind_stage,
#     projection_pipeline
# ]

# d = [
#     {
#         "$lookup": {
#             "from": "customfield.values",
#             "let": {emp_id: "$_id"},
#             pipeline: [
#                 {"$match": {"$expr": {"$eq": ["$employee", "$$emp_id"]}}},
#                 {
#                     "$lookup": {
#                         "from": "customfields",
#                         "let": {custom_field: "$customfield"},
#                         pipeline: [
#                             {"$match": {"$expr": {"$eq": ["$_id", "$$custom_field"]}}}
#                         ],
#                         "as": "customfield",
#                     }
#                 },
#             ],
#             "as": "customfieldvalues",
#         }
#     }
# ]

data_semilla_pipeline = [
    # {
    #     "$facet": {
    #         "fecha_rechazo_pipeline": fecha_rechazo_pipeline,
    #     },
    # },
    # {"$unwind": "$fecha_rechazo_pipeline"},
    # {
    #     "$project": {
    #         FechaRechazoID_Label: "$fecha_rechazo_pipeline.Fecha_Rechazo",
    #     }
    # },
    fecha_rechazo_stage,
    fecha_rechazo_unwind_stage,
    # projection_pipeline,
    fecha_aprobacion_stage,
    fecha_aprobacion_unwind_stage,
    # projection_aprobacion,
    # {
    #     "$facet": {
    #         "fecha_aprobacion_pipeline": fecha_aprobacion_pipeline,
    #     },
    # },
    # {"$unwind": "$fecha_aprobacion_pipeline"},
    {
        "$project": {
            FechaAprobacionID_Label: "$ddAprobacion._id",
            FechaRechazoID_Label: "$ddRechazo._id",
        },
    },
    {"$out": "fact_test"},
]

fact_test.drop()
res = data_semilla.aggregate(pipeline=data_semilla_pipeline)

print(fact_test.count_documents({}))
