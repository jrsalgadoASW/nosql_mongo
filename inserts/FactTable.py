from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")

# FECHAS
# FECHAS LABELS
FechaRechazoID_Label = "FechaRechazoID"
FechaAprobacionID_Label = "FechaAprobacionID"
FechaDefinitivoID_Label = "FechaDefinitivoID"
FechaRevisionID_Label = "FechaRevisionID"
FechaDigitalizacionID_Label = "FechaDigitalizacionID"

# Dim Fechas label
dim_date_label = "DimDate"


date_parsing_operation = {
    "$dateFromString": {
        "dateString": "$curr_date",
        "format": "%d/%m/%Y %H:%M",
    },
}

# Fechas Pipeline
fecha_rechazo_conditions = {
    "$match": {
        "$expr": {
            "$and": [
                {
                    "$eq": [
                        {"$dayOfWeek": "$FECHA_RECHAZO"},
                        "$DateDay",
                    ]
                },
                {
                    "$eq": [
                        {"$month": "$FECHA_RECHAZO"},
                        "$DateMonth",
                    ]
                },
                {
                    "$eq": [
                        {"$year": "$FECHA_RECHAZO"},
                        "$DateYear",
                    ]
                },
                {
                    "$eq": [
                        {"$hour": "$FECHA_RECHAZO"},
                        "$DateHour",
                    ]
                },
            ]
        },
    },
}
fecha_rechazo_pipeline = {
    "$lookup": {
        "from": "DimDate",
        "pipeline": [fecha_rechazo_conditions],
        "as": "ddRechazo",
    }
}

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


data_semilla_pipeline = [fecha_rechazo_pipeline]

res = data_semilla.aggregate(pipeline=data_semilla_pipeline)
print(len(list(res)))
