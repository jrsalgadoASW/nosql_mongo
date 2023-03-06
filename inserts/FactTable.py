from pymongo import MongoClient
from fact_table.dates import get_date_lookup, get_date_unwind
from fact_table.insert_queries import get_lookup, get_unwind
from fact_table.products import get_product_condition


# Fact table labels
fact_table_label = {
    "fecha_rechazo": "FechaRechazoID",
    "fecha_aprobacion": "FechaAprobacionID",
    "fecha_definitivo": "FechaDefinitivoID",
    "fecha_revision": "FechaRevisionID",
    "fecha_digitalizacion": "FechaDigitalizacionID",
    "fecha_ejecucion": "FechaEjecucionID",
    "producto_id": "ProductoID",
    "pais_origen": "PaisOrigenID",
    "pais_bandera": "PaisBanderaID",
    "pais_destino": "PaisDestinoID",
    "pais_procedencia": "PaisProcedenciaID",
    "pais_compra": "PaisCompraID",
    "empresa": "EmpresaID",
    "importador": "ImporterId",
    "sia": "SiaID",
    "estado": "EstadoID",
    "tipo_transaccion": "TipoTransaccionID",
    "cantidad": "CANTIDAD",
    "precio": "PRECIO",
    "peso_bruto": "PESO_BRUTO",
    "peso_neto": "PESO_NETO",
    "fletes": "FLETES",
    "fob": "FOB",
}

# DataSemilla columns
ds_labels = {
    "fecha_aprobacion": "FECHA_APROBACION",
    "fecha_definitivo": "FECHA_DEFINITIVO",
    "fecha_digitalizacion": "FECHA_DIGITALIZACION",
    "fecha_ejecucion": "FECHA_EJECUCION",
    "fecha_rechazo": "FECHA_RECHAZO",
    "fecha_revision": "FECHA_REVISION",
    "bandera": "BANDERA",
    "bultos": "BULTOS",
    "cantidad": "CANTIDAD",
    "cdcia_usuaria": "CDCIA_USUARIA",
    "cddoc_transporte": "CDDOC_TRANSPORTE",
    "cdestado": "CDESTADO",
    "cdfactura": "CDFACTURA",
    "cdtransaccion": "CDTRANSACCION",
    "cod_item": "COD_ITEM",
    "cod_pais_bandera": "COD_PAIS_BANDERA",
    "cod_pais_compra": "COD_PAIS_COMPRA",
    "cod_pais_destino": "COD_PAIS_DESTINO",
    "cod_pais_origen": "COD_PAIS_ORIGEN",
    "cod_pais_procedencia": "COD_PAIS_PROCEDENCIA",
    "cod_zonafranca": "COD_ZONAFRANCA",
    "descrip_subpartida": "DESCRIP_SUBPARTIDA",
    "dscia_usuaria": "DSCIA_USUARIA",
    "dsestado": "DSESTADO",
    "dstipo_actividad": "DSTIPO_ACTIVIDAD",
    "dstransaccion": "DSTRANSACCION",
    "embalaje": "EMBALAJE",
    "fletes": "FLETES",
    "fmm": "FMM",
    "fob": "FOB",
    "item": "ITEM",
    "modo_transporte": "MODO_TRANSPORTE",
    "nit_compania": "NIT_COMPANIA",
    "nit_importador": "NIT_IMPORTADOR",
    "nmconversion": "NMCONVERSION",
    "nombre_importador": "NOMBRE_IMPORTADOR",
    "pais_compra": "PAIS_COMPRA",
    "pais_destino": "PAIS_DESTINO",
    "pais_origen": "PAIS_ORIGEN",
    "pais_procedencia": "PAIS_PROCEDENCIA",
    "peso_bruto": "PESO_BRUTO",
    "peso_neto": "PESO_NETO",
    "precio": "PRECIO",
    "sndeclaracion": "SNDECLARACION",
    "subpartida": "SUBPARTIDA",
    "tasa_cambio": "TASA_CAMBIO",
    "tipo": "TIPO",
    "tipo_item": "TIPO_ITEM",
    "unidad_comercial": "UNIDAD_COMERCIAL",
    "unidad_medida": "UNIDAD_MEDIDA",
    "unidad_subpartida": "UNIDAD_SUBPARTIDA",
}


property_name = {
    "fecha_aprobacion": "dd_fecha_aprobacion",
    "fecha_definitivo": "dd_fecha_definitivo",
    "fecha_digitalizacion": "dd_fecha_digitalizacion",
    "fecha_ejecucion": "dd_fecha_ejecucion",
    "fecha_rechazo": "dd_fecha_rechazo",
    "fecha_revision": "dd_fecha_revision",
    "bandera": "dd_bandera",
    "bultos": "dd_bultos",
    "cantidad": "dd_cantidad",
    "cdcia_usuaria": "dd_cdcia_usuaria",
    "cddoc_transporte": "dd_cddoc_transporte",
    "cdestado": "dd_cdestado",
    "cdfactura": "dd_cdfactura",
    "cdtransaccion": "dd_cdtransaccion",
    "cod_item": "dd_cod_item",
    "cod_pais_bandera": "dd_cod_pais_bandera",
    "cod_pais_compra": "dd_cod_pais_compra",
    "cod_pais_destino": "dd_cod_pais_destino",
    "cod_pais_origen": "dd_cod_pais_origen",
    "cod_pais_procedencia": "dd_cod_pais_procedencia",
    "cod_zonafranca": "dd_cod_zonafranca",
    "descrip_subpartida": "dd_descrip_subpartida",
    "dscia_usuaria": "dd_dscia_usuaria",
    "dsestado": "dd_dsestado",
    "dstipo_actividad": "dd_dstipo_actividad",
    "dstransaccion": "dd_dstransaccion",
    "embalaje": "dd_embalaje",
    "fletes": "dd_fletes",
    "fmm": "dd_fmm",
    "fob": "dd_fob",
    "item": "dd_item",
    "modo_transporte": "dd_modo_transporte",
    "nit_compania": "dd_nit_compania",
    "nit_importador": "dd_nit_importador",
    "nmconversion": "dd_nmconversion",
    "nombre_importador": "dd_nombre_importador",
    "pais_compra": "dd_pais_compra",
    "pais_destino": "dd_pais_destino",
    "pais_origen": "dd_pais_origen",
    "pais_procedencia": "dd_pais_procedencia",
    "peso_bruto": "dd_peso_bruto",
    "peso_neto": "dd_peso_neto",
    "precio": "dd_precio",
    "sndeclaracion": "dd_sndeclaracion",
    "subpartida": "dd_subpartida",
    "tasa_cambio": "dd_tasa_cambio",
    # "tipo": "dd_tipo",
    # "tipo_item": "dd_tipo_item",
    "unidad_comercial": "dd_unidad_comercial",
    "unidad_medida": "dd_unidad_medida",
    "unidad_subpartida": "dd_unidad_subpartida",

    "producto" : "dd_producto"
}


client = MongoClient("mongodb://localhost:27017/")
database = client["Customs"]
data_semilla = database.get_collection("DataSemilla")
fact_test = database.get_collection("fact_test")

data_semilla_pipeline = [
    # Fechas
    get_date_lookup(table_name=ds_labels["fecha_rechazo"], field_name=property_name["fecha_rechazo"]),
    get_date_unwind(field_name=property_name["fecha_rechazo"]),

    get_date_lookup(table_name=ds_labels["fecha_ejecucion"], field_name=property_name["fecha_ejecucion"]),
    get_date_unwind(field_name=property_name["fecha_ejecucion"]),

    get_date_lookup(table_name=ds_labels["fecha_digitalizacion"], field_name=property_name["fecha_digitalizacion"]),
    get_date_unwind(field_name=property_name["fecha_digitalizacion"]),

    get_date_lookup(table_name=ds_labels["fecha_definitivo"], field_name=property_name["fecha_definitivo"]),
    get_date_unwind(field_name=property_name["fecha_definitivo"]),

    get_date_lookup(table_name=ds_labels["fecha_aprobacion"], field_name=property_name["fecha_aprobacion"]),
    get_date_unwind(field_name=property_name["fecha_aprobacion"]),

    get_date_lookup(table_name=ds_labels["fecha_revision"], field_name=property_name["fecha_revision"]),
    get_date_unwind(field_name=property_name["fecha_revision"]),

    # get_lookup(),
    # get_unwind(property_name["producto"]),
    {
        "$project": {
            #Fechas
            fact_table_label["fecha_rechazo"]: "$" + property_name["fecha_rechazo"] + "._id",
            fact_table_label["fecha_ejecucion"]: "$" + property_name["fecha_ejecucion"] + "._id",
            fact_table_label["fecha_digitalizacion"]: "$" + property_name["fecha_digitalizacion"] + "._id",
            fact_table_label["fecha_definitivo"]: "$" + property_name["fecha_definitivo"] + "._id",
            fact_table_label["fecha_aprobacion"]: "$" + property_name["fecha_aprobacion"] + "._id",
            fact_table_label["fecha_revision"]: "$" + property_name["fecha_revision"] + "._id",

            #producto
            # fact_table_label["producto_id"]: "$" + property_name["producto"] + "._id",
        },
    },
    {"$out": "fact_test"},
]

fact_test.drop()
res = data_semilla.aggregate(pipeline=data_semilla_pipeline)

print(fact_test.count_documents({}))
