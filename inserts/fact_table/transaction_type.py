transaction_type_condition = {
    "$lookup": {
        "from": "DimTransactionType",
        "let": {"transactionTypeId": {"$ifNull": ["$CDTRANSACCION", 0]}},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$CDTRANSACCION", "$$transactionTypeId"]}}}
        ],
        "as": "dtt",
    }
}
