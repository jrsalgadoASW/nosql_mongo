status_condition = {
    "$lookup": {
        "from": "DimStatus",
        "let": {"statusId": {"$ifNull": ["$CDESTADO", 0]}},
        "pipeline": [{"$match": {"$expr": {"$eq": ["$CDESTADO", "$$statusId"]}}}],
        "as": "dStatus",
    }
}
