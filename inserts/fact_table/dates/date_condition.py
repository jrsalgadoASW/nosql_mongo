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
