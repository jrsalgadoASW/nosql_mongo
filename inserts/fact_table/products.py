def get_product_condition(ds_item_label: str):
    product_condition = {
        "$match": {
            "$expr": {
                "$and": [
                    {
                        "$eq": [
                            "$" + ds_item_label,
                            "$Item"
                        ]
                    },
                    # {
                    #     "$eq": [
                    #         {"$month": "$" + ds_column_name},
                    #         "$DateMonth",
                    #     ]
                    # },
                    # {
                    #     "$eq": [
                    #         {"$year": "$" + ds_column_name},
                    #         "$DateYear",
                    #     ]
                    # },
                    # {
                    #     "$eq": [
                    #         {"$hour": "$" + ds_column_name},
                    #         "$DateHour",
                    #     ]
                    # },
                ]
            },
        },
    }
    return product_condition
