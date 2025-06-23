table_configs = [
    {
        "dataset": {
            "source": "bronze", 
            "target": "silver"
        }, 
        "table_name": "ecomm_transactions", 
        "columns": [
            {
                "target_column": "invoice_no", 
                "source_column": "InvoiceNo", 
                "column_type": "str", 
                "nullable": False, 
                "transformation_rules": [
                    {"trim": ["ltrim", "rtrim"]}
                ]
            }, 
            {
                "target_column": "stock_code", 
                "source_column": "StockCode", 
                "column_type": "str", 
                "nullable": False, 
                "transformation_rules": [
                    {"trim": ["trim"]}
                ]
            }, 
            {
                "target_column": "description", 
                "source_column": "Description", 
                "column_type": "str", 
                "nullable": True, 
                "transformation_rules": [
                    {"trim": ["trim"]}
                ]
            }, 
            {
                "target_column": "quantity", 
                "source_column": "Quantity", 
                "column_type": "int", 
                "nullable": False
            }, 
            {
                "target_column": "invoice_date", 
                "source_column": "InvoiceDate", 
                "column_type": "date", 
                "nullable": False, 
                "transformation_rules": [
                    {"trim": ["trim"]}, 
                    {
                        "format_datetime": {
                            "source_format": "M/d/yyyy H:mm", 
                            "target_format": "yyyy-MM-dd"
                        }
                    }
                ]
            }, 
            {
                "target_column": "unit_price", 
                "source_column": "UnitPrice", 
                "column_type": "float", 
                "nullable": False, 
                "transformation_rules": [{"round": 2}]
            }, 
            {
                "target_column": "customer_id", 
                "source_column": "CustomerID", 
                "column_type": "int", 
                "nullable": True, 
                "transformation_rules": [{"trim": ["trim"]}]
            }, 
            {
                "target_column": "country", 
                "source_column": "Country", 
                "column_type": "str", 
                "nullable": False
            }
        ], 
        "new_columns": [
            {
                "target_column": "invoice_timestamp", 
                "source_column": "InvoiceDate", 
                "column_type": "timestamp", 
                "nullable": False, 
                "transformation_rules": [
                    {
                        "format_datetime": {
                            "source_format": "M/d/yyyy H:mm", 
                            "target_format": "yyyy-MM-dd HH:mm:ss"
                        }
                    }
                ]
            }
        ], 
            "primary_key": ["invoice_no", "stock_code", "quantity", "invoice_date", "unit_price", "country"], 
            "partition_column": ["invoice_date", "country"]
    }
]