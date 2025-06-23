# workdir: project_root

python code/ingestion/__main__.py \
    --spark-log-level ERROR \
    --ingestion-config '{"dataset": {"source": "bronze", "target": "silver"}, "table_name": "ecomm_transactions", "columns": [{"target_column": "invoice_no", "source_column": "InvoiceNo", "column_type": "str", "nullable": false, "transformation_rules": [{"trim": ["ltrim", "rtrim"]}]}, {"target_column": "stock_code", "source_column": "StockCode", "column_type": "str", "nullable": false, "transformation_rules": [{"trim": ["trim"]}]}, {"target_column": "description", "source_column": "Description", "column_type": "str", "nullable": true, "transformation_rules": [{"trim": ["trim"]}]}, {"target_column": "quantity", "source_column": "Quantity", "column_type": "int", "nullable": false}, {"target_column": "invoice_date", "source_column": "InvoiceDate", "column_type": "date", "nullable": false, "transformation_rules": [{"trim": ["trim"]}, {"format_datetime": {"source_format": "M/d/yyyy H:mm", "target_format": "yyyy-MM-dd"}}]}, {"target_column": "unit_price", "source_column": "UnitPrice", "column_type": "float", "nullable": false, "transformation_rules": [{"round": 2}]}, {"target_column": "customer_id", "source_column": "CustomerID", "column_type": "int", "nullable": true, "transformation_rules": [{"trim": ["trim"]}]}, {"target_column": "country", "source_column": "Country", "column_type": "str", "nullable": false}], "new_columns": [{"target_column": "invoice_timestamp", "source_column": "InvoiceDate", "column_type": "timestamp", "nullable": false, "transformation_rules": [{"format_datetime": {"source_format": "M/d/yyyy H:mm", "target_format": "yyyy-MM-dd HH:mm:ss"}}]}], "primary_key": ["invoice_no", "stock_code", "quantity", "invoice_date", "unit_price", "country"], "partition_column": ["invoice_date", "country"]}' \
    --input-path ./data/uncleaned_data.csv \
    --output-path ./test_output/output.parquet \
    --dt "2025-04-23" \
    --write-mode overwrite \
    --read-format csv \
    --write-format parquet \
    --ingestion-mode batch
