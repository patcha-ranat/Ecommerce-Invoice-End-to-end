-- initial script creating source db

-- SELECT * FROM read_parquet("data/ecomm_invoice_transaction.parquet")

CREATE TABLE IF NOT EXISTS ecomm.sales_transaction AS (
    SELECT * FROM read_parquet("data/ecomm_invoice_transaction.parquet")
);