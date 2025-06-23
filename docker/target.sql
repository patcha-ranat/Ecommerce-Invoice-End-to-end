DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    "invoice_no" TEXT,
    "stock_code" TEXT,
    "description" TEXT,
    "quantity" INT,
    "invoice_date" TIMESTAMP,
    "unit_rrice" FLOAT,
    "customer_id" INT,
    "country" TEXT,
    "total_spend" FLOAT
);