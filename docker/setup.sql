CREATE SCHEMA IF NOT EXISTS kde_ecomm;

DROP TABLE IF EXISTS kde_ecomm.transactions;

CREATE TABLE myschema.transactions (
    "InvoiceNo" TEXT,
    "StockCode" TEXT,
    "Description" TEXT,
    "Quantity" INT,
    "InvoiceDate" TIMESTAMP,
    "UnitPrice" FLOAT,
    "CustomerID" TEXT,
    "Country" TEXT
);

COPY myschema.transactions
FROM '/data/uncleaned_data.csv' DELIMITER ',' CSV HEADER;