CREATE SCHEMA IF NOT EXISTS myschema;

DROP TABLE IF EXISTS myschema.ecomm_invoice;

CREATE TABLE myschema.ecomm_invoice (
    InvoiceNo TEXT,
    StockCode TEXT,
    "Description" TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice FLOAT,
    CustomerID TEXT,
    Country TEXT
);

COPY myschema.ecomm_invoice
FROM '/data/cleaned_data.csv' DELIMITER ',' CSV HEADER;