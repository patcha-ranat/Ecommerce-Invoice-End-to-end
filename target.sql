DROP TABLE IF EXISTS ecomm_invoice_target;

CREATE TABLE ecomm_invoice_target (
    InvoiceNo TEXT,
    StockCode TEXT,
    "Description" TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice FLOAT,
    CustomerID INT,
    Country TEXT,
    total_spend FLOAT
);