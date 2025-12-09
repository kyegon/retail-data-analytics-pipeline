/**SQL commands to set up database, schema, and tables 
   for the project with defined foreign keys and checks.
 > Database -DB_Ecommerce_Analytics
 > Scema - online_retail
 ---------Key tables -----------------------
 > Trasaction table - Ecom_Sales_Transaction
 > customer data - Ecom_Customer
 > Product def  - Ecom_Product **/
------------------------------------------------------------------------
--BEGIN
-- Script 1 Set up the Database...
CREATE DATABASE 
    WITH
    OWNER = postgres
    ENCODING = 'UTF8';
------------------------------------------------------------------------
-- Script 2 -Create Schema 
CREATE SCHEMA IF NOT EXISTS online_retail;
-------------------------------------------------------------------------
-- Script 3 - Create table to store customer personal data
CREATE TABLE online_retail.ecom_customer(CustomerID INTEGER PRIMARY KEY,
    Country VARCHAR(100) NOT NULL
	);

--------------------------------------------------------------------------
--Script 4- Create table to store products
CREATE TABLE online_retail.ecom_product(StockCode VARCHAR(50) PRIMARY KEY,
    Description VARCHAR(255) NOT NULL
	);
-----------------------------------------------------------------------------
--Script 5 -Create the Sales transactions table
CREATE TABLE online_retail.ecom_sales_transaction(InvoiceNo VARCHAR(50) NOT NULL,
    CustomerID INTEGER,
    StockCode VARCHAR(50) NOT NULL,
	Description VARCHAR(255) NOT NULL,
    InvoiceDate TIMESTAMP NOT NULL,
    Quantity INTEGER NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL CHECK (UnitPrice>0),
	Revenue NUMERIC(10, 4) NOT NULL,
	Cancelled CHAR(1),
	Dt_Day INTEGER,
	Dt_Month VARCHAR(10),
	Dt_Year INTEGER,
	Country VARCHAR(50) NOT NULL,
	          
    --Define composite primary key i.e., (InvoiceNo + StockCode = unique line item)
    PRIMARY KEY (InvoiceNo, StockCode),
    --Define foreign keys
    CONSTRAINT fk_customer FOREIGN KEY (CustomerID)
        REFERENCES online_retail.ecom_customer (CustomerID),
    CONSTRAINT fk_product FOREIGN KEY (StockCode)
        REFERENCES online_retail.ecom_product (StockCode)
);
-------------------------------------------------------------------------------
-- Script 6 - Create Indexes
CREATE INDEX idx_transaction_customer ON online_retail.Ecom_Sales_Transaction (CustomerID);
CREATE INDEX idx_transaction_product ON online_retail.Ecom_Sales_Transaction (StockCode);
CREATE INDEX idx_transaction_date ON online_retail.Ecom_Sales_Transaction (InvoiceDate);
--END

---	sanity checks
SELECT * FROM online_retail.ecom_sales_transaction;
SELECT * FROM online_retail.ecom_Product;
where "Dt_Year" = 2011;

--fecth sales excluding UK
SELECT "InvoiceNo","StockCode","Quantity","Revenue", "Country" FROM online_retail.ecom_sales_transaction
where "Country" <> 'United Kingdom';
---get revenue per customer
SELECT cust."CustomerID", cust."Country",txn."Revenue"
FROM online_retail.ecom_sales_transaction txn 
INNER JOIN online_retail.ecom_customer cust 
ON txn."CustomerID" = cust."CustomerID" AND txn."Country" = cust."Country";
