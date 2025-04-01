CREATE SCHEMA gold;


------------------------
-- CREATE VIEW CALENDAR
------------------------
CREATE VIEW gold.calendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Calendar/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW CUSTOMERS
------------------------
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Customers/',
            FORMAT = 'PARQUET'
        ) as QUER1



------------------------
-- CREATE VIEW PRODUCTS
------------------------
CREATE VIEW gold.products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Products/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW RETURNS
------------------------
CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Returns/',
            FORMAT = 'PARQUET'
        ) as QUER1
        

------------------------
-- CREATE VIEW SALES
------------------------
CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Sales/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW SUBCAT
------------------------
CREATE VIEW gold.subcat
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Subcategories/',
            FORMAT = 'PARQUET'
        ) as QUER1



------------------------
-- CREATE VIEW TERRITORIES
------------------------
CREATE VIEW gold.territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://datastorageaw.blob.core.windows.net/silver/AdventureWorks_Territories/',
            FORMAT = 'PARQUET'
        ) as QUER1

CREATE DATABASE SCOPED CREDENTIAL cred_dav
WITH
IDENTITY = 'Managed Identity'



CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION ='https://datastorageaw.blob.core.windows.net/silver',
    CREDENTIAL = cred_dav
)


CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION ='https://datastorageaw.blob.core.windows.net/gold',
    CREDENTIAL = cred_dav
)


CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

------------------------
--CREATE EXTERNAL TABLE EXTSALES
-----------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION ='extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.sales

------------------------
--CREATE EXTERNAL TABLE CUSTOMERS
-----------------------

CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION ='extcustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.customers

------------------------
--CREATE EXTERNAL TABLE PRODUCTS
-----------------------

CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION ='extproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.products


------------------------
--CREATE EXTERNAL TABLE CALENDAR
-----------------------

CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION ='extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.calendar


------------------------
--CREATE EXTERNAL TABLE RETURNS
-----------------------

CREATE EXTERNAL TABLE gold.extreturn
WITH
(
    LOCATION ='extreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.returns


------------------------
--CREATE EXTERNAL TABLE SUBCAT
-----------------------

CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION ='extsubcat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.subcat


------------------------
--CREATE EXTERNAL TABLE TERRITORIES
-----------------------

CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION ='extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet        
)
AS
SELECT * FROM gold.territories
