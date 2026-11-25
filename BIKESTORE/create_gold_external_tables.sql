-- Run this in Synapse Studio > Develop > New SQL Script
-- Connect to: Built-in (serverless SQL pool)

-- 1. Create a dedicated database for your Gold layer
CREATE DATABASE GoldDB;
GO

USE GoldDB;
GO

-- 2. Create the external data source pointing at your ADLS container
-- (No explicit credential needed since Synapse's managed identity
--  already has Storage Blob Data Reader on this storage account)
CREATE EXTERNAL DATA SOURCE GoldDataSource
WITH (
    LOCATION = 'https://gamingstorageset.dfs.core.windows.net/gamingdata/Gold'
);
GO

-- 3. Define the file format (CSV with header row)
CREATE EXTERNAL FILE FORMAT GoldCsvFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2,          -- skips the header row
        USE_TYPE_DEFAULT = FALSE
    )
);
GO

-- 4. External table: Daily Sales Summary
CREATE EXTERNAL TABLE DailySalesSummary (
    sales_date      DATE,
    total_revenue   DECIMAL(18,2),
    total_orders    INT,
    total_quantity  INT
)
WITH (
    LOCATION = '/daily_sales_summary/',
    DATA_SOURCE = GoldDataSource,
    FILE_FORMAT = GoldCsvFormat
);
GO

-- 5. External table: Monthly Sales Summary
CREATE EXTERNAL TABLE MonthlySalesSummary (
    sales_month     VARCHAR(7),
    total_revenue   DECIMAL(18,2),
    total_orders    INT,
    total_quantity  INT
)
WITH (
    LOCATION = '/monthly_sales_summary/',
    DATA_SOURCE = GoldDataSource,
    FILE_FORMAT = GoldCsvFormat
);
GO

-- 6. External table: Top Products By Revenue
CREATE EXTERNAL TABLE TopProductsByRevenue (
    product_id          INT,
    product_name        VARCHAR(200),
    total_revenue       DECIMAL(18,2),
    total_quantity_sold INT
)
WITH (
    LOCATION = '/top_products_by_revenue/',
    DATA_SOURCE = GoldDataSource,
    FILE_FORMAT = GoldCsvFormat
);
GO

-- 7. External table: Customer Lifetime Value
CREATE EXTERNAL TABLE CustomerLifetimeValue (
    customer_id         INT,
    lifetime_value      DECIMAL(18,2),
    total_orders        INT,
    first_order_date    DATE,
    last_order_date     DATE,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100)
)
WITH (
    LOCATION = '/customer_lifetime_value/',
    DATA_SOURCE = GoldDataSource,
    FILE_FORMAT = GoldCsvFormat
);
GO

-- 8. Quick sanity check: confirm all 4 tables return data
SELECT TOP 5 * FROM DailySalesSummary;
SELECT TOP 5 * FROM MonthlySalesSummary;
SELECT TOP 5 * FROM TopProductsByRevenue;
SELECT TOP 5 * FROM CustomerLifetimeValue;
