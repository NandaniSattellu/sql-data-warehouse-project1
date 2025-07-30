/*
===============================================================================
📦 Bronze Layer - Raw Data Table Definitions
===============================================================================
Author        : Nandani Sattellu
Created On    : [2025-07-10]
Last Updated  : [2025-07-30]
Project       : Data Warehouse Setup
Purpose       : 
    This script creates the raw ingestion tables in the `bronze` layer.
    These tables store unprocessed data pulled directly from source systems.
    They serve as the foundation for further transformation into the silver 
    and gold layers.

Usage:
    - Run after initializing the `DataWarehouse` database.
    - Tables will only be created if they do not already exist.
    - No data transformations occur at this layer.

MySQL Version : Compatible with MySQL 8+
===============================================================================
*/

-- Customer master raw data
-- This table stores basic customer info like name, gender, and marital status.
CREATE TABLE IF NOT EXISTS bronze_crm_cust_info (
    cst_id INT,                                -- Unique Customer ID
    cst_key VARCHAR(50),                       -- Customer Key (from source)
    cst_firstname VARCHAR(50),                 -- First Name
    cst_lastname VARCHAR(50),                  -- Last Name
    cst_marital_status VARCHAR(50),            -- Marital Status (S, M, etc.)
    cst_gndr VARCHAR(50),                      -- Gender (M, F, etc.)
    cst_create_date DATE                       -- Customer Creation Date
);

-- Product master raw data
-- Contains basic product details such as name, cost, and category line.
CREATE TABLE IF NOT EXISTS bronze_crm_prd_info (
    prd_id INT,                                -- Unique Product ID
    prd_key VARCHAR(50),                       -- Product Key (from source)
    prd_nm VARCHAR(100),                       -- Product Name
    prd_cost INT,                              -- Cost of Product
    prd_line VARCHAR(100),                     -- Product Line/Category
    prd_start_dt DATETIME,                     -- Product Start Date
    prd_end_dt DATE                            -- Product End Date
);

-- Sales transaction raw data
-- Stores sales orders including order dates, quantities, and amounts.
CREATE TABLE IF NOT EXISTS bronze_crm_sales_details (
    sls_ord_num VARCHAR(50),                   -- Sales Order Number
    sls_prd_key VARCHAR(50),                   -- Product Key from CRM
    sls_cust_id INT,                           -- Linked Customer ID
    sls_order_dt DATE,                         -- Order Date
    sls_ship_dt DATE,                          -- Shipping Date
    sls_due_dt DATE,                           -- Due Date
    sls_sales INT,                             -- Total Sales Amount
    sls_quantity INT,                          -- Quantity Ordered
    sls_price INT                              -- Price per Unit
);

-- ERP location data (raw)
-- Maps customer or sales data to countries (from ERP system).
CREATE TABLE IF NOT EXISTS bronze_erp_loc_a101 (
    cid VARCHAR(50),                           -- Customer or Country ID
    cntry VARCHAR(100)                         -- Country Name
);

-- ERP customer demographic data (raw)
-- Stores customer's birthdate and gender (used for profiling and insights).
CREATE TABLE IF NOT EXISTS bronze_erp_cust_az12 (
    cid VARCHAR(50),                           -- Customer ID
    bdate DATE,                                -- Birthdate
    gen VARCHAR(50)                            -- Gender
);

-- ERP product category data (raw)
-- Includes product category, subcategory, and maintenance type.
CREATE TABLE IF NOT EXISTS bronze_erp_px_cat_g1v2 (
    id VARCHAR(100),                           -- Product or Category ID
    cat VARCHAR(100),                          -- Product Category
    subcat VARCHAR(100),                       -- Product Subcategory
    maintenance VARCHAR(100)                   -- Maintenance Type
);
