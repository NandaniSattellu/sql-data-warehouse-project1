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
CREATE TABLE IF NOT EXISTS bronze_crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- Product master raw data
CREATE TABLE IF NOT EXISTS bronze_crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(100),
    prd_start_dt DATETIME,
    prd_end_dt DATE
);

-- Sales transaction raw data
CREATE TABLE IF NOT EXISTS bronze_crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- ERP location data (raw)
CREATE TABLE IF NOT EXISTS bronze_erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(100)
);

-- ERP customer demographic data (raw)
CREATE TABLE IF NOT EXISTS bronze_erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

-- ERP product category data (raw)
CREATE TABLE IF NOT EXISTS bronze_erp_px_cat_g1v2 (
    id VARCHAR(100),
    cat VARCHAR(100),
    subcat VARCHAR(100),
    maintenance VARCHAR(100)
);
