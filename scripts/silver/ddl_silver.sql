/*
===============================================================================
📂 Silver Layer – Transformed Clean Data Tables
===============================================================================

🔹 Purpose: 
These tables hold cleaned, standardized, and slightly enriched versions of the
raw (bronze) data. They're structured for easier querying and are used for 
further processing or reporting in the data warehouse pipeline.

📋 Key Features:
- All tables include `dwh_create_date` to track when each record was loaded.
- Table names mirror the bronze layer, prefixed with `silver_` to maintain clarity.

🛠️ Data Cleaning Highlights:
1. Handled nulls in gender, sales, and price fields using fallback logic.
2. Standardized column names, data types, and date formats.
3. Removed duplicates using ROW_NUMBER() logic in stored procedures.
4. Normalized product data with added hierarchy (`cat_id`, `prd_key`).
5. Renamed/dropped unnecessary columns for better reporting alignment.

📌 Note:
Data was bulk loaded using MySQL Workbench Import Wizard (GUI) 
instead of SQL Server’s BULK INSERT. Stored procedures used for logic.

Author        : Nandani Sattellu  
Created On    : 2025-07-12 
Last Updated  : 2025-07-30  
===============================================================================
*/

-- =========================
-- Customer master (cleaned & standardized)
-- =========================
CREATE TABLE IF NOT EXISTS silver_crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- Product master with added hierarchy & timestamps
-- =========================
CREATE TABLE IF NOT EXISTS silver_crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(100),
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(100),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- Sales transactions with tracking
-- =========================
CREATE TABLE IF NOT EXISTS silver_crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ERP location data
-- =========================
CREATE TABLE IF NOT EXISTS silver_erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(100),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ERP customer demographics
-- =========================
CREATE TABLE IF NOT EXISTS silver_erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ERP product categories with maintenance details
-- =========================
CREATE TABLE IF NOT EXISTS silver_erp_px_cat_g1v2 (
    id VARCHAR(100),
    cat VARCHAR(100),
    subcat VARCHAR(100),
    maintenance VARCHAR(100),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
