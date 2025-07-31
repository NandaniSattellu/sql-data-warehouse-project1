-- =====================================================================
-- Database: DataWarehouse
-- Views: gold_dim_customer, gold_dim_product, gold_fact_sales
-- Description: Creating Gold Layer views for Customer, Product, and Sales Fact
-- Author: [Nandani Sattellu]
-- Date: [30-07-2025 ]
-- =====================================================================

-- Switch to the appropriate database
USE DataWarehouse;

-- ===============================================================
-- View: gold_dim_customer
-- Description: Dimension table for customer information combining CRM and ERP data
-- ===============================================================
CREATE VIEW gold_dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate primary key
    ci.cst_id AS customer_id,                             -- Original customer ID from CRM
    ci.cst_key AS customer_number,                        -- Internal customer number
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,                                  -- Country from location table
    ci.cst_marital_status AS marital_status,
    -- Gender from CRM if available, else fallback to ERP gender data
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,                                -- Date of birth from ERP
    ci.cst_create_date AS create_date                     -- Customer creation date
FROM DataWarehouse.silver_crm_cust_info AS ci
LEFT JOIN DataWarehouse.silver_erp_cust_az12 AS ca 
    ON ci.cst_key = ca.cid
LEFT JOIN DataWarehouse.silver_erp_loc_a101 AS la 
    ON ci.cst_key = la.cid;

-- ===============================================================
-- View: gold_dim_product
-- Description: Dimension table for product details enriched with category information
-- ===============================================================
CREATE VIEW gold_dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate primary key
    pn.prd_id AS product_id,                           -- Product ID
    pn.prd_key AS product_number,                      -- Internal product key
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,                          -- Foreign key to category
    pc.cat AS category,                                -- Category name
    pc.subcat AS subcategory,                          -- Sub-category name
    pc.maintenance,                                    -- Maintenance flag/info
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date                      -- Product launch/start date
FROM DataWarehouse.silver_crm_prd_info AS pn
LEFT JOIN DataWarehouse.silver_erp_px_cat_g1v2 AS pc 
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;                              -- Only include active products

-- ===============================================================
-- View: gold_fact_sales
-- Description: Fact table for sales transactions joined with customer and product dimensions
-- ===============================================================
CREATE VIEW gold_fact_sales AS
SELECT   
    sd.sls_ord_num AS order_number,                    -- Sales order number
    pr.product_key,                                    -- Surrogate product key from dim_product
    cu.customer_key,                                   -- Surrogate customer key from dim_customer
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,                      -- Total sales value
    sd.sls_quantity AS quailty,                        -- Quantity sold
    sd.sls_price                                        -- Unit price
FROM DataWarehouse.silver_crm_sales_details AS sd
LEFT JOIN gold_dim_product AS pr 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customer AS cu 
    ON sd.sls_cust_id = cu.customer_id;

-- End of script
