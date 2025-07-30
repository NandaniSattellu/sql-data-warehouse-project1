/*
===============================================================================
📦 Stored Procedure: LoadSilverTables()
===============================================================================

🔧 Purpose:
Cleans, transforms, and loads data from the Bronze Layer into the Silver Layer 
in the `DataWarehouse` schema. It ensures that all silver tables hold 
deduplicated, standardized, and cleaned records ready for analytics/reporting.

⚙️ How it works:
- Step 1: Truncates each silver table to avoid duplication
- Step 2: Transforms and inserts data from respective bronze tables
- Step 3: Applies cleansing logic such as:
    - Handling nulls
    - Deduplicating with ROW_NUMBER
    - Standardizing gender, dates, IDs, product categories, etc.
    - Normalizing and enriching fields like `cat_id`, `prd_line`, `cntry`, etc.

🚨 Important:
- Assumes bronze tables are already loaded and accessible.
- Relies on MySQL 8+ window functions (e.g., ROW_NUMBER, LEAD).

Author        : Nandani Sattellu  
Created On    : 2025-07-17  
Last Updated  : 2025-07-30  
===============================================================================
*/

DELIMITER $$

DROP PROCEDURE IF EXISTS LoadSilverTables$$

CREATE PROCEDURE LoadSilverTables()
BEGIN

  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_crm_cust_info (Customer Master)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 1: Truncating silver_crm_cust_info...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_crm_cust_info;
  
  SELECT '📥 Inserting cleaned customer records into silver_crm_cust_info...' AS status;
  INSERT INTO DataWarehouse.silver_crm_cust_info(
    cst_id, cst_key, cst_firstname, cst_lastname, 
    cst_marital_status, cst_gndr, cst_create_date
  )
  SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
      WHEN cst_marital_status = 'M' THEN 'Married'
      WHEN cst_marital_status = 'S' THEN 'Single'
      ELSE 'n/a'
    END,
    CASE 
      WHEN cst_gndr = 'F' THEN 'Female'
      WHEN cst_gndr = 'M' THEN 'Male'
      ELSE 'n/a'
    END,
    cst_create_date
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flage_last
    FROM DataWarehouse.bronze_crm_cust_info
  ) AS Tag
  WHERE flage_last = 1;


  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_crm_prd_info (Product Master)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 2: Truncating silver_crm_prd_info...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_crm_prd_info;

  SELECT '📥 Inserting transformed product data into silver_crm_prd_info...' AS status;
  INSERT INTO DataWarehouse.silver_crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, 
    prd_line, prd_start_dt, prd_end_dt, dwh_create_date
  )
  SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Derive category from prd_key
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    CASE UPPER(TRIM(prd_line)) -- Decode product line codes
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END,
    CAST(prd_start_dt AS DATE),
    DATE_SUB(
      LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
      INTERVAL 1 DAY
    ) AS prd_end_dt,
    CURRENT_TIMESTAMP
  FROM DataWarehouse.bronze_crm_prd_info;


  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_crm_sales_details (Sales Transactions)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 3: Truncating silver_crm_sales_details...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_crm_sales_details;

  SELECT '📥 Inserting calculated sales data into silver_crm_sales_details...' AS status;
  INSERT INTO DataWarehouse.silver_crm_sales_details(
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
  )
  SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    -- Fix invalid or mismatched sales values
    CASE 
      WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
      THEN sls_quantity * ABS(sls_price)
      ELSE sls_sales
    END,
    sls_quantity,
    -- Fix invalid or zero price using fallback from sales/quantity
    CASE 
      WHEN sls_price IS NULL OR sls_price <= 0
      THEN ROUND(sls_sales / NULLIF(sls_quantity, 0), 0) 
      ELSE sls_price
    END
  FROM DataWarehouse.bronze_crm_sales_details;


  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_erp_cust_az12 (ERP Customer Demographics)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 4: Truncating silver_erp_cust_az12...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_erp_cust_az12;

  SELECT '📥 Inserting ERP customer records into silver_erp_cust_az12...' AS status;
  INSERT INTO DataWarehouse.silver_erp_cust_az12(
    cid, bdate, gen
  )
  SELECT 
    -- Clean up customer ID format
    CASE 
      WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
      ELSE cid
    END,
    -- Remove future birthdates
    CASE 
      WHEN bdate > CURRENT_DATE() THEN NULL
      ELSE bdate
    END,
    -- Normalize gender field
    CASE 
      WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
    END
  FROM DataWarehouse.bronze_erp_cust_az12;


  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_erp_loc_a101 (Location Data)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 5: Truncating silver_erp_loc_a101...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_erp_loc_a101;

  SELECT '📥 Inserting cleaned location data into silver_erp_loc_a101...' AS status;
  INSERT INTO DataWarehouse.silver_erp_loc_a101(
    cid, cntry
  )
  SELECT 
    REPLACE(cid, '-', ''), -- Remove hyphens from customer IDs
    CASE 
      WHEN TRIM(cntry) = 'DE' THEN 'Germany'
      WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
      WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
      ELSE TRIM(cntry)
    END
  FROM DataWarehouse.bronze_erp_loc_a101;


  -- ──────────────────────────────────────────────────────────────
  -- Load: silver_erp_px_cat_g1v2 (Product Categories)
  -- ──────────────────────────────────────────────────────────────
  SELECT '🔄 Step 6: Truncating silver_erp_px_cat_g1v2...' AS status;
  TRUNCATE TABLE DataWarehouse.silver_erp_px_cat_g1v2;

  SELECT '📥 Inserting ERP product category data into silver_erp_px_cat_g1v2...' AS status;
  INSERT INTO DataWarehouse.silver_erp_px_cat_g1v2 (
    id, cat, subcat, maintenance
  )
  SELECT 
    id,
    cat,
    subcat,
    maintenance
  FROM DataWarehouse.bronze_erp_px_cat_g1v2;

  -- ✅ Final completion message
  SELECT '✅ All Silver Layer tables successfully loaded and cleaned!' AS status;

END$$

DELIMITER ;
