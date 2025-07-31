/*
===============================================================================
Gold Layer Quality Check Script
===============================================================================
Author        : [Nandani Sattellu]
Last Updated  : [Date]
Environment   : MySQL
Purpose       : 
    This script runs a series of validation checks on the Gold Layer tables to 
    ensure they meet expected data quality standards, specifically:
        - Ensuring primary key uniqueness in dimension tables.
        - Validating referential integrity between fact and dimension tables.
        - Highlighting any disconnected or orphan records.

Instructions  :
    - Execute each section independently during QA review.
    - Investigate any rows returned by the queries below.
===============================================================================
*/

-- =========================================================================
-- SECTION 1: Uniqueness Checks on Dimension Tables
-- =========================================================================

-- Check for duplicate customer surrogate keys in gold_dim_customer
SELECT 
    customer_key,
    COUNT(*) AS total_count
FROM gold_dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check for duplicate product surrogate keys in gold_dim_product
SELECT 
    product_key,
    COUNT(*) AS total_count
FROM gold_dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;

-- =========================================================================
-- SECTION 2: Referential Integrity Checks on Fact Table
-- =========================================================================

-- Identify sales records with missing customer or product links
-- These indicate referential integrity violations between fact and dimensions
SELECT 
    fs.order_number,
    fs.customer_key,
    fs.product_key,
    CASE 
        WHEN cu.customer_key IS NULL THEN 'Missing Customer'
        WHEN pr.product_key IS NULL THEN 'Missing Product'
        ELSE 'OK'
    END AS issue_detected
FROM gold_fact_sales AS fs
LEFT JOIN gold_dim_customer AS cu 
    ON fs.customer_key = cu.customer_key
LEFT JOIN gold_dim_product AS pr 
    ON fs.product_key = pr.product_key
WHERE cu.customer_key IS NULL OR pr.product_key IS NULL;

-- =========================================================================
-- SECTION 3: Optional - Check Nulls in Critical Fact Columns
-- =========================================================================

-- Ensure no important fields are missing in the fact table
SELECT *
FROM gold_fact_sales
WHERE order_number IS NULL 
   OR order_date IS NULL 
   OR sales_amount IS NULL 
   OR customer_key IS NULL 
   OR product_key IS NULL;

-- =========================================================================
-- End of Quality Checks
-- =========================================================================
