/*
===============================================================================
📦 Data Import & ETL Process Overview
===============================================================================

🗂️ Data Import:
- Method Used     : MySQL Workbench – Table Data Import Wizard
- Format          : CSV files
- Source Location : /datasets (local project directory)
- Target Tables   :
    ▸ bronze_crm_cust_info
    ▸ bronze_crm_prd_info
    ▸ bronze_crm_sales_details
    ▸ bronze_erp_loc_a101
    ▸ bronze_erp_cust_az12
    ▸ bronze_erp_px_cat_g1v2
- Note:
  All files were imported using MySQL Workbench’s built-in Import Wizard.
  Although no SQL code is auto-generated, the wizard simulates bulk insert by
  mapping CSV columns to table fields and loading in bulk.

⚙️ Stored Procedure Usage:
- Purpose         : Automate data transformation, cleaning, and quality checks.
- Where Used      : Applied after raw (bronze) data is loaded into base tables.
- Logic Included  :
    ▸ Truncation of tables before each load
    ▸ Null handling and default value assignment
    ▸ Calculated fields (e.g., sales recalculation)
    ▸ Insert into next-layer (silver) tables

🧾 Summary:
This project follows a simplified Data Warehouse pipeline approach with:
    ▸ Bronze Layer – Raw imported data
    ▸ Stored Procedures – For ETL transformation logic
    ▸ Future Layers – To be added for silver/gold reporting as needed
*/
