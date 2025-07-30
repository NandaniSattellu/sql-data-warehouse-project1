/*
===============================================================================
🗂️ DataWarehouse - Database Initialization Script
===============================================================================
Author        : Nandani Sattellu
Created On    : [Insert Date]
Last Updated  : [Insert Date]
Project       : Data Warehouse Setup
Purpose       : 
    Initializes the `DataWarehouse` database if it does not already exist.
    Ensures that all subsequent scripts run within the correct database context.

Usage:
    - Execute this script before running any schema or data load scripts.
    - If the database already exists, it will not be recreated.
    - Sets the default context to `DataWarehouse`.

MySQL Version : Compatible with MySQL 8+
===============================================================================
*/

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS DataWarehouse;

-- Set the current database context
USE DataWarehouse;

-- Output message for confirmation
SELECT '✅ DataWarehouse database is ready.' AS status;
