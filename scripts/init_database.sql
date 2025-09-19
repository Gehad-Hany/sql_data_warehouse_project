/*
========================================================
        Data Warehouse Project - SQL Server Setup
========================================================

📌 Purpose:
- Set up a dedicated Data Warehouse database to store 
  and process data in multiple layers (Bronze, Silver, Gold).

📂 Layers (Schemas):
1. Bronze:
   - Raw data layer.
   - Stores data as-is from different sources without 
     any cleaning or transformation.

2. Silver:
   - Cleaned & transformed data layer.
   - Data is standardized, cleaned, and partially 
     transformed for consistency.

3. Gold:
   - Final presentation layer.
   - Data is fully processed and ready for analytics, 
     BI dashboards, and reporting.

⚙️ Steps in this script:
1. Use the [master] database.
2. Create a new database named [Datawarehouse].
3. Create 3 schemas (Bronze, Silver, Gold) inside the database.

========================================================
*/

-- Use the Master database
USE master;

-- Create a new Data Warehouse database
CREATE DATABASE Datawarehouse;

-- Create schemas for different layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
