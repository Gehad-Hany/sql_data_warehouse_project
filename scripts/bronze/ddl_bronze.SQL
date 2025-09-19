/********************************************************************************************
    Script:    Create Tables for Bronze Layer
    Purpose:   This script is responsible for creating all the source/raw data tables 
               that will be stored in the Bronze Layer of the Data Warehouse.
               
    Notes:
    - Bronze Layer = Raw data layer (stores data as-is from source systems).
    - Each table represents raw ingestion from different sources (CRM, ERP).
    - Existing tables with the same name will be dropped before creating new ones.
********************************************************************************************/

--------- CREATE TABLES FOR BRONZE LAYER ---------

-- CRM: Customer Information
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_data DATE
);


-- CRM: Product Information
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
  DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm  NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);


-- CRM: Sales Details
IF OBJECT_ID ('bronze.crm_sales_datails','U') IS NOT NULL
  DROP TABLE bronze.crm_sales_datails;

CREATE TABLE bronze.crm_sales_datails(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);


-- ERP: Location Data (File A101)
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
  DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);


-- ERP: Customer Data (File AZ12)
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
  DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bddate DATE,
    gen NVARCHAR(50)
);


-- ERP: Product Category Data (File G1V2)
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
  DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
