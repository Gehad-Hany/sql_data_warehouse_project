/*
===============================================================================
Data Quality Checks & Transformation
===============================================================================
Purpose:
    - Perform data quality checks on Bronze layer.
    - Clean and standardize data.
    - Load cleaned data into Silver layer.

Tables Covered:
    1. crm_cust_info
    2. crm_prd_info
    3. crm_sales_details
    4. erp_cust_az12
    5. erp_loc_a101
    6. erp_px_cat_g1v2
===============================================================================
*/

/* ============================================================================
   1) crm_cust_info
============================================================================ */
-- Quality checks: duplicates, nulls, unwanted spaces, standardization
-- Final step: Insert into silver.crm_cust_info

----1)check null or dublicate in primary key
--expection : no result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;
------------------------
SELECT 
*
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;
---------------------------
----2)check unwanted spaces cst_firstname----------
--expection : no result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
---
----2)check unwanted spaces cst_lastname----------
--expection : no result
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
---
----2)check unwanted spaces cst_gndr----------
--expection : no result
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
----- then two coulmn cst_lastname,cst_lastname You need to clean them and remove the extra spaces.
SELECT 
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,
cst_material_status,
cst_gndr,
cst_create_data
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;

--------------------------
---3)data standarization & consistincey
------------1)cst_gndr
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
--then we to modify f,m for original word female,male
SELECT 
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,
cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_data
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;
-----------2)cst_marital_satus
SELECT DISTINCT cst_material_status 
FROM bronze.crm_cust_info;
--then we to modify s,m for original word single,married
SELECT 
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,
CASE 
  WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
  WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
  ELSE 'n/a'
END AS cst_marital_status, -- Normalize marital status values to readable format
CASE 
  WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
  ELSE 'n/a'
END AS cst_gndr, -- Normalize gender values to readable format
cst_create_data
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;
-------------------------------------
--------------------------------------
-----final step then load clean data in table silver
---save new result in silver table
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_data
)
SELECT 
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,
CASE 
  WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
  WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
  ELSE 'n/a'
END AS cst_marital_status, -- Normalize marital status values to readable format
CASE 
  WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
  ELSE 'n/a'
END AS cst_gndr, -- Normalize gender values to readable format
cst_create_data
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;
 

----------------check table silver in the end
SELECT * FROM silver.crm_cust_info;

/* ============================================================================
   2) crm_prd_info
============================================================================ */
-- Quality checks: duplicates, unwanted spaces, null/negative values, 
-- normalization of prd_line, fixing invalid dates
-- Final step: Insert into silver.crm_prd_info
----1)check null or dublicate in primary key
--expection : no result
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;
---there is no duplicate
------------------------
---2)check two column prd_key
---- have too much info in this coulmn then substring for this column for first string in two column
----ex:co-rf in coulmn 
----we have another problem in this coulmn we have (-) this sign in crm but erp in coulmn we have (_)this sign then we modidy this sign.
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
----------------
----3)check coulmn prd_nm there is unwanted spaces or not
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
--there is no result then the column is safe
----------------
------4)check nulls or negative numbers
---for coulmn prd_nm
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
---THERE IS NULL VALUE replace null value with 0
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
---------------------
------5)normalization & standarilization
------column prd_line
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
----replace m,r,s,t,null for word 
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
------------------------------
--6)check last two coulmn in the table
-----check for invalid date orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
-----there is many problem in data quality
---then we solve this
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
     ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE )AS prd_end_dt
FROM bronze.crm_prd_info;
--------------------------
---------final step insert into------------
INSERT INTO silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-' ,'_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
     ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE )AS prd_end_dt
FROM bronze.crm_prd_info;

/* ============================================================================
   3) crm_sales_details
============================================================================ */
-- Quality checks: unwanted spaces, foreign key consistency, 
-- invalid dates, business rules (sales = qty * price)
-- Final step: Insert into silver.crm_sales_details

-----1)check un wanted spaces in the first coulmn
SELECT 
*
FROM bronze.crm_sales_datails
WHERE sls_ord_num != TRIM(sls_ord_num);
----there is no problem
---------------------------------------------
-----2) check two key coulmn prd_key,cust_id
---check match with other table or not
SELECT 
*
FROM bronze.crm_sales_datails
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
---there is no problem and there is no problem in cst_id
------------------------------------------------
------3)check invalid date for 3 coulmn order,ship,due
-----these date are number and we can convert for date and check if num<0
SELECT 
sls_order_dt 
FROM bronze.crm_sales_datails
WHERE sls_order_dt <= 0;
----there is no negative value but there is many 0 then we replace 0 with n/a
----number in the column (year_month_day)
SELECT 
sls_order_dt 
FROM bronze.crm_sales_datails
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt>20500101
OR sls_order_dt<19000101;
---there is two number are problem we can fix this
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_datails;
-----------check ship_dt,due_dt---
SELECT 
sls_ship_dt 
FROM bronze.crm_sales_datails
WHERE sls_ship_dt  <= 0
OR LEN(sls_ship_dt ) != 8
OR sls_ship_dt>20500101
OR sls_ship_dt<19000101;
-----there is no problem in this coulmn but we modify for date(year,month,day)--
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_datails;
----------4)check invalid date orders
SELECT 
*
FROM bronze.crm_sales_datails
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
---there is no result then there is no problem
-------------------------------------
-------5)check the last three coulmn for bussiness rule
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_datails
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales,
sls_quantity,
sls_price;
----WE have many problem then we fix it
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_datails
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales,
sls_quantity,
sls_price;
----------------------------------------
-----6)final step
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_quantity,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_datails;
---------------------------------------------
--------------------------------------
-------INSERT INTO-----
INSERT INTO silver.crm_sales_datails(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price

)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_quantity,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_datails;

/* ============================================================================
   4) erp_cust_az12
============================================================================ */
-- Quality checks: fix cid prefix, out-of-range birthdates, gender normalization
-- Final step: Insert into silver.erp_cust_az12

--------------------------------------------------
----1)check the first column
---check id match id in the other column because join 
SELECT 
*
FROM bronze.erp_cust_az12;

SELECT * FROM silver.crm_cust_info;
----there is problem is not match then we fix the problem
---there is 3 character in the first we can delete this
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END AS cid,
bddate,
gen
FROM bronze.erp_cust_az12;
----------2)check second column identify out of range dates
SELECT DISTINCT 
bddate
FROM bronze.erp_cust_az12
WHERE bddate < '1924-01-01' OR bddate > GETDATE();
-----there is many problem there many dates are in future
----replace this with null value
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END AS cid,
CASE WHEN bddate > GETDATE() THEN NULL
     ELSE bddate
END AS bddate,
gen
FROM bronze.erp_cust_az12;
--------------------------------------------
---3)check data consistency & standarization
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
------we have many problem m,f,null,female,male,empty cell
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END AS cid,
CASE WHEN bddate > GETDATE() THEN NULL
     ELSE bddate
END AS bddate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;
----------------------------------------------
------insert into stepp finally
INSERT INTO silver.erp_cust_az12(
cid,
bddate,
gen
)
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END AS cid,
CASE WHEN bddate > GETDATE() THEN NULL
     ELSE bddate
END AS bddate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

/* ============================================================================
   5) erp_loc_a101
============================================================================ */
-- Quality checks: fix cid, standardize country codes/names
-- Final step: Insert into silver.erp_loc_a101

-------1)check the first coulmn
---check the id match with other id in another table cust_info
SELECT 
cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;
---there in cid - between id but not in the other table this is problem
----replace this - by nothing
SELECT 
REPLACE(cid,'-','') cid,
cntry
FROM bronze.erp_loc_a101;
----------------------------------------------
-----2)check second column 
----data standardization & consistency
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry;
-----there is many problem in this coulmn there is null,empty cell,country full name,country with no full name
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry)= 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'N/A'
     ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;
----------------------------------
-------finall step finally----
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry)= 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'N/A'
     ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

/* ============================================================================
   6) erp_px_cat_g1v2
============================================================================ */
-- Quality checks: unwanted spaces, category standardization
-- Final step: Insert into silver.erp_px_cat_g1v2
------1)check the first coulmn
-----check the id match with another id in another table
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
------------------------------------------
------exactly match there is no problem
-------2)check unwanted spaces in the last three coulmn
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);
----- perfect there is no problem
----------------------------------------------
------3)check data consistency & standarization
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;
-----there is no problem in the coulmn and the last three coulmn
--------------------------------------------------
-------4)final step insert into
INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
