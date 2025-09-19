/* 
   Procedure: bronze.load_bronze
   Purpose: Load raw data from CSV files into Bronze Layer tables.
   Steps:
     - Truncate existing data in Bronze tables.
     - Bulk insert data from CRM & ERP CSV sources.
     - Print load duration for each table.
     - Handle errors using TRY...CATCH.
   Tables loaded:
     CRM: crm_cust_info, crm_prd_info, crm_sales_datails
     ERP: erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2
*/
------------------------------
EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '==================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '==================================================';

		PRINT'--------------------------------------------------'
		PRINT 'LOADING CRM TABELS';
		PRINT'--------------------------------------------------'
		--insert data into tabels----
		--1) table bronze.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.crm_cust_info' ;
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> inserting data into table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
	    SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST (DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';
		-----------------
		--2) table bronze.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.crm_prd_info' ;

		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> inserting data into table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST(DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';
	
		-----------------
		--3) table bronze.crm_sales_datails
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.crm_sales_datails' ;

		TRUNCATE TABLE bronze.crm_sales_datails;
		PRINT '>> inserting data into table: bronze.crm_sales_datails';

		BULK INSERT bronze.crm_sales_datails
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST(DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';

	-------------------------------------
		PRINT'--------------------------------------------------'
		PRINT 'LOADING ERP TABELS';
		PRINT'--------------------------------------------------'
		-----------------
		--4) table bronze.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.erp_loc_a101' ;

		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT '>> inserting data into table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
	    SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST(DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';
		---------------
		--5) bronze.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.erp_cust_az12' ;

		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT '>> inserting data into table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST(DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';
		---------------
		--6) table bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> turncating table: bronze.erp_px_cat_g1v2' ;
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> inserting data into table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\gehad\OneDrive\Desktop\projects\project1_datawarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR= ',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> load duration' + CAST(DATEdIFF(second,@start_time,@end_time) AS NVARCHAR);
		print '>>---------------------';
		---------------------------------
	END TRY
	BEGIN CATCH
	 PRINT '=============================';
	 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	 PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	 PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	 PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);

	 PRINT '==============================';

	END CATCH
END
