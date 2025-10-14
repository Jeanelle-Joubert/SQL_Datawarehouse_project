/*
==================================================================================
Stored Procedure: load bronze layer (source -> bronze)
==================================================================================
Script Purpose:
  This stored procedure loads data into the bronze schema from external CSV files.
  it performs the following actions:
  - truncates the bronze tables before loading data 
  - uses the BULK INSERT command to load data from csv files to bronze tables 
Parameters: none 
Usage:
  EXEC bronze.load_bronze;

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info; -- removes all rows from table 
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info; -- removes all rows from table 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details; -- removes all rows from table 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12; -- removes all rows from table 
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101; -- removes all rows from table 
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CATG1V2; -- removes all rows from table 
		BULK INSERT bronze.erp_PX_CATG1V2
		FROM 'C:\Users\joube\Desktop\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- locks entire table while loading
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading Bronze Layer Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=======================================================';

	END TRY
	BEGIN CATCH 
		PRINT '=======================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT '=======================================================';
	END CATCH 
END
