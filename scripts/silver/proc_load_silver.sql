

/*
===================================================================
Stored Procedure: load silver layer (bronze -> silver)
===================================================================
Script Purpose:
  this script performs ETL (extract, transform, load) process to 
  populate the 'silver' schema from the 'bronze' schema
Actions PerformedL
  - truncates silver tables 
  - insert transformed and cleansed data from bronze layer into silver layer 
usage:
  EXEC silver.load_silver
===================================================================
*/
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '=======================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gender,
		cst_create_date)

		SELECT 
		cst_id,
		cst_key,

		TRIM(cst_firstname) AS cst_firstname, --remove unwanted spaces 
		TRIM(cst_lastname) AS cst_lastname,

		CASE WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
			 WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_material_status,


		-- F -> female M -> male 
		CASE WHEN UPPER(cst_gender) = 'F' THEN 'Female'
			 WHEN UPPER(cst_gender) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gender,

		cst_create_date
		FROM (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info ) t
		WHERE flag_last = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- t1
		SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
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

			-- split key into category id to use as foreign key to connect tables 
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

			-- allows us to join with sales details
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,

			-- replaces NULL with 0
			ISNULL(prd_cost, 0) AS prd_cost,

	
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,

			-- fix end dates earlier than start dates 
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CAST(DATEADD(DAY, -1, LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		-- finds category id that does not match data in category table 
		/* WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
			(SELECT DISTINCT id FROM bronze.erp_PX_CATG1V2) */


		-- products that dont have any orders
		/* WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN 
			  (SELECT sls_prd_key FROM bronze.crm_sales_details) */

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		-- t2

		SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details(
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

		-- convert int to date 
		CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

 
		CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

 
		CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

 
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,

		 sls_quantity,
 
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
		END AS sls_price

		FROM bronze.crm_sales_details;

		--check for invalid dates 
		/* WHERE sls_order_dt <= 0 OR 
			  LEN(sls_order_dt) != 8 */

		/* SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt */

		-- sales = quantity * price 
		-- no negative, 0, or NULL
		-- ask experts on sources

		/*SELECT 
		sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price AS old_sls_price,

		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,

		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
		END AS sls_price

		FROM bronze.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

		ORDER BY sls_sales, sls_quantity, sls_price */

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- t3
		
		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------';

		SET @end_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		INSERT INTO silver.erp_CUST_AZ12
		(
			cid,
			bdate,
			gen
		)
		SELECT 

		-- old data has NAS at start of cid, does not match with other tables  
		CASE WHEN cid LIKE 'NAS%' 
			THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,


		CASE WHEN bdate > GETDATE() 
			THEN NULL
			ELSE bdate
		END AS bdate,


		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen

		FROM bronze.erp_CUST_AZ12;



		-- check if gender entries are correct 
		/* SELECT DISTINCT gen 
		FROM bronze.erp_CUST_AZ12 */

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- t4
		SET @end_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.erp_LOC_A101';
		TRUNCATE TABLE silver.erp_LOC_A101;
		INSERT INTO silver.erp_LOC_A101
		(	cid, 
			cntry
		)
		SELECT 
		REPLACE(cid, '-', '') cid, 

		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) in ('US' , 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry

		FROM bronze.erp_LOC_A101


		--key does not match format of crm_cust_info 

		-- check countries 
		/* SELECT DISTINCT cntry
		FROM bronze.erp_LOC_A101
		ORDER BY cntry */

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		--t5
		SET @end_time = GETDATE();
		PRINT '>> Inserting Data Into:silver.erp_PX_CATG1V2';
		TRUNCATE TABLE silver.erp_PX_CATG1V2;
		INSERT INTO silver.erp_PX_CATG1V2
		(
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
		FROM bronze.erp_PX_CATG1V2


		-- data consistency 
		/*
		SELECT DISTINCT 
		cat
		FROM bronze.erp_PX_CATG1V2

		SELECT DISTINCT 
		subcat
		FROM bronze.erp_PX_CATG1V2

		SELECT DISTINCT 
		maintenance 
		FROM bronze.erp_PX_CATG1V2
		*/
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading Silver Layer Completed';
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
GO
