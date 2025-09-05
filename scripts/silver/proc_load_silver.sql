/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE SILVER.LOAD_SILVER AS
BEGIN
    DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;

BEGIN
	TRY SET @BATCH_START_TIME = GETDATE();
	PRINT '================================================';
	PRINT 'Loading Silver Layer';
	PRINT '================================================';
	PRINT '------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '------------------------------------------------';
 
	-- Loading silver.crm_cust_info
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE SILVER.CRM_CUST_INFO;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
 
	-- Insert transformed and deduplicated customer data into the silver.crm_cust_info table
	INSERT INTO SILVER.CRM_CUST_INFO (
		CST_ID,
		CST_KEY,
		CST_FIRSTNAME,
		CST_LASTNAME,
		CST_MARITAL_STATUS,
		CST_GNDR,
		CST_CREATE_DATE
	)
 -- Select and transform data from the source table
		SELECT
			CST_ID, -- Pass through customer ID without transformation
			CST_KEY, -- Pass through customer key without transformation
			TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME, -- Remove leading/trailing whitespace from first name
			TRIM(CST_LASTNAME) AS CST_LASTNAME, -- Remove leading/trailing whitespace from last name
			CASE
				WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN
					'Single' -- Convert 'S' to 'Single'
				WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN
					'Married' -- Convert 'M' to 'Married'
				ELSE
					'n/a' -- Default to 'n/a' for invalid or missing values
			END AS CST_MARITAL_STATUS, -- Normalize marital status values to readable format
			CASE
				WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN
					'Female' -- Convert 'F' to 'Female'
				WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN
					'Male' -- Convert 'M' to 'Male'
				ELSE
					'n/a' -- Default to 'n/a' for invalid or missing values
			END AS CST_GNDR, -- Normalize gender values to readable format
			CST_CREATE_DATE -- Pass through creation date without transformation
		FROM
			(
 -- Subquery to assign row numbers for deduplication
				SELECT
					*, -- Select all columns from the source table
					ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG_LAST -- Assign row number per customer, ordered by most recent creation date
				FROM
					BRONZE.CRM_CUST_INFO -- Source table in the bronze schema (raw data layer)
				WHERE
					CST_ID IS NOT NULL -- Exclude records with NULL customer IDs
			) T -- Alias the subquery as 't'
		WHERE
			FLAG_LAST = 1; -- Select only the most recent record per customer
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
 
	-- Loading silver.crm_prd_info
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE SILVER.CRM_PRD_INFO;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO SILVER.CRM_PRD_INFO (
		PRD_ID,
		CAT_ID,
		PRD_KEY,
		PRD_NM,
		PRD_COST,
		PRD_LINE,
		PRD_START_DT,
		PRD_END_DT
	)
		SELECT
			PRD_ID,
			REPLACE(SUBSTRING(PRD_KEY,
			1,
			5),
			'-',
			'_') AS CAT_ID, -- Extract category ID
			SUBSTRING(PRD_KEY,
			7,
			LEN(PRD_KEY)) AS PRD_KEY, -- Extract product key
			PRD_NM,
			ISNULL(PRD_COST,
			0) AS PRD_COST,
			CASE
				WHEN UPPER(TRIM(PRD_LINE)) = 'M' THEN
					'Mountain'
				WHEN UPPER(TRIM(PRD_LINE)) = 'R' THEN
					'Road'
				WHEN UPPER(TRIM(PRD_LINE)) = 'S' THEN
					'Other Sales'
				WHEN UPPER(TRIM(PRD_LINE)) = 'T' THEN
					'Touring'
				ELSE
					'n/a'
			END AS PRD_LINE, -- Map product line codes to descriptive values
			CAST(PRD_START_DT AS DATE) AS PRD_START_DT,
			CAST( LEAD(PRD_START_DT) OVER (PARTITION BY PRD_KEY ORDER BY PRD_START_DT) - 1 AS DATE ) AS PRD_END_DT -- Calculate end date as one day before the next start date
		FROM
			BRONZE.CRM_PRD_INFO;
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
 
	-- Loading crm_sales_details
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE SILVER.CRM_SALES_DETAILS;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO SILVER.CRM_SALES_DETAILS (
		SLS_ORD_NUM,
		SLS_PRD_KEY,
		SLS_CUST_ID,
		SLS_ORDER_DT,
		SLS_SHIP_DT,
		SLS_DUE_DT,
		SLS_SALES,
		SLS_QUANTITY,
		SLS_PRICE
	)
		SELECT
			SLS_ORD_NUM,
			SLS_PRD_KEY,
			SLS_CUST_ID,
			CASE
				WHEN SLS_ORDER_DT = 0 OR LEN(SLS_ORDER_DT) != 8 THEN
					NULL
				ELSE
					CAST(CAST(SLS_ORDER_DT AS VARCHAR) AS DATE)
			END AS SLS_ORDER_DT,
			CASE
				WHEN SLS_SHIP_DT = 0 OR LEN(SLS_SHIP_DT) != 8 THEN
					NULL
				ELSE
					CAST(CAST(SLS_SHIP_DT AS VARCHAR) AS DATE)
			END AS SLS_SHIP_DT,
			CASE
				WHEN SLS_DUE_DT = 0 OR LEN(SLS_DUE_DT) != 8 THEN
					NULL
				ELSE
					CAST(CAST(SLS_DUE_DT AS VARCHAR) AS DATE)
			END AS SLS_DUE_DT,
			CASE
				WHEN SLS_SALES IS NULL OR SLS_SALES <= 0 OR SLS_SALES != SLS_QUANTITY * ABS(SLS_PRICE)
				THEN
					SLS_QUANTITY * ABS(SLS_PRICE)
				ELSE
					SLS_SALES
			END AS SLS_SALES, -- Recalculate sales if original value is missing or incorrect
			SLS_QUANTITY,
			CASE
				WHEN SLS_PRICE IS NULL OR SLS_PRICE <= 0
				THEN
					SLS_SALES / NULLIF(SLS_QUANTITY, 0)
				ELSE
					SLS_PRICE -- Derive price if original value is invalid
			END AS SLS_PRICE
		FROM
			BRONZE.CRM_SALES_DETAILS;
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
 
	-- Loading erp_cust_az12
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE SILVER.ERP_CUST_AZ12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO SILVER.ERP_CUST_AZ12 (
		CID,
		BDATE,
		GEN
	)
		SELECT
			CASE
				WHEN CID LIKE 'NAS%' THEN
					SUBSTRING(CID, 4, LEN(CID)) -- Remove 'NAS' prefix if present
				ELSE
					CID
			END AS CID,
			CASE
				WHEN BDATE > GETDATE() THEN
					NULL
				ELSE
					BDATE
			END AS BDATE, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN
					'Female'
				WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN
					'Male'
				ELSE
					'n/a'
			END AS GEN -- Normalize gender values and handle unknown cases
		FROM
			BRONZE.ERP_CUST_AZ12;
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
	PRINT '------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '------------------------------------------------';
 
	-- Loading erp_loc_a101
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE SILVER.ERP_LOC_A101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO SILVER.ERP_LOC_A101 (
		CID,
		CNTRY
	)
		SELECT
			REPLACE(CID,
			'-',
			'') AS CID,
			CASE
				WHEN TRIM(CNTRY) = 'DE' THEN
					'Germany'
				WHEN TRIM(CNTRY) IN ('US', 'USA') THEN
					'United States'
				WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN
					'n/a'
				ELSE
					TRIM(CNTRY)
			END AS CNTRY -- Normalize and Handle missing or blank country codes
		FROM
			BRONZE.ERP_LOC_A101;
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
 
	-- Loading erp_px_cat_g1v2
	SET @START_TIME = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE SILVER.ERP_PX_CAT_G1V2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO SILVER.ERP_PX_CAT_G1V2 (
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	)
		SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM
			BRONZE.ERP_PX_CAT_G1V2;
	SET @END_TIME = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
	SET @BATCH_END_TIME = GETDATE();
	PRINT '==========================================' PRINT 'Loading Silver Layer is Completed';
	PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + ' seconds';
	PRINT '=========================================='
END TRY BEGIN CATCH PRINT '==========================================' PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER' PRINT 'Error Message' + ERROR_MESSAGE();

PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '=========================================='
END CATCH END