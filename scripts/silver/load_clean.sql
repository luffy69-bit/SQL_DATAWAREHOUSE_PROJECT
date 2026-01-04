 -- Created Procedure to load data in silver Schema
 --===========================================================
CREATE OR ALTER PROCEDURE silver.load_silver 
AS 
DECLARE  @start_time DATETIME , @end_time DATETIME 
BEGIN
		
		SET @start_time = GETDATE()
		PRINT @start_time --Print the  loading start time
						  --====================================
	 BEGIN TRY

		PRINT '==============================='
		PRINT 'TRUNCATING TABLE  silver.crm_cust_info'
		PRINT '==============================='
			TRUNCATE TABLE silver.crm_cust_info;

		PRINT '=================================================='
		PRINT 'LOADING CLEAN DATA IN  TABLE  silver.crm_prd_info '
		PRINT '=================================================='
		INSERT INTO  silver.crm_cust_info( 
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,cst_gndr,
		cst_create_date
		)
		--==========================
		--Query for cleaning data
		--==========================
		Select  cst_id,
		cst_key,
		--==================================================
		-- Used TRIM() to remov Whitespaces from the column
		--==================================================
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname)  as cst_lastname,
		 --========================================================================
		 -- CASE STATEMENT FOR REMOVING ABBREVIATIONS  FROM cst_marital_status
		 -- ========================================================================
		CASE 
			WHEN cst_marital_status = 'M' THEN 'Married'
			WHEN cst_marital_status = 'S' THEN 'Single'
			ELSE 'N/A'
		END AS cst_marial_status,

		 --========================================================================
		 -- CASE STATEMENT FOR REMOVING ABBREVIATIONS  FROM cst_gndr
		 -- ========================================================================
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
			ELSE 'N/A'
		END AS cst_gndr,
		cst_create_date
		from
		 --========================================================================
		 -- SUBQUERY FOR REMOVING Duplicate AND NULLS
		 -- ========================================================================
		(select *,
		ROW_NUMBER() OVER( PARTITION BY cst_id  order by cst_create_date desc) as flag
		FROM bronze.crm_cust_info 
		WHERE cst_id IS NOT NULL)t
		where flag = 1 ;




		PRINT '==============================='
		PRINT 'TRUNCATING TABLE  silver.crm_prd_info '
		PRINT '==============================='
			TRUNCATE TABLE silver.crm_prd_info;


		PRINT '==============================='
		PRINT 'LOADING CLEAN DATA  silver.crm_prd_info '
		PRINT '==============================='

		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm ,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		Select prd_id , 

		--================================================
		-- Replace and  extract value from the column
		--================================================

		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, 
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key ,
		prd_nm ,

		--============================
		-- Remove NULL  with a Value
		--============================

		ISNULL(prd_cost,0) as prd_cost ,

		--===========================================
		-- CASE Statement use to remove Abbreviation
		--===========================================

		CASE UPPER(TRIM(prd_line))
			WHEN 'R' THEN 'Roads'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'M' THEN 'Mountains'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		--===============================================
		-- Converting the data type in DATE USING CAST()
		-- DATEADD used to subtract 1 day from the Date
		--===============================================
		CAST(prd_start_dt AS DATE) AS prd_start_dt  ,
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		from bronze.crm_prd_info; 




	
	
	
	
	
		PRINT '==============================='
		PRINT 'TRUNCATING TABLE  silver.crm_sales_details '
		PRINT '==============================='
		TRUNCATE TABLE silver.crm_sales_details ;

		PRINT '==========================================='
		PRINT 'LOADING CLEAN DATA  silver.crm_sales_details '
		PRINT '==========================================='
		INSERT INTO silver.crm_sales_details 
		(   sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT sls_ord_num ,

		--======================================================
		--SUBSTRING used to extract a specific part from a value 
		--=======================================================

		SUBSTRING(sls_prd_key,1,7) as sls_prd_key,
		sls_cust_id,
		--===========================================================
		--CASE Statement use to convet INT into DATE using cast .
		-- Also checking if the number is suitable to convert in DATE.
		--============================================================

		CASE 
			WHEN sls_order_dt < 0 OR LEN(sls_order_dt)!= 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)  
		END AS sls_order_dt,

		CASE 
			WHEN sls_ship_dt < 0 OR LEN(sls_ship_dt)!= 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)  
		END AS sls_ship_dt,

		CASE 
			WHEN sls_due_dt < 0 OR LEN(sls_due_dt)!= 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)  
		END AS sls_due_dt,
		ISNULL(sls_sales,0) as sls_sales,
		sls_quantity,
		ABS(ISNULL(sls_sales,0) * sls_quantity) AS sls_price
		FROM  bronze.crm_sales_details;


		PRINT '=================================================='
		PRINT 'TRUNCATING  TABLE  silver.erp_loc_a101 '
		PRINT '=================================================='
			TRUNCATE TABLE  silver.erp_loc_a101;

		PRINT '=================================================='
		PRINT 'LOADING CLEAN DATA IN  TABLE  silver.erp_loc_a101 '
		PRINT '=================================================='

		INSERT INTO silver.erp_loc_a101
		(CID,
		CNTRY)
		--===============================================
		--REPLACE() use to replace a value with a value
		--===============================================

			SELECT REPLACE(CID,'-','') as CID ,
			CASE 
				WHEN UPPER(TRIM(CNTRY)) IN ('US','USA')  THEN 'United States'
				WHEN UPPER(TRIM(CNTRY)) = 'DE'  THEN 'Germany'
				WHEN TRIM(CNTRY)= '' OR TRIM(CNTRY) IS NULL THEN 'N/A'
				ELSE TRIM(CNTRY)
			END AS CNTRY FROM bronze.erp_loc_a101;
			 



		PRINT '=================================================='
		PRINT 'TRUNCATING  TABLE  silver.erp_cust_az12 '
		PRINT '=================================================='

		TRUNCATE TABLE  silver.erp_cust_az12;

		PRINT '=================================================='
		PRINT 'LOADING CLEAN DATA  silver.erp_cust_az12 '
		PRINT '==================================================' 

		INSERT INTO  silver.erp_cust_az12
		(CID ,
		BDATE,
		GEN
		)
			Select 
			CASE 
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID ,

			CASE 
				WHEN BDATE  > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
			 
			CASE 
				WHEN UPPER(TRIM(GEN)) IN ( 'F' , 'FEMALE') THEN 'Female'
				WHEN  UPPER(TRIM(GEN)) IN ( 'M' , 'MALE') THEN 'Male'
				ELSE 'N/A'
			END AS GEN
			from bronze.erp_cust_az12;


			PRINT '=================================================='
			PRINT 'TRUNCATING  TABLE  silver.erp_px_cat_g1v2 '
			PRINT '=================================================='

			TRUNCATE TABLE silver.erp_px_cat_g1v2;

			PRINT '======================================================'
			PRINT 'LOADING CLEAN  DATA IN  TABLE  silver.erp_px_cat_g1v2 '
			PRINT '======================================================'

			INSERT INTO silver.erp_px_cat_g1v2
			( ID ,
			CAT,
			SUBCAT,
			MAINTENANCE)
			Select ID , CAT , SUBCAT , MAINTENANCE FROM bronze.erp_px_cat_g1v2;
		END TRY

		BEGIN CATCH
			PRINT 'Error Message' + error_message()
			PRINT 'Error Number' + error_number()
		END CATCH
		
		SET @end_time =GETDATE()
		PRINT @end_time
		PRINT 'Load Duration ' +  CAST(DATEDIFF( second ,@Start_time,@end_time) AS NVARCHAR(10)) + ' Seconds '

END



EXEC silver.load_silver;
