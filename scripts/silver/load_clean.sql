
--========================================
-- INSERTED CLEAN DATA INTO SILVER SCHEMA  
--========================================
INSERT INTO  silver.crm_cust_info( 
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,cst_gndr,
cst_create_date
)
--Query for cleaning data
Select  cst_id,
cst_key,
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
where flag = 1 

