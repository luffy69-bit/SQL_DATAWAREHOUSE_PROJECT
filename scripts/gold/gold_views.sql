IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;

GO 

CREATE VIEW gold.dim_customers AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) customer_key,
 ci.cst_id AS customer_id ,
 ci.cst_key AS customer_number,
 ci.cst_firstname  AS First_name,
 ci.cst_lastname AS Last_name,
 CASE 
	WHEN UPPER(ci.cst_gndr)!='N/A' THEN ci.cst_gndr
	ELSE COALESCE(ca.GEN,'N/A')
 END  AS   gender,
 ca.BDATE as Birth_date,
 la.CNTRY AS Country,
 ci.cst_marital_status AS marital_status
 FROM silver.crm_cust_info ci 
 LEFT JOIN silver.erp_cust_az12 ca
 ON ci.cst_key =ca.CID
 LEFT JOIN silver.erp_loc_a101 la
 ON ci.cst_key= la.CID;
 
