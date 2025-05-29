/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
	cst_id,
	COUNT(*)
FROM  silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL 
-- In case there is only 1 NULL


-- Check for unwanted spaces
-- Expectation: No Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data Standardization & Consistency
SELECT 
	DISTINCT cst_gndr
FROM silver.crm_cust_info


-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Exception: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0 or prd_cost IS NULL


-- Data Standardization & Consistency
SELECT 
	DISTINCT prd_line
FROM silver.crm_prd_info


-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt <prd_start_dt

SELECT * 
FROM silver.crm_prd_info



-- Check for invalid dates
SELECT 
NULLIF (sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0 
OR LEN(sls_due_dt) !=8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19900101

-- Check for invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt >sls_ship_dt OR sls_order_dt >sls_due_dt

-- Check data consistency: between sales, quantity and price
--> sales = quantity* price
--> values must not be NULL, zero or negative

SELECT
sls_sales,
sls_quantity, 
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity* sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- Identify out-of-range dates

SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- data standardization & consistency

SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM silver.erp_cust_az12

SELECT *
FROM silver.erp_cust_az12


-- data standardization & consistency
SELECT 
	DISTINCT cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' 
		 WHEN TRIM(cntry) IS NULL OR TRIM(cntry)= '' THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
FROM silver.erp_loc_a101


-- Check for unwanted spaces
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE TRIM (cat) != cat OR TRIM (subcat) != subcat OR TRIM (maintenance) != maintenance
 
 -- data standardization & consistency
SELECT DISTINCT 
subcat
FROM bronze.erp_px_cat_g1v2

