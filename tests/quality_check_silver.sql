-- Check For nulls or duplicates in the primary key column
-- Expectation: No Results
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
WHERE cst_id IS NULL
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- check for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for nulls or negatives in the cost column
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check for Invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT *
FROM silver.crm_prd_info

--check data consistency: between sales, quantiy and price
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;



