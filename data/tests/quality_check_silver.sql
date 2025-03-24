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
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
