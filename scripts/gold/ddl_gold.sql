/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS CUSTOMER_KEY,
	ci.cst_id AS CUSTOMER_ID,
	ci.cst_key AS CUSTOMER_NUMBER,
	ci.cst_firstname AS FIRSTNAME,
	ci.cst_lastname AS LASTNAME,
	la.CNTRY AS COUNTRY,
	CASE WHEN ci.cs_gndr!= 'n/a' THEN ci.cs_gndr
		ELSE COALESCE(ca.GENDER,'n/a')
	END AS GENDER,
	--ci.cs_gndr AS GEN,
	--ca.GENDER AS GENDR,
	ca.BDATE AS BIRTHDATE,
	ci.cst_marital_status AS MARITAL_STATUS,
	ci.cst_create_date AS CREATE_DATE
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_CUST_AZ12 AS ca
	ON ci.cst_key=ca.CID
	LEFT JOIN silver.erp_LOC_A101 AS la
	ON la.CID=ci.cst_key
GO
--========================================
--Create Dimension: gold.dim_products
--========================================

IF OBJECT_ID('gold.dim_products','V' ) IS NOT NULL
DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS PRODUCT_ID,
	pn.prd_key AS PRODUCT_NUMBER,
	pn.prd_nm AS PRODUCT_NAME,
	pn.cat_id AS CATEGORY_ID,
	pc.CAT AS CATEGORY,
	pc.SUBCAT AS SUBCATEGORY,
	pc.MAINTENANCE AS MAINTENANCE,
	pn.prd_cost AS COST,
	pn.prd_line AS PRODUCT_LINE,
	pn.prd_start_dt AS START_DATE 
	--pn.prd_dt(remove this col after removing historical data and present data's end date gets null)
	--pc.ID
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id=pc.ID
WHERE prd_end_dt IS NULL
GO

--=====================================
--Creating Fact:gold.fact_sales
--=====================================

IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS ORDER_NUMBER,
	pr.product_key AS PRODUCT_KEY,--sd.sls_prd_key
	cu.CUSTOMER_KEY AS CUSTOMER_KEY,--sd.sls_cust_id(using this because we want to join to other dims and thus here using surrogate key,process refers to data lookup)
	sd.sls_order_dt AS ORDER_DATE,
	sd.sls_ship_dt AS SHIP_DATE,
	sd.sls_due_dt AS DUE_DATE,
	sd.sls_sales AS SALES,
	sd.sls_quantity AS QUANTITY,
	sd.sls_price AS PRICE
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.CUSTOMER_ID
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.PRODUCT_NUMBER
GO
