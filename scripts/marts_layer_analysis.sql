-- NOTE: If your dim/fct tables don't have valid primary keys, you'll have to build surrogate keys
-- SURROGATE KEY = system-generated unique identifier assinged to each record in a table (no business meaning, just ensures uniqueness)

-- 1. dim_customer model
--- Once you have the model, double check that the joins did not create any unwanted duplicates
with joined_data AS (
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
FROM DWH.STAGING.stg_crm_cust_info ci
LEFT JOIN DWH.STAGING.stg_erp_CUST_AZ12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN DWH.STAGING.stg_erp_LOC_A101 la
    ON ci.cst_key = la.cid
)
SELECT 
    cst_id, 
    count(*) as no_entries
FROM joined_data
GROUP BY cst_id
HAVING no_entries > 1

--- we notice having 2 columns for gener, so we look more into it
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM DWH.STAGING.stg_crm_cust_info ci
LEFT JOIN DWH.STAGING.stg_erp_CUST_AZ12 ca
    ON ci.cst_key = ca.cid

--- we notice some matching, 
--- we also notice some cases where one column has an actual value while other has 'N/A' ==> we prefer the actual value
--- we also notice some straight up opposite values (Male-Female) ==> we talk to business, decide which source is more reliable (we chose the crm source)
--- we also some nulls (on gen column) due to the left join ==> we coalesce to ensure actual value
SELECT 
    COALESCE(                        -- COALESCE has order 
        NULLIF(ci.cst_gndr, 'N/A'),  -- checks if 'master' is 'N/A' -> if yes, turns into NULL; -> if value, value is kept
        NULLIF(ca.gen, 'N/A'),       -- same for 'slave'
        'N/A'
    ) AS gender
FROM DWH.STAGING.stg_crm_cust_info ci
LEFT JOIN DWH.STAGING.stg_erp_CUST_AZ12 ca
    ON ci.cst_key = ca.cid

--- i think the solution from the video tutorial has overlooked one scenario: if cst_gndr != 'N/A' will evalute to UNKNOW (not TRUE nor FALSE)
--- if cst_gndr = NULL (like, Q: Is NULL != 'N/A'? A: Unknown )

-- Run this in Snowflake to better understand:
SELECT 
    NULL != 'N/A'         AS direct_inequality_test,   -- Result is NULL (UNKNOWN)
    NULL = 'N/A'          AS direct_equality_test,     -- Result is NULL (UNKNOWN)
    NULL IS NOT NULL      AS correct_is_not_null_test, -- Result is FALSE
    NULL IS NULL          AS correct_is_null_test      -- Result is TRUE
;


--- From a modeling perspective, dim_customer should absolutely have a surrogate key as a primary key
/*
1. Data Quality Concerns: Even if cst_id doesn't have duplicates now, multiple left joins create a higher risk of potential duplicates or data issues over time

2. Complexity of Multi-Source Merging: The left joins between CRM and ERP data sources could potentially cause:
    - One-to-many relationships if there are multiple records in the joined tables
    - Duplicate customer records if business rules change
    - Data inconsistency issues when merging from multiple sources

3. Future-Proofing: Without a surrogate key, any future changes in source systems or business requirements could break referential integrity.

Recommended Strategy: 
*/

ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,  -- Surrogate key

/*
Benefits of This Approach:

   1. Guaranteed Uniqueness: The surrogate key ensures each customer record is uniquely identifiable regardless of source data issues
   2. Referential Integrity: Other dimension tables and fact tables can reliably reference this key
   3. Type 2 SCD Support: If you need to implement Slowly Changing Dimension Type 2 later (to track customer changes over time), having a surrogate key makes this much easier
   4. Performance: Integer surrogate keys typically perform better than character-based customer IDs in join operations
   5. Business Change Resilience: If the business changes customer ID assignment logic or merges/acquires other customer databases, your data model remains stable
*/

--- For the 'technical' date column:  I would keep the `dwh_create_date` in all mart models, both historical and non-historical
--- The storage overhead is minimal, and the benefits for data governance, troubleshooting, and auditability are significant
--- Even for models that only keep the latest data, knowing when that "latest" record was processed provides valuable context
---  You can think of it this way:
   -- For historical models: dwh_create_date supports the time-based tracking of changes
   -- For current-state models: dwh_create_date provides data lineage and freshness information



-- 2. dim_product model
--- In our silver layer investigation we notice that the CRM product info table contains both present and historical data
--- This means, yet another discussion with the business side: Do they (really) need historical data or not?
--- We also noticed we need the prd_key not the actual primary key (prd_id) to join with the erp source

/* 
Scenario A: Preserve the History

For products that have a start and end date indicating different versions of the same product over time, 
we need to implement a Slowly Changing Dimension (SCD) Type 2 approach:
    -- For historical tracking of product information
    -- This approach maintains all versions of a product with their respective time periods
*/
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
    CASE 
        WHEN pn.prd_end_dt IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current,  -- Flag to identify current record
    pn.dwh_create_date
FROM DWH.STAGING.stg_crm_prd_info pn
-- This includes ALL records, both historical and current

/*
Methodology for Historical Product Dimension:
1. Preserve Full History: Include all records regardless of end date status
2. Add Current Flag: Create an `is_current` flag to identify the most recent record for each product
3. Maintain Time Context: Keep both `prd_start_dt` and `prd_end_dt` to maintain the time periods for each version


Why Preserve History?
- Business Intelligence: Track product price changes over time for proper financial reporting
- Trend Analysis: Understand how product lines have evolved
- Audit Trail: Maintain a complete record of all product information changes
- Fact Table Joins: When joining with fact tables that reference historical transactions, you can ensure proper context by matching the transaction date with the product's valid time period

Usage Pattern:
When joining this historical dimension with fact tables, you would typically:
-- Join with proper date filtering to ensure correct historical context
FROM fact_sales fs
JOIN dim_product dp 
    ON fs.prd_key = dp.prd_key
    AND fs.transaction_date >= dp.prd_start_dt
    AND (fs.transaction_date <= dp.prd_end_dt OR dp.prd_end_dt IS NULL)

This approach ensures that your fact transactions are always joined with the product definition that was valid at the time of the transaction, providing accurate historical reporting
*/



/* Scenario B: Keep only current data ==>
For this scenario, we take note of the prd_start_dt and prd_end_dt date columns (basically SCD Type 2)
We will filter for prd_end_dt = NULL (meaning it has not changed, since start, thus is current)
For better understanding go look at the records for prd_ids 215, 216 & 217
*/
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
FROM DWH.STAGING.stg_crm_prd_info pn
WHERE prd_end_dt IS NULL



/*
Scenario C: Keeping both

This approach creates two separate dimensional tables to serve different analytical needs:
1. dim_products_history - Maintains complete historical data with all versions of each product
2. dim_products - Contains only current product data for operational reporting

For the historical dimension (dim_products_history):
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
    CASE 
        WHEN pn.prd_end_dt IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current,  -- Flag to identify current record
    pn.dwh_create_date
FROM DWH.STAGING.stg_crm_prd_info pn

For the current state dimension (dim_products), derived from the historical table:
SELECT
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt
For the current state dimension (dim_products), derived from the historical table:
SELECT
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt
FROM dim_products_history
WHERE is_current = TRUE  -- or where prd_end_dt IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY prd_key ORDER BY prd_start_dt DESC) = 1  -- gets the most recent version of each product

Benefits of the Two-Table Approach:
- Separation of Concerns: Clear distinction between historical analysis needs and current operational reporting
- Performance: Current state queries run against a much smaller table without date filters
- Flexibility: Different user needs can be served by different tables
- Maintainability: Historical table remains unchanged while current view can be optimized for current-state queries
- Auditability: Full historical trail is preserved in the historical table
- Simplicity: Business users focused on current state don't have to deal with date filters
*/

--- For the surrogate key, considering we are dealing with a history table (we could see same prd_id multiple times, with differences only in start/end date)
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key

/*
Logic of choosing this wouldd be:
   1. Chronological ordering: Records are first ordered by their start date, which makes sense for historical data
   Note: Keep it ascending to follow the natural flow of time; older records get lower surrogate key numbers
   2. Grouping by product: Within each time period, products are grouped together by their key
   3. Deterministic: This creates a consistent and predictable sequence for the surrogate keys
   4. Business logic: It makes sense to process products in chronological order as they became valid
   Note: It also makes sense for time-based analysis
*/


-- 3. fct_sales model
/*
In dimension modeling:
    -- **source keys** (like sls_prd_key and sls_cust_id) come from the source system
    -- **dimension surrogate keys** (like product_key and customer_key) are what you need in a fact table for proper data warehouse design

Why this matters?
1. Referential Integrity: The fact table joins to dimensions using surrogate keys, not source system keys
2. Historical Handling: When a dimension record changes, the surrogate key remains stable while the natural key might change
3. SCD Support: Surrogate keys allow for Type 2 Slowly Changing Dimensions to work properly
4. Performance: Surrogate key (typically integers) are faster to join than potentially long natural keys
*/

-- FINAL CHECKS:
-- Foreign Key Integrity: Checking for orphan records in the fact table for customers dimension
-- The query will find sales records in the fct_sales table that don't have a corresponding match in the dim_customers table
-- If we have any then we have referential integrity issues or data quality problems and for sure join issues (the records would result in NULL customer information)
SELECT *
FROM DWH.MART.fct_sales fs
LEFT JOIN DWH.MART.dim_customers cu
    ON cu.customer_key = fs.customer_key
WHERE cu.customer_key IS NULL;

-- Foreign Key Integrity: Checking for orphan records in the fact table for products dimension
-- This query will find sales records in the fct_sales table that don't have a corresponding match in the dim_products table
-- If we have any then we have referential integrity issues or data quality problems and for sure join issues (the records would result in NULL product information)
-- NOTE: If you have results here, it's because in the video tutorial there is a missing step when creating the stg_crm_sales_details
-- you need to replace the '-' with '_' for sls_prd_key column
SELECT *
FROM DWH.MART.fct_sales fs
LEFT JOIN DWH.MART.dim_products dp
    ON dp.product_key = fs.product_key
WHERE dp.product_key IS NULL;

-- Combined Foreign Key Integrity Check: Validating joins with both dimensions
-- This query checks if there are any sales records that can't join to either customer or product dimensions
-- It will count records that have NULL values in either dimension after the joins (indicating foreign key issues)
SELECT
    COUNT(*) as orphaned_sales_records
FROM DWH.MART.fct_sales fs
LEFT JOIN DWH.MART.dim_customers cu
    ON cu.customer_key = fs.customer_key
LEFT JOIN DWH.MART.dim_products dp
    ON dp.product_key = fs.product_key
WHERE cu.customer_key IS NULL OR dp.product_key IS NULL;

/*
  If you're seeing many results, this indicates:
   - Records in the fact table that reference dimension keys that don't exist
   - Potential data loading issues where fact data was loaded before corresponding dimension data
   -- Missing data transformations in the staging (silver) layer
   - Orphaned records that should be investigated and potentially cleaned up
*/


-- 4. Reporting Layer Models
/*
The reporting layer sits on top of the mart layer and provides business-focused views that are easier for end users to consume.
As the video tutorial was 'missing' the next step, I've decided to create two reporting models that aggregate and present the data in meaningful ways:

1. rpt_sales_summary_by_customer.sql:
   - Purpose: Provides a comprehensive sales summary grouped by customer information
   - Key Metrics:
     * Total orders per customer
     * Total sales amount per customer
     * Total quantity sold per customer
     * Average order value per customer
     * Customer tenure (time between first and last order)
   - Business Value: Enables customer segmentation, identifying high-value customers, understanding customer behavior patterns, and supporting customer relationship management decisions

2. rpt_sales_performance_by_product.sql:
   - Purpose: Provides detailed sales performance metrics grouped by product information
   - Key Metrics:
     * Total orders per product
     * Total sales amount per product
     * Total quantity sold per product
     * Average sales per order per product
     * Average unit price per product
     * Total cost of goods sold per product
     * Total profit per product
     * Product lifecycle (time between first and last sale)
   - Business Value: Enables product performance analysis, identifies top-performing products, supports inventory planning, and helps with pricing strategy decisions

Benefits of the Reporting Layer:
- Business-Friendly: Simplifies complex dimensional models into intuitive business metrics
- Performance: Pre-aggregated metrics reduce query complexity and improve response times
- Consistency: Ensures consistent calculations across different reports and dashboards
- Accessibility: Makes data more accessible to business users without requiring deep technical knowledge
- Reusability: Common reporting logic is centralized and maintained in one place
- Audit Trail: Clear lineage from raw data to final business metrics
*/