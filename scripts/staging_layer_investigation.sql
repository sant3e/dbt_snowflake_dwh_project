
/*
================================================
========= A. DATA INTEGRATION ANALYSIS =========
===============================================
*/

---- CRM Customer info table ----
-- We see 2 identifier (cst_id and cst_key) that can be used as primary key
-- We will use cst_id as primary key, as it is an integer and more efficient for joins
-- We will need to handle duplicates and nulls in the cst_id field


---- CRM Product info table ----
-- We see 2 identifier (prd_id and prd_key) that can be used as primary key
-- On closer look, we see multiple records for the same prd_key with different prd_id
-- Find prd_id values that map to more than one prd_key
    SELECT
        prd_id,
        COUNT(DISTINCT prd_key)               AS prd_key_count,
        MIN(prd_key)                          AS sample_prd_key
    FROM DWH.LANDING.RAW_CRM_PRD_INFO
    GROUP BY prd_id
    HAVING COUNT(DISTINCT prd_key) > 1
    ORDER BY prd_key_count DESC;

    -- Find prd_key values that map to more than one prd_id
    SELECT
        prd_key,
        COUNT(DISTINCT prd_id)                AS prd_id_count,
        MIN(prd_id)                           AS sample_prd_id
    FROM DWH.LANDING.RAW_CRM_PRD_INFO
    GROUP BY prd_key
    HAVING COUNT(DISTINCT prd_id) > 1
    ORDER BY prd_id_count DESC;

-- For prd_id: 212, 213, 214 we see the same prd_key 'AC-HE-HL-U509-R' with different prd_cost and different prd_start_dt
-- This suggest that our table is tracking historical changes to products (like SCD2) especially when we see a prd_end_dt column as well
-- We will use prd_id as primary key

-- NOTE: For now, we see that we have nothing to connect the customer info table with the product info table


---- CRM Sales info table ----
-- We see 3 identifiers (sls_ord_num, sls_cust_id and sls_prd_key)
-- sls_prd_key seem to be a good candidate to connect with the product info table
-- sls_cust_id seem to be a good candidate to connect with the customer info table
-- sls_ord_num seem to be a good candidate to be used as primary key

-- NOTE: This looks like a transactional table (event based) which can be used to connect with other tables (customer to products and orders)
-- The sls_prd_key from sales will coonect to prd_id from product info table ==> NOTE: we cannot do this, thus we will connect to prd_key instead
-- The sls_cust_id from sales will connect to cst_id from customer info table


---- ERP Customer table ----
-- We see 1 identifier (cid) that can be used as primary key
-- This contains extra information about customers that is not present in the CRM customer info table

-- NOTE: If we intend to connect this table with the CRM customer info table, we notice that cid is not exactly the same as cst_id but we can transform it
-- we can extract the 'id' part from cid and cast it to integer to match cst_id
-- NOTE: If we take a closer look at the cid values, we see that we could also extract parts of it that matches cst_key from the CRM customer info table
-- We choose this 2nd option, but that means the we need to update our model such that the customer information will include cst_key as well


---- ERP Location table ----
-- We see 1 identifier (cid) that can be used as primary key
-- This contains extra information about customer locations that is not present in the CRM customer info table
-- We will use the cid to connect with the customer information through the same cst_key as mentioned in the ERP Customer table section


---- ERP Product Categories table ----
-- We see 1 identifier (id) that can be used as primary key
-- This contains extra information about product categories that is not present in the CRM product info table
-- We will use the id to connect with the product information through the prd_key
-- NOTE: if we look at both the crm_prd_info and erp_px_cat_g1v2 tables, we see that the prd_key contains the category id as part of its value (first 5 characters)

-- Final Note: We notice that for the CRM Product info we don't need the id column as well; prd_key is sufficient as connector to other tables
-- Check the 'data_integration' png from the docs folder for a visual representation of the data model

-- ============================================================================================================================================== --


/*
==================================================
========= B. BUILDING THE 'SILVER' LAYER =========
==================================================
*/



--- Quality Check for CRM_CUST_INFO

-- 1. Investigate duplicate and null customer IDs in the DWH.LANDING.RAW_CRM_CUST_INFO table
SELECT
    cst_id,
    COUNT(*) AS total_records
FROM DWH.LANDING.RAW_CRM_CUST_INFO
GROUP BY cst_id
HAVING total_records > 1 OR cst_id IS NULL
ORDER BY total_records DESC;

--- we have both duplicates and nulls in the cst_id field ==>

-- Investigate for one of the duplicate cst_id values
SELECT *
FROM DWH.LANDING.RAW_CRM_CUST_INFO
WHERE cst_id = 29466;

--- based on findings, we will need to implement a deduplication strategy and handle null values appropriately in the staging layer
--- this is highly subjective and will require business input on how to handle these cases
--- in our case, we will keep the most recent record based on cst_create_date and remove duplicates
--- for null cst_id values, we will flag them for further review and not include them ==>

-- Deduplication straetegy based on most recent cst_create_date
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_recent
FROM DWH.LANDING.RAW_CRM_CUST_INFO
QUALIFY flag_recent = 1;

-- 2. Investigate for unwanted spaces
SELECT DISTINCT cst_firstname, 
    LENGTH(cst_firstname) AS len_firstname, 
    LENGTH(TRIM(cst_firstname)) AS len_trimmed 
FROM DWH.LANDING.RAW_CRM_CUST_INFO 
WHERE cst_firstname !=  TRIM(cst_firstname);

SELECT DISTINCT cst_lastname, 
    LENGTH(cst_lastname) AS len_lastname, 
    LENGTH(TRIM(cst_lastname)) AS len_trimmed 
FROM DWH.LANDING.RAW_CRM_CUST_INFO 
WHERE cst_lastname !=  TRIM(cst_lastname);

SELECT DISTINCT cst_gndr, 
    LENGTH(cst_gndr) AS len_gndr, 
    LENGTH(TRIM(cst_gndr)) AS len_trimmed 
FROM DWH.LANDING.RAW_CRM_CUST_INFO 
WHERE cst_gndr !=  TRIM(cst_gndr);

--- we have unwanted spaces in the cst_firstname and cst_lastname field, but not for cst_gndr ==>
--- we will need to implement a trimming strategy in the staging layer to remove leading and trailing spaces
SELECT 
    TRIM(cst_firstname), 
    TRIM(cst_lastname)
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

-- 3. Data Standardization & Consistency Checks
SELECT DISTINCT cst_gndr
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

--- we got M, F and NULL values in the cst_gndr field ==>
--- you can choose to standardize these values based on your business requirements
--- for example, you might want to convert 'M' to 'Male' and 'F' to 'Female'
-- make sure to capture lowercase values as well if any; throw in a TRIM function to be safe
--- NULL can be kept as is or replaced with 'Unknown' or 'N/A' based on business needs
SELECT 
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS cst_gndr
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

-- similar to cst_marital_status field
SELECT DISTINCT cst_marital_status
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

SELECT 
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_gndr
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

-- 4. For cst_create_date we already enforced a DATE data type in the landing layer, so no further checks needed here

-- 5. Final check: after you materialize the staging layer, run again the checks from above to ensure data quality has improved



--- Quality Check for CRM_PRD_INFO
-- 1. Investigate duplicate and null prod IDs in the DWH.LANDING.RAW_CRM_PRD_INFO table
SELECT
    prd_id,
    COUNT(*) AS total_records
FROM DWH.LANDING.RAW_CRM_PRD_INFO
GROUP BY prd_id
HAVING total_records > 1 OR prd_id IS NULL
ORDER BY total_records DESC;

--- no duplicates and nulls in the cst_id field ==> no action needed

-- 2. For the prd_key field we have a combination of information within the values:
-- category id (first 5 characters; we know this from the erp data - check id in erp_px_cat_g1v2 table) 
--     - we'll have to replace the '-' with '_' to match)
--     - we also have a category that doesn't match: CO_PE
        SELECT *,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
        FROM DWH.LANDING.RAW_CRM_PRD_INFO
        WHERE cat_id NOT IN (
            SELECT DISTINCT id 
            FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
        );
-- product key (from character 7th to end; we know this from the crm data - check sls_prd_key in the crm_sales_details table)
--     - we'll have to replace the '-' with '_' to match)
--     - we also have a some product keys that don't match: like products starting with 'FK' (probably products with no sales yet)
        SELECT *,
                REPLACE(SUBSTRING(prd_key, 7), '-', '_') AS prd_key
        FROM DWH.LANDING.RAW_CRM_PRD_INFO
        WHERE prd_key NOT IN (
            SELECT DISTINCT sls_prd_key 
            FROM DWH.LANDING.RAW_CRM_SALES_DETAILS
        );


-- 3. Investigate for unwanted spaces
SELECT DISTINCT prd_nm, 
    LENGTH(prd_nm) AS len_firstname, 
    LENGTH(TRIM(prd_nm)) AS len_trimmed 
FROM DWH.LANDING.RAW_CRM_PRD_INFO 
WHERE prd_nm !=  TRIM(prd_nm);

--- we have don't have unwanted spaces ==> no action needed


-- 4. Check for NULLs or Negative values in prd_price
SELECT prd_cost
FROM DWH.LANDING.RAW_CRM_PRD_INFO
WHERE prd_cost IS NULL OR prd_cost < 0;

---- we don't have any negative values, but we do have NULLs ==> we will need to handle NULL values appropriately in the staging layer
---- this is subjective and will require business input on how to handle these cases
---- in our case, we will replace NULLs with 0
SELECT 
    COALESCE(prd_cost, 0) AS prd_cost
FROM DWH.LANDING.RAW_CRM_PRD_INFO
ORDER BY prd_cost;


-- 5. Data Standardization & Consistency Checks
SELECT DISTINCT prd_line
FROM DWH.LANDING.RAW_CRM_PRD_INFO;

--- we got M, T, S, R and NULL values in the prd_line field ==>
--- you can choose to standardize these values based on your business requirements
--- for example, you might want to convert 'M' to 'Mountain' and 'R' to 'ROAD'
-- make sure to capture lowercase values as well if any; throw in a TRIM function to be safe
--- NULL can be kept as is or replaced with 'Unknown' or 'N/A' based on business needs
SELECT
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line
FROM DWH.LANDING.RAW_CRM_PRD_INFO;

-- 6. Check for Invalid Order Dates
SELECT *
FROM DWH.LANDING.RAW_CRM_PRD_INFO
WHERE prd_end_dt < prd_start_dt;

--- we have some invalid order dates (like prd_end_dt before prd_start_dt) ==> window functions (see more below)
--- pick a sample of these records and investigate further to understand the root cause
--- based on findings, we will need to implement a strategy in the staging layer to handle these
--- in our case, we will swap the dates, thus prd_start_dt will get the value of prd_end_dt and vice versa
--- also, for same products with multiple records, we will fix the prd_start_dt to continue from the last prd_end_dt
--- (similar to SCD2 logic)
--- Make sure to do this only for problematic records, not for all records

-- ==>
-- Using the LEAD function we can determine the prd_end_date for the same product by looking at the next record's prd_start_dt
-- We will partition by prd_key and order by prd_start_dt to ensure we are looking at the correct sequence of records for each product
-- Next, we subtract 1 day from that date to get the new prd_end_dt for the current record
-- This allows us to create a continuous timeline for each product without overlaps
-- Once we do this we will lose the time component of the datetime, so we should recast to date
SELECT *,
       (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt
FROM DWH.LANDING.RAW_CRM_PRD_INFO;

-- 7. Final check: after you materialize the staging layer, run again the checks from above to ensure data quality has improved



--- Quality Check for CRM_SALES_DETAILS
-- 1. Investigate for unwanted spaces
SELECT DISTINCT sls_ord_num, 
    LENGTH(sls_ord_num) AS len_firstname, 
    LENGTH(TRIM(sls_ord_num)) AS len_trimmed 
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS 
WHERE sls_ord_num !=  TRIM(sls_ord_num);

--- no unwanted spaces ==> no action needed

-- 2. Investigate if all sls_prd_key values exist in the product info table
--- Given that for staging layer we already made changes to stg_crm_prd_info.sql and stg_crm_prd_info.sql
--- we need to reflect on how we are going to perform this check
   SELECT *
   FROM DWH.LANDING.RAW_CRM_SALES_DETAILS lnd
   WHERE NOT EXISTS (
       SELECT 1
       FROM DWH.STAGING.STG_CRM_PRD_INFO stg
       WHERE stg.prd_key = REPLACE(lnd.sls_prd_key, '-', '_')  
   );

-- no missing sls_prd_key values, thus we can connect the sales with the product table ==> no action needed
-- reminder here: withing the reporting layer we weill have to join these tables
-- thuse we need to always check if there are any missing values in the foreign key fields adn deal with them accordingly

-- Check the same for sls_cust_id values against the customer info table
SELECT *
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS lnd
WHERE NOT EXISTS (
    SELECT 1
    FROM DWH.STAGING.STG_CRM_CUST_INFO stg
    WHERE stg.cst_id = lnd.sls_cust_id
);

-- no missing sls_cust_id values, thus we can connect the sales with the customer table ==> no action needed

-- 3. Check Invalid Dates
--- We have 3 columns representing dates but they are stored as integers
--- You can start with some regular checks like negative values or NULLs

-- Check if we have 0 (zero) values in the date columns, as those can't be converted to valid dates
SELECT
  COUNT(*) as total_rows,
  SUM(CASE WHEN sls_order_dt <= 0 THEN 1 ELSE 0 END),
  SUM(CASE WHEN sls_ship_dt  <= 0 THEN 1 ELSE 0 END),
  SUM(CASE WHEN sls_due_dt   <= 0 THEN 1 ELSE 0 END)
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS;

-- for those with 0 (zero) values we could simply replace them with NULL ==>
SELECT
    NULLIF(sls_order_dt, 0) AS sls_order_dt,
    NULLIF(sls_ship_dt, 0)  AS sls_ship_dt,
    NULLIF(sls_due_dt, 0)   AS sls_due_dt,
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS;

-- Format appears to be YYYYMMDD, meaning all values should be 8 digits long
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN LENGTH(CAST(sls_order_dt AS STRING)) = 8 THEN 1 ELSE 0 END),
    SUM(CASE WHEN LENGTH(CAST(sls_ship_dt AS STRING))  = 8 THEN 1 ELSE 0 END),
    SUM(CASE WHEN LENGTH(CAST(sls_due_dt AS STRING))   = 8 THEN 1 ELSE 0 END)
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS;

--- We notice one value for sls_order_dt that is not 8 digits long ==> we will need to handle this somehow (if few records, turn them to NULL)
--- We could also add a boundry check for our data (established with business)

-- We should also check the natural order of these dates orderb << ship << due
SELECT *
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- There are no violations of the natural order ==> no action needed

-- 4. Check the numeric fields to align with business expectations
-- We need to have sales = quantity * price
-- We also should check for negative, zero or NULL values
SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM DWH.LANDING.RAW_CRM_SALES_DETAILS
WHERE TRUE 
    AND sls_sales != (sls_quantity * sls_price)
    OR (sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL)
    OR (sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0)
ORDER BY sls_sales NULLS first, sls_quantity, sls_price;

--- we have some records that don't align with our business expectations ==> we will need to handle these based on findings - best to check with business!!!
--- Among solutions: Fix them in the source system or in the data warehouse
--- In our case we could do: 
    -- if sales is NULL or negative or 0, we can calculate (ourselves) it as quantity * price
    -- if price is NULL or 0, we can calculate (ourselves) it as sales / quantity
    -- if price is negative, we can take the absolute value
-- NOTE: The solution is more complex than anticipated, so the statging model will include CTES and a specific order of operations to ensure we get the correct results
-- I feel that the youtube video overlooked some of these complexities

-- NOTE: As stated before, some of checks should be redone after materializing the staging layer to ensure data quality has improved!!!



--- Quality Check for ERP_CUST_AZ12
-- 1. Connectivity check with CRM_CUST_INFO table
-- We stated that we will connect the tables through cst_key and cid (after some transformation) but let's check if all values exist in both tables
SELECT *
FROM DWH.LANDING.RAW_ERP_CUST_AZ12;

SELECT *
FROM DWH.LANDING.RAW_CRM_CUST_INFO;

--- we see that cst_key can be derived from cid by removing the first 3 characters of cid, but on further look we can see that not all values match this pattern
--- some values contain the cst_key without any other characters (prefix or suffix)
--- NOTE: I overcomplicated this check, on purpose, to show how a comprehensive one would look like

-- Returns: total_rows, max_len, and a single VARIANT/OBJECT column (len_counts) with keys len_1..len_N
WITH lens AS (
  SELECT LENGTH(CAST(cid AS VARCHAR)) AS len
  FROM DWH.LANDING.RAW_ERP_CUST_AZ12
),
summary AS (
  SELECT COUNT(*) AS total_rows, MAX(len) AS max_len FROM lens
),
counts AS (
  SELECT len, COUNT(*) AS cnt FROM lens GROUP BY len
)
SELECT
  s.total_rows,
  s.max_len,
  (SELECT OBJECT_AGG('len_' || len, cnt) FROM counts) AS len_counts
FROM summary s;

--- we see from the result that it supports the initial observation
-- that we have some values that requires transformation, while others match directly with cst_key ==>

-- We can use a CASE statement to handle the different scenarios
-- My solution is different as my test revealed these 2 lengths only
-- You could spin this in so many ways, but the idea is to capture all possible scenarios and handle them accordingly
SELECT
    cid,
    CASE 
        WHEN LENGTH(CAST(cid AS VARCHAR)) = 13 THEN SUBSTRING(cid, 4)   -- remove first 3 characters
        WHEN LENGTH(CAST(cid AS VARCHAR)) = 10 THEN cid                 -- keep as is
        ELSE NULL                                                       -- handle unexpected cases
    END AS derived_cid
FROM DWH.LANDING.RAW_ERP_CUST_AZ12;

-- Next, we can check if all derived_cst_key values exist in the CRM_CUST_INFO table
SELECT *
FROM (
    SELECT
        cid,
        CASE 
            WHEN LENGTH(CAST(cid AS VARCHAR)) =13 THEN SUBSTRING(cid, 4)  
            WHEN LENGTH(CAST(cid AS VARCHAR)) = 10 THEN cid                 
            ELSE NULL                                                      
        END AS derived_cst_key
    FROM DWH.LANDING.RAW_ERP_CUST_AZ12
) AS derived
WHERE TRUE 
AND derived_cst_key IS NOT NULL -- check other lengths as well (they become NULL from the CASE statement)
AND NOT EXISTS (
    SELECT 1
    FROM DWH.STAGING.STG_CRM_CUST_INFO crm
    WHERE crm.cst_key = derived.derived_cst_key
);

-- no missing values, thus we can connect the erp customer table with the crm customer table ==> only action needed is to implement the transformation logic from above in the staging layer

-- 2. Check invalit Dates
-- Check with the business on the expected date ranges (for instance customers older than 100, or younger than 18, or created in the future)
SELECT DISTINCT
    bdate
FROM DWH.LANDING.RAW_ERP_CUST_AZ12
WHERE TRUE
    AND (bdate < '1900-01-01' OR bdate > CURRENT_DATE);

-- we have some invalid birth dates ==> we will need to handle these based on findings - best to check with business!!!
-- In our case, we will only turn the future dates to NULL ==>
SELECT
    CASE 
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate
FROM DWH.LANDING.RAW_ERP_CUST_AZ12;

-- 3. Data Standardization & Consistency Checks
SELECT DISTINCT
    gen
FROM DWH.LANDING.RAW_ERP_CUST_AZ12;

-- we get values all over the place ==> we will need to standardize these values
SELECT 
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
        ELSE 'N/A'
    END AS gen
FROM DWH.LANDING.RAW_ERP_CUST_AZ12;


--- Quality Check for ERP_LOC_A101
-- 1. Connectivity check with CRM_CUST_INFO table
-- We stated that we will connect the tables through cst_key and cid (after some transformation) but let's check if all values exist in both tables
SELECT *
FROM DWH.LANDING.RAW_ERP_LOC_A101;

SELECT *
FROM DWH.STAGING.STG_CRM_CUST_INFO;

-- We can observe that the cid has an extra hypehn, between first 2 characters and the rest of the characters ==>
SELECT
    cid,
    REPLACE(cid, '-', '') AS derived_cst_key
FROM DWH.LANDING.RAW_ERP_LOC_A101;

-- Next, we can check if all derived_cst_key values exist in the CRM_CUST_INFO table
SELECT *
FROM (
    SELECT
        cid,
        REPLACE(cid, '-', '') AS derived_cst_key
    FROM DWH.LANDING.RAW_ERP_LOC_A101
) AS derived
WHERE NOT EXISTS (
    SELECT 1
    FROM DWH.STAGING.STG_CRM_CUST_INFO crm
    WHERE crm.cst_key = derived.derived_cst_key
);

-- no missing values, thus we can connect the erp location table with the crm customer table ==> only action needed is to implement the transformation logic from above in the staging layer

-- 2. Data Standardization & Consistency Checks
SELECT DISTINCT
    CNTRY
FROM DWH.LANDING.RAW_ERP_LOC_A101
ORDER BY CNTRY;

--- we have nulls (even an empty string) and different representations of country names (e.g. United State, US, USA)==> we will need to standardize these values
--- you can choose to standardize these values based on your business requirements
SELECT DISTINCT
    CASE 
        WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES OF AMERICA') THEN 'United States'
        WHEN UPPER(TRIM(CNTRY)) = 'UK' THEN 'United Kingdom'
        WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(CNTRY)) = 'FR' THEN 'France'
        WHEN CNTRY IS NULL OR UPPER(TRIM(CNTRY)) IN ('N/A', '', 'null') THEN 'N/A'
        ELSE INITCAP(TRIM(CNTRY))  -- keep as is, but trim and captialize first letter
    END AS CNTRY
FROM DWH.LANDING.RAW_ERP_LOC_A101
ORDER BY CNTRY;


--- Quality Check for ERP_PX_CAT_G1V2
-- 1. Connectivity check with CRM_PRD_INFO table
-- We stated that we will connect the tables through prd_key and id
-- if you recall though, we did create a cat_id field in the stg_crm_prd_info table, so we need to check if all values exist in both tables
SELECT
    id
FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
WHERE NOT EXISTS (
    SELECT 1
    FROM DWH.STAGING.STG_CRM_PRD_INFO crm
    WHERE crm.cat_id = id
);

-- we get CO_PD which is not present in the stg_crm_prd_info table, meaning we have a category that has no products associated with it ==> no action needed
-- a discussion with business might be needed to understand why this is the case

-- 2. Check for unwanted spaces
SELECT DISTINCT cat, 
    LENGTH(cat) AS len_firstname, 
    LENGTH(TRIM(cat)) AS len_trimmed 
FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
WHERE cat !=  TRIM(cat);

SELECT DISTINCT subcat, 
    LENGTH(subcat) AS len_firstname, 
    LENGTH(TRIM(subcat)) AS len_trimmed 
FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
WHERE subcat !=  TRIM(subcat);

SELECT DISTINCT maintenance, 
    LENGTH(maintenance) AS len_firstname, 
    LENGTH(TRIM(maintenance)) AS len_trimmed 
FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
WHERE maintenance !=  TRIM(maintenance);

--- no unwanted spaces ==> no action needed

-- 3. Data Standardization & Consistency Checks
-- check distinct for each of the 3 columns of interest, like:
SELECT DISTINCT
    cat
FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2;

--- ooorr, if you like an overkill, you can do this as well:
WITH 
DistinctCats AS (
    SELECT
        cat,
        ROW_NUMBER() OVER (ORDER BY cat) AS rn -- Assign a row number to each distinct category
    FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
    GROUP BY cat
),
DistinctSubcats AS (
    SELECT
        subcat,
        ROW_NUMBER() OVER (ORDER BY subcat) AS rn -- Assign a row number to each distinct sub-category
    FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
    GROUP BY subcat
),
DistinctMaintenance AS (
    SELECT
        maintenance,
        ROW_NUMBER() OVER (ORDER BY maintenance) AS rn -- Assign a row number to each distinct maintenance value
    FROM DWH.LANDING.RAW_ERP_PX_CAT_G1V2
    GROUP BY maintenance
)
SELECT 
    c.cat         AS CAT,
    s.subcat      AS SUBCAT,
    m.maintenance AS MAINTENANCE
FROM DistinctCats AS c
FULL OUTER JOIN DistinctSubcats AS s ON c.rn = s.rn
FULL OUTER JOIN DistinctMaintenance AS m ON COALESCE(c.rn, s.rn) = m.rn
ORDER BY COALESCE(c.rn, s.rn, m.rn);

--- either way ==> no action needed