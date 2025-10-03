WITH raw_data AS (
    SELECT
        sls_ord_num,
        REPLACE(sls_prd_key, '-', '_') AS sls_prd_key,  -- Replace hyphens with underscores (missing from video; required for fct table in gold layer)
        sls_cust_id,
        -- Convert integer date to proper DATE format, handling zero values
        CASE 
            WHEN sls_order_dt IS NULL OR sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 
            THEN NULL 
            ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt IS NULL OR sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 
            THEN NULL 
            ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt IS NULL OR sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 
            THEN NULL 
            ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'YYYYMMDD')
        END AS sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    FROM {{ ref('raw_crm_sales_details') }}
),
step1_corrected_price AS (
    SELECT *,
        -- Step 1: Correct negative prices to absolute values
        CASE 
            WHEN sls_price < 0 THEN ABS(sls_price)
            ELSE sls_price
        END AS corrected_price
    FROM raw_data
),
step2_corrected_sales AS (
    SELECT *,
        -- Step 2: Calculate sales based on corrected price
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity * corrected_price) 
            THEN sls_quantity * corrected_price
            ELSE sls_sales
        END AS corrected_sales
    FROM step1_corrected_price
),
step3_final_price AS (
    SELECT *,
        -- Step 3: If corrected_price is NULL or 0, calculate it from corrected_sales and quantity
        -- This handles cases where original price was NULL or 0
        CASE
            WHEN corrected_price IS NULL OR corrected_price = 0 THEN 
                CASE 
                    WHEN sls_quantity IS NOT NULL AND sls_quantity != 0 THEN 
                        corrected_sales / sls_quantity
                    ELSE corrected_price
                END
            ELSE corrected_price
        END AS final_price
    FROM step2_corrected_sales
)
-- Final selection with all corrections applied
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    corrected_sales AS sls_sales,
    sls_quantity,
    final_price AS sls_price,
    CURRENT_TIMESTAMP() AS dwh_create_date
FROM step3_final_price