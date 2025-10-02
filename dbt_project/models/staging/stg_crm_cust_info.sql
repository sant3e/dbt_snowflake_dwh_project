SELECT 
    cst_id AS cst_id,
    cst_key AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date AS cst_create_date,
    CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('raw_crm_cust_info') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) = 1