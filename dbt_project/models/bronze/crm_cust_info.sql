SELECT
    cst_id::INTEGER AS cst_id,
    cst_key::VARCHAR(255) AS cst_key,
    cst_firstname::VARCHAR(100) AS cst_firstname,
    cst_lastname::VARCHAR(100) AS cst_lastname,
    cst_marital_status::VARCHAR(20) AS cst_marital_status,
    cst_gndr::VARCHAR(10) AS cst_gndr,
    cst_create_date::DATE AS cst_create_date
FROM {{ ref('cust_info') }}