SELECT
    prd_id::INTEGER AS prd_id,
    prd_key::VARCHAR(255) AS prd_key,
    prd_nm::VARCHAR(255) AS prd_nm,
    prd_cost::INTEGER AS prd_cost,
    prd_line::VARCHAR(100) AS prd_line,
    prd_start_dt::TIMESTAMP AS prd_start_dt,
    prd_end_dt::TIMESTAMP AS prd_end_dt
FROM {{ ref('prd_info') }}