SELECT
    CASE 
        WHEN LENGTH(CAST(CID AS VARCHAR)) = 13 THEN SUBSTRING(CID, 4)   -- remove first 3 characters
        WHEN LENGTH(CAST(CID AS VARCHAR)) = 10 THEN CID                 -- keep as is
        ELSE NULL                                                       -- handle unexpected cases
    END AS CID,
    CASE 
        WHEN BDATE > CURRENT_DATE THEN NULL
        ELSE BDATE
    END AS BDATE,
    CASE 
        WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS GEN,
    CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('raw_erp_CUST_AZ12') }}