SELECT
    sls_ord_num::VARCHAR(50) AS sls_ord_num,
    sls_prd_key::VARCHAR(255) AS sls_prd_key,
    sls_cust_id::INTEGER AS sls_cust_id,
    sls_order_dt::INTEGER AS sls_order_dt,
    sls_ship_dt::INTEGER AS sls_ship_dt,
    sls_due_dt::INTEGER AS sls_due_dt,
    sls_sales::INTEGER AS sls_sales,
    sls_quantity::INTEGER AS sls_quantity,
    sls_price::INTEGER AS sls_price
FROM {{ ref('sales_details') }}