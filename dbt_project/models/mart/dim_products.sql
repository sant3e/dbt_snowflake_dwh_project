SELECT *
FROM {{ ref('dim_products_history') }}
WHERE is_current = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_number ORDER BY start_date DESC) = 1