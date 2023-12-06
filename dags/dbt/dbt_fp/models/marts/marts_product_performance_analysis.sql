WITH product_sales AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        p.category_id,
        c.name AS category_name,
        COUNT(DISTINCT o.order_id) AS number_of_orders,
        SUM(o.amount) AS total_units_sold,
        SUM(p.price * o.amount) AS total_sales
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
    JOIN {{ ref('dim_categories') }} c ON p.category_id = c.category_id
    WHERE o.status = 'FINISHED'
    GROUP BY p.product_id, p.name, p.category_id, c.name
)
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    number_of_orders,
    total_units_sold,
    total_sales,
    total_sales / NULLIF(total_units_sold, 0) AS average_price_per_unit
FROM product_sales
