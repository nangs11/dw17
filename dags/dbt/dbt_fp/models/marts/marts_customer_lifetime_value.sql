WITH customer_orders AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(p.price * o.amount) AS total_spending
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
    WHERE o.status = 'FINISHED'
    GROUP BY o.customer_id
)
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    co.total_orders,
    co.total_spending,
    co.total_spending / NULLIF(co.total_orders, 0) AS average_order_value,
    c.zip_code
FROM {{ ref('dim_customers') }} c
JOIN customer_orders co ON c.customer_id = co.customer_id
