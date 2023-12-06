WITH customer_category_spending AS (
    SELECT
        c.customer_id,
        p.category_id,
        cat.name AS category_name,
        SUM(p.price * o.amount) AS total_spending_in_category
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
    JOIN {{ ref('dim_categories') }} cat ON p.category_id = cat.category_id
    WHERE o.status = 'FINISHED'
    GROUP BY c.customer_id, p.category_id, cat.name
)
SELECT
    customer_id,
    category_id,
    category_name,
    total_spending_in_category
FROM customer_category_spending
