WITH order_details AS (
    SELECT
        o.id AS order_id,
        o.customer_id,
        o.status,
        o.created_at,
        oi.product_id,
        oi.amount,
        oi.coupon_id
    FROM {{ source('public', 'orders') }} AS o
    JOIN {{ source('public', 'order_items') }} AS oi ON o.id = oi.order_id
)
SELECT
    order_id,
    customer_id,
    status,
    created_at,
    product_id,
    amount,
    coupon_id
FROM order_details
