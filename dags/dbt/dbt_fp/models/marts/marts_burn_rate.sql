SELECT
    DATE_TRUNC('day', o.created_at) AS day,
    SUM(p.price * o.amount * COALESCE(c.discount_percent, 0) / 100) AS total_discount_given
FROM {{ ref('fact_orders') }} o
JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
LEFT JOIN {{ ref('dim_coupons') }} c ON o.coupon_id = c.coupon_id
GROUP BY 1
ORDER BY day
