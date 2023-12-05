SELECT
  c.id AS customer_id,
  c.first_name,
  c.last_name,
  p.name AS product_name,
  oi.amount,
  o.status,
  o.created_at
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id
JOIN {{ ref('stg_order_items') }} oi ON o.id = oi.order_id
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.id
