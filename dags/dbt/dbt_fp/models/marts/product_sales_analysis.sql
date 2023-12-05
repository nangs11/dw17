SELECT
  pc.name AS category_name,
  s.name AS supplier_name,
  p.name AS product_name,
  SUM(oi.amount) AS total_sold,
  AVG(p.price) AS average_price
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.id
JOIN {{ ref('stg_product_categories') }} pc ON p.category_id = pc.id
JOIN {{ ref('stg_suppliers') }} s ON p.supplier_id = s.id
GROUP BY pc.name, s.name, p.name
