SELECT
  id,
  order_id,
  product_id,
  amount,
  coupon_id
FROM {{ source('public', 'order_items') }}
