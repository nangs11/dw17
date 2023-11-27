SELECT
  id,
  customer_id,
  status,
  created_at
FROM {{ source('public', 'orders') }}
