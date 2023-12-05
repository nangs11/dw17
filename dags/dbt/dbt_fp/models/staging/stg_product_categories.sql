SELECT
  id,
  "name"
FROM {{ source('public', 'product_category') }}
