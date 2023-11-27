SELECT
  id,
  "name",
  country
FROM {{ source('public', 'suppliers') }}
