SELECT
    id AS supplier_id,
    name,
    country
FROM {{ source('public', 'suppliers') }}