SELECT
    id AS category_id,
    name
FROM {{ source('public', 'product_category') }}