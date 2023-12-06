SELECT
    id AS product_id,
    name,
    price,
    category_id,
    supplier_id
FROM {{ source('public', 'product') }}