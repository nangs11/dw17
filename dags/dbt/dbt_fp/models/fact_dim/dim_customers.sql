SELECT
    id AS customer_id,
    first_name,
    last_name,
    gender,
    address,
    zip_code
FROM {{ source('public', 'customers') }}
