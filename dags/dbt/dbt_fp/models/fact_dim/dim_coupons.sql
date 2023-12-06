SELECT
    id AS coupon_id,
    discount_percent
FROM {{ source('public', 'coupons') }}
