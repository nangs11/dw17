SELECT
    id AS attempt_id,
    customer_id,
    login_successful,
    attempted_at
FROM {{ source('public', 'login_attempt_history') }}
