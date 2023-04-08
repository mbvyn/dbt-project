{{ config(post_hook='insert into {{this}}(customer_id) VALUES (-1)') }}

WITH customer AS (
    SELECT *
    FROM {{ source('stg', 'customer') }}
),

address AS (
    SELECT *
    FROM {{ source('stg', 'address') }}
),

city AS (
    SELECT *
    FROM {{ source('stg', 'city') }}
),

country AS (
    SELECT * 
    FROM {{ source('stg', 'country') }}
)

SELECT
    customer.customer_id::int,
    customer.store_id::int,
    customer.first_name,
    customer.last_name,
    concat(customer.first_name,' ',customer.last_name) AS full_name,
    substring(email FROM POSITION('@' IN email)+1 FOR char_length(email)-POSITION('@' IN email)) AS domain,
    customer.email,
    customer.active::int,
    customer.address_id::int,
    address.address,
    city.city_id::int,
    city.city,
    country.country_id,
    country.country,
    (CASE WHEN customer.active = 0 THEN 'no' ELSE 'yes' END)::varchar(100) AS active_desc,
    customer.create_date::timestamp,
    customer.last_update::timestamp,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S") }}'::timestamp AS dbt_time
FROM customer

LEFT JOIN address ON true
AND customer.address_id = address.address_id

LEFT JOIN city ON TRUE
AND address.city_id = city.city_id

LEFT JOIN country ON true
AND country.country_id = city.country_id
