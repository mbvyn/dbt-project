{{ config(post_hook='insert into {{this}}(store_id) VALUES (-1)') }}

WITH stg_store AS (
    SELECT *,
           '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp AS dbt_time 
    FROM {{ source('stg', 'store') }}
),

staff AS (
    SELECT * FROM {{ ref('dim_staff') }}
),

address AS (
    SELECT * FROM {{ source('stg', 'address') }}
),

city AS (
    SELECT * FROM {{ source('stg', 'city') }}
),

country AS (
    SELECT * FROM {{ source('stg', 'country') }}
),

stg_store_1 AS (
    SELECT stg_store.*,
    staff.first_name AS staff_first_name,
    staff.last_name AS staff_last_name
    FROM stg_store
    LEFT JOIN staff ON true
    AND stg_store.manager_staff_id = staff.staff_id
),

stg_store_2 AS (
    SELECT stg_store_1.*,
           address.address,
           city.city_id,
           city.city,
           country.country_id,
           country.country
    FROM stg_store_1
    LEFT JOIN address ON true
    AND stg_store_1.address_id = address.address_id
    LEFT JOIN city ON true
    AND address.city_id = city.city_id
    LEFT JOIN country ON true
    AND city.country_id = country.country_id
)

SELECT store_id,
       manager_staff_id
       staff_first_name,
       staff_last_name,
       address_id,
       address,
       city_id,
       city,
       country_id,
       country,
       last_update,
       dbt_time
FROM stg_store_2
