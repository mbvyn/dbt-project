WITH rental_base AS (
    SELECT *,
           EXTRACT(EPOCH FROM rental_date::timestamp)                                              AS rental_epoch,
           EXTRACT(EPOCH FROM return_date::timestamp)                                              AS new_return_date,
           EXTRACT(EPOCH FROM return_date::timestamp) - EXTRACT(EPOCH FROM rental_date::timestamp) AS diff,
           (CASE WHEN return_date IS NOT NULL THEN 1 ELSE 0 END)                                   AS is_return,
           to_char(rental_date::timestamp, 'YYYYMMDD')::integer                                    AS date_key,
           '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S") }}'::timestamp                        AS dbt_time

    FROM {{ source('stg', 'rental') }}
),

inventory AS (
    SELECT * FROM {{ source('stg', 'inventory') }}
),

dim_film AS (
    SELECT * FROM {{ ref('dim_film') }}
),

dim_store AS (
    SELECT * FROM {{ ref('dim_store')}}
),

dim_staff AS (
    SELECT * FROM {{ ref('dim_staff') }}
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

rental_base_1 AS (
    SELECT rental_base.*,
           inventory.store_id,
           inventory.film_id
    FROM rental_base
    INNER JOIN inventory ON true
    AND inventory.inventory_id = rental_base.inventory_id
),

rental_base_2 AS (
    SELECT rental_base_1.*,
           (CASE WHEN dim_staff.staff_id IS NOT NULL THEN dim_staff.staff_id ELSE -1 END)             AS staff_id_rental_check,
           (CASE WHEN dim_customer.customer_id IS NOT NULL THEN dim_customer.customer_id ELSE -1 END) AS customer_id_check,
           (CASE WHEN dim_film.film_id IS NOT NULL THEN dim_film.film_id ELSE -1 END)                 AS film_id_check,
           (CASE WHEN dim_store.store_id IS NOT NULL THEN dim_store.store_id ELSE -1 END)             AS store_id_check
    FROM rental_base_1
    LEFT JOIN dim_staff ON true
    AND rental_base_1.staff_id = dim_staff.staff_id

    LEFT JOIN dim_customer ON true
    AND rental_base_1.customer_id =dim_customer.customer_id

    LEFT JOIN dim_film ON true
    AND rental_base_1.film_id = dim_film.film_id

    LEFT JOIN dim_store ON true
    AND rental_base_1.store_id = dim_store.store_id
)

SELECT rental_id,
       rental_date,
       date_key,
       inventory_id,
       customer_id_check                                              AS customer_id,
       customer_id                                                    AS customer_id_origin,
       film_id_check                                                  AS film_id,
       store_id_check                                                 AS store_id,
       staff_id_rental_check                                          AS staff_id_rental,
       new_return_date                                                AS return_date,
       CASE WHEN new_return_date IS NOT NULL THEN diff/3600 ELSE NULL END AS rental_hours,
       is_return,
       last_update,
       dbt_time
FROM rental_base_2
