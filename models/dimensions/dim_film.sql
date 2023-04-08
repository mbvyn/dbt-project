{{ config(post_hook='insert into {{this}}(film_id) VALUES (-1)') }}

WITH film AS (
    SELECT *,
           '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp AS dbt_time 
    FROM {{ source('stg', 'film') }}
),

film_category AS (
    SELECT * FROM {{ source('stg', 'film_category') }}
),

category AS (
    SELECT * FROM {{ source('stg', 'category') }}
),

language AS (
    SELECT * FROM {{ source('stg', 'language') }}
)

SELECT
  f.film_id,
  f.title,
  f.description,
  f.release_year,
  f.language_id,
  f.original_language_id,
  f.rental_duration,
  f.rental_rate,
  f.length,
  f.replacement_cost,
  f.rating,
  f.special_features,
  f.last_update,
  fc.category_id,
  c.name AS category_name,
  l.name AS language_name,
  f.dbt_time
from film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
JOIN language l ON f.language_id = l.language_id
