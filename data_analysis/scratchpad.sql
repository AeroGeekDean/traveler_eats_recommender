# basic join of 2 tables
# output into new table
SELECT r.data->>'user_id' AS user_id,
       r.data->>'business_id' AS business_id,
       b.data->>'name' AS business_name,
       r.data->>'stars' AS stars,
       concat(b.data->>'city',', ', b.data->>'state') AS locale
      INTO user_reviews
FROM reviews AS r JOIN businesses AS b
ON r.data->>'business_id' = b.data->>'business_id'
WHERE (b.data->'categories' @> '["Restaurants"]'::jsonb);

   OR (b.data->'categories' @> '["Food"]'::jsonb);

#-------------
# Once a new (user_reviews) table has been created...

# num of multi-locale reviewed USERS count
WITH tmp AS (
  SELECT count(DISTINCT locale) AS num_locale
  FROM user_reviews
  GROUP BY user_id
)
SELECT count(*)
FROM tmp
WHERE tmp.num_locale > 1;
#  count
#--------
# 129705
#(1 row)


# --- user centric ---
# top users sorted by # of locales & # of distinct biznis,
# along with # of reviews (limit to 100)
WITH tmp AS(
  SELECT user_id,
         count(user_id) AS num_reviews,
         count(DISTINCT business_id) AS num_biznis,
         count(DISTINCT locale) AS num_locale
  FROM user_reviews
  GROUP BY user_id
)
SELECT *
FROM tmp
WHERE tmp.num_locale > 1
ORDER BY tmp.num_locale DESC,
         tmp.num_biznis DESC
LIMIT 100;

#--- business centric ---
# num of businesses reviewed by multi-distance USERS
WITH tmp AS(
  SELECT count(DISTINCT user_id) AS num_users
  FROM user_reviews
  GROUP BY business_id
)
SELECT count(*)
FROM tmp
WHERE tmp.num_users > 1;
# result = 76274


# top biznis sorted by locales & # of distinct reviewed users,
# along with # of reviews (limit to 100)
WITH tmp AS(
  SELECT business_id,
         count(business_id) AS num_reviews,
         count(DISTINCT user_id) AS num_users,
         avg(CAST(stars AS int)) AS avg_stars,
         max(locale) AS locale1
  FROM user_reviews
  GROUP BY business_id
)
SELECT *
FROM tmp
WHERE tmp.num_reviews > 1
ORDER BY tmp.num_reviews DESC,
         tmp.locale1 DESC
LIMIT 100;


SELECT COUNT(*)
FROM user_reviews AS ur
  JOIN businesses AS b
  ON ur.business_id = b.data->>'business_id'
WHERE (b.data->'categories' @> '["Restaurants"]'::jsonb);


# ==== reviewed restaurant mapping ====
WITH ur AS (SELECT DISTINCT business_id FROM user_reviews)
SELECT b.name,
       b.biz_id,
       cast(b.data->>'stars' as numeric) AS stars,
       b.data AS json_data,
       cast(b.data->>'review_count' as numeric) AS review_count,
       round(cast(b.data->>'latitude' as numeric), 6) AS latitude,
       round(cast(b.data->>'longitude' as numeric), 6) as longitude
  INTO reviewed_restaurants
FROM businesses AS b JOIN ur
  ON ur.business_id = b.biz_id;
ORDER BY review_count DESC;
LIMIT 5;

# to save from PSQL:
copy reviewed_restaurants to '<path>/reviewed_restaurants.csv' delimiter ',' CSV HEADER;
#=======================================


# query on CartoDB...
WITH tmp AS
(SELECT business_id
 FROM reviews
 WHERE user_id = 'b0jSP2vzSCJAvVlg_vxwtQ'
)
SELECT *
FROM reviewed_restaurants AS r
JOIN tmp
ON r.biz_id = tmp.business_id