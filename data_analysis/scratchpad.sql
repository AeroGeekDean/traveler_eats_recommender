# basic join of 2 tables
<<<<<<< HEAD
# output into new table
SELECT r.data->>'user_id' AS user_id,
       r.data->>'business_id' AS business_id,
       b.data->>'name' AS business_name,
       r.data->>'stars' AS stars,
       concat(b.data->>'city',', ', b.data->>'state') AS locale
INTO user_reviews
FROM reviews AS r
JOIN businesses AS b
ON r.data->>'business_id' = b.data->>'business_id'
WHERE (b.data->'categories' @> '["Restaurants"]'::jsonb);

   OR (b.data->'categories' @> '["Food"]'::jsonb);

=======
SELECT r.data->>'user_id' as user_id,
       r.data->>'business_id' as business_id,
       r.data->>'stars' as stars,
       concat(b.data->>'city',', ', b.data->>'state') as locale
FROM reviews AS r
JOIN businesses as b
ON r.data->>'business_id' = b.data->>'business_id';

# (same as above) output into new table
SELECT r.data->>'user_id' as user_id,
       r.data->>'business_id' as business_id,
       CAST(r.data->>'stars' AS int) as stars,
       concat(b.data->>'city',', ', b.data->>'state') as locale
INTO user_reviews
FROM reviews AS r
JOIN businesses as b
ON r.data->>'business_id' = b.data->>'business_id';
>>>>>>> 8ff5b4735010dfd2c2146c0abc8dadfed5f5cf25

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


<<<<<<< HEAD
# --- user centric ---
=======
>>>>>>> 8ff5b4735010dfd2c2146c0abc8dadfed5f5cf25
# top users sorted by # of locales & # of distinct biznis,
# along with # of reviews (limit to 100)
WITH tmp AS(
  SELECT user_id,
<<<<<<< HEAD
         count(user_id) AS num_reviews,
         count(DISTINCT business_id) AS num_biznis,
         count(DISTINCT locale) AS num_locale
=======
         count(user_id) as num_reviews,
         count(DISTINCT business_id) as num_biznis,
#         avg(CAST(stars AS int)) as avg_stars,
         count(DISTINCT locale) as num_locale
>>>>>>> 8ff5b4735010dfd2c2146c0abc8dadfed5f5cf25
  FROM user_reviews
  GROUP BY user_id
)
SELECT *
FROM tmp
WHERE tmp.num_locale > 1
ORDER BY tmp.num_locale DESC,
         tmp.num_biznis DESC
LIMIT 100;

<<<<<<< HEAD
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
=======

>>>>>>> 8ff5b4735010dfd2c2146c0abc8dadfed5f5cf25

