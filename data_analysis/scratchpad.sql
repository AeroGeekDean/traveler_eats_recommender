# basic join of 2 tables
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


# top users sorted by # of locales & # of distinct biznis,
# along with # of reviews (limit to 100)
WITH tmp AS(
  SELECT user_id,
         count(user_id) as num_reviews,
         count(DISTINCT business_id) as num_biznis,
#         avg(CAST(stars AS int)) as avg_stars,
         count(DISTINCT locale) as num_locale
  FROM user_reviews
  GROUP BY user_id
)
SELECT *
FROM tmp
WHERE tmp.num_locale > 1
ORDER BY tmp.num_locale DESC,
         tmp.num_biznis DESC
LIMIT 100;



