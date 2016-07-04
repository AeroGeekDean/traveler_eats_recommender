import os, sys, argparse
import numpy as np
import graphlab as gl
import pandas as pd
import random

# load reviews into df
print 'loading reviews into Pandas df...'
df = pd.read_csv('data_analysis/user_reviews.csv', header=None,
                 names=['user_id', 'business_id', 'business_name', 'stars', 'locale'])
# store the observation data in Graphlab's SFrame type
sf_obs = gl.SFrame(df[['user_id', 'business_id', 'stars']])

print 'creating groupby tables...'
# number of reviews by user (388k distinct users)
sf_reviews_by_user = sf_obs.groupby('user_id', [gl.aggregate.COUNT()]) \
                           .sort('Count', ascending=False)
print 'created sf_reviews_by_user...'
# table of unique user-restaurant pair data
# some users reviewed SAME restaurant multiple times!!!
sf_unique_user_items = sf_obs.groupby(['user_id', 'business_id'],
                                      {'count':      gl.aggregate.COUNT(),
                                       'avg_rating': gl.aggregate.AVG('stars'),
                                       'ratings':    gl.aggregate.CONCAT('stars')}) \
                             .sort('count', ascending=False)
print 'created sf_unique_user_items...'

# Distinct reviewed restaurants for each user
sf_restaurants_by_user = sf_unique_user_items.groupby('user_id',
                                                      {'count'        :gl.aggregate.COUNT(),
                                                       'restaurants'  :gl.aggregate.CONCAT('business_id','avg_rating')}) \
                                             .sort('count', ascending=False)
# Usage for later...
# dict = sf_restaurants_by_user.filter_by(<user_id>, 'user_id')['restaurants]
# dict contains {restaurant_id:avg_rating}
print 'created sf_restaurants_by_user...'

# load the models
print 'loading models...'
model_ws  = gl.load_model('models/frec_fixed_ws')
model_wos = gl.load_model('models/frec_fixed_wos')

threshold_lo = 5
threshold_hi = 10

while raw_input("type 's' to stop:") != 's':
  # find similar users (via cosine similiarity) to the given user
  # test_user = sf_restaurants_by_user[sf_restaurants_by_user['count']>threshold].sample(0.01)[0:1]
  test_user = sf_restaurants_by_user[(threshold_lo<sf_restaurants_by_user['count']) &
                                     (sf_restaurants_by_user['count']<threshold_hi)] \
                                    .sample(0.01)[0:1]


  print '---'
  print test_user
  
  simuser_ws = model_ws.get_similar_users([test_user[0]['user_id']], k=2000)
  print 'with side data'
  print simuser_ws.head()
  # list(simuser_ws['similar','score','rank'][0:5]) #<-- to show first 5 elements, list of dicts

  simuser_ns = model_wos.get_similar_users([test_user[0]['user_id']], k=2000)
  print 'without side data'
  print simuser_ns.head()





from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
  '''
  Compute great circle distance between 2 lat/lon pairs
  INPUT: lat1, lon1, lat2, lon2, in [degree]
  OUTPUT: distance between the points, in [km]
  '''
  R = 6372.8 # Earth radius in kilometers
 
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)
  lat1 = radians(lat1)
  lat2 = radians(lat2)
 
  a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
  c = 2*asin(sqrt(a))
 
 # 1 km = 0.621371 mi or 1 mi = 1.61 km
  return R * c