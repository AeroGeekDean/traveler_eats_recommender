import os, sys, argparse
import numpy as np
import graphlab as gl
import pandas as pd
import random

# load reviews into df
df = pd.read_csv('data_analysis/user_reviews.csv', header=None,
                 names=['user_id', 'business_id', 'biz_name', 'stars', 'locale'])
# store the observation data in Graphlab's SFrame type
sf_obs = gl.SFrame(df[['user_id', 'business_id', 'stars']])
# number of reviews by user (388k distinct users)
reviews_by_user = sf_obs.groupby('user_id', [gl.aggregate.COUNT()]).sort('Count', ascending=False)#['Count']


# load the models
model_ws  = gl.load_model('frec_tmp_ws')
model_wos = gl.load_model('frec_tmp_wos')

while raw_input("type 's' to stop:") != 's':
  # find similar users (via cosine similiarity) to the given user
  test_user = reviews_by_user[random.randint(0,len(reviews_by_user))]
  print '---'
  print test_user
  
  simuser_ws = model_ws.get_similar_users([test_user['user_id']], k=2000)
  print 'with side data'
  print simuser_ws.tail()
  # list(simuser_ws['similar','score','rank'][0:5]) #<-- to show first 5 elements, list of dicts

  simuser_ns = model_wos.get_similar_users([test_user['user_id']], k=2000)
  print 'without side data'
  print simuser_ns.tail()





from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
'''Compute great circle distance between 2 lat/lon pairs
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