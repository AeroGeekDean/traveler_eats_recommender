import os, sys, argparse
import numpy as np
import graphlab as gl
import pandas as pd
import json
import random
from collections import Counter
from math import radians, sin, cos, sqrt, asin

def distance(lat1, lon1, lat2, lon2):
  '''
  Compute great circle distance between 2 lat/lon pairs using Haversine formula
  INPUT: lat1, lon1, lat2, lon2, in [degree]
  OUTPUT: distance between the points, in [km]
  '''
  R = 6372.8 # Earth radius in kilometers
  km2mi = 0.621371 # [mi]/[km]
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)
  lat1 = radians(lat1)
  lat2 = radians(lat2)
  a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
  c = 2*asin(sqrt(a))
  return R * c * km2mi

class Recommend(object):
  def __init__(self):
    self.rec_model = None           #graphlab model
    self.restaurant_by_user = None  #graphlab SFrame
    self.restaurants_df = None      #Pandas DataFrame
    self.test_user = {}             #Json dict
    self.similar_users = None       #graphlab SFrame
    self.city_latlon = {'Pittsburgh':(40.4406,-79.9959),
                        'Charlotte':(35.2271,-80.8431),
                        'Champaign':(40.1164,-88.2434),
                        'Phoenix':(33.4484,-112.0740),
                        'Las Vegas':(36.1699,-115.1398),
                        'Madison':(43.0731,-89.4012),
                        'Montreal':(45.5017,-73.5673),
                        'Waterloo':(43.4643,-80.5204),
                        'Karlsruhe':(49.0069,8.4037),
                        'Edinburgh':(55.9533,-3.1883)}
    return

  def load_data(self):
    self.load_model('models/frec_fixed_wos')
    self.load_review_data('data_analysis/user_reviews.csv')
    self.load_business_data('data_analysis/yelp_data/yelp_academic_dataset_business.json')
    return

  def load_model(self, modelname):
    print '>> Loading pre-built recommender model: {}'.format(modelname)
    self.rec_model = gl.load_model(modelname)
    return

  def load_review_data(self, filename):
    print '>> Loading review data into Graphlab...'
    cols = ['user_id', 'business_id', 'stars']
    observed_reviews = gl.SFrame.read_csv(filename, usecols=cols)

    # table of unique user-restaurant reviews
    # some users reviewed the same restaurant multiple times (up to 29x !!!)
    user_reviews = observed_reviews.groupby(
                                      ['user_id', 'business_id'],
                                      {'count':      gl.aggregate.COUNT(),
                                       'avg_rating': gl.aggregate.AVG('stars'),
                                       'ratings':    gl.aggregate.CONCAT('stars')}
                                      ).sort('count', ascending=False)
    print '-- created user_reviews table...'

    # list of DISTINCT reviewed restaurants for each user
    self.restaurant_by_user = user_reviews.groupby(
                                              'user_id',
                                              {'count'      :gl.aggregate.COUNT(),
                                               'restaurants':gl.aggregate.CONCAT('business_id','avg_rating')}
                                              ).sort('count', ascending=False)
    # Usage for later...
    # dict = restaurant_by_user.filter_by(<user_id>, 'user_id')['restaurants']
    # dict contains {restaurant_id:avg_rating}
    print '-- created restaurant_by_user table...'

    return

  def load_business_data(self, filename):
    print '>> Loading business data into Graphlab...'
    with open(filename) as js_file:
        js_list = [json.loads(js_line) for js_line in js_file]
        js_list_filtered = [js for js in js_list if 'Restaurants' in js['categories']]
        self.restaurants_df = pd.DataFrame(js_list_filtered).set_index('business_id')
    return

  def load_user_data(self, filename):
    return

  def create_test_user(self):
    print '>> Creating a test user from pool of existing users...'
    lo = 5
    hi = 10
    rbu = self.restaurant_by_user
    #randomly sample 1% of data, then take 1st element of result
    self.test_user = rbu[(lo<rbu['count']) & (rbu['count']<hi)].sample(0.01)[0]
    return self.test_user

  def get_recommendations(self, my_loc, dist=50, user_id=None):
    rec_restaurants = {}
    # loop thru in reversed order (least similar to most similar)
    # so we capture biz rating of most similar in the dict
    for similar_user_id in list(self.find_similar_users(user_id)['similar'])[::-1]:
      biz_rating_dict = self.find_reviewed_businesses(similar_user_id)
      rec_restaurants.update(biz_rating_dict)

    # Now filter based on distance
    for biz_id in rec_restaurants.keys():
      biz = self.restaurants_df.ix[biz_id] # return pandas series
      if (distance(my_loc[0], my_loc[1], biz.latitude, biz.longitude)>dist):
        rec_restaurants.pop(biz_id) # remove if too far away
    return rec_restaurants

  def find_similar_users(self, user_id=None):
    print '>> Finding a list of similar users to the test user...'
    if user_id==None:
      user_id = self.test_user['user_id']
    print "test_user[{}] = {}".format(user_id, )
    num_users = 20
    self.similar_users = self.rec_model.get_similar_users([user_id],k=num_users)
    return self.similar_users

  def find_reviewed_businesses(self, user_id):
    print '>> Finding list of reviewed businesses from user: {}'.format(user_id)
    restaurants_dict = self.restaurant_by_user.filter_by(user_id, 'user_id')[0]['restaurants']
    return restaurants_dict # dict of {id:ratings}

  def find_business_info(self, biz_id):
    print '>> Finding business info...'
    return

  def print_business_info(self):
    print '>> Priting out business info...'
    return

  def use_city(self, city_name):
    latlon = self.city_latlon.get(city_name)
    return latlon


#----------------------------
# Below is standard boilerplate that calls the main() function.
def main():
  rec = Recommend()

  # rec.load_model('models/frec_fixed_wos')
  # rec.load_review_data('data_analysis/user_reviews.csv')
  # rec.load_business_data('data_analysis/yelp_data/yelp_academic_dataset_business.json')
  rec.load_data()

  rec.create_test_user()
  # rec.find_similar_users(rec.create_test_user()['user_id'])


  rec.get_recommendations(rec.find_city('Montreal'))

  return

if __name__ == '__main__':
    ''' '''
    parser = argparse.ArgumentParser(
      description='Make a restaurant recommendation based on a test user and a new locale.',)
    # parser.add_argument('json_path', type = str, help = 'Path to json data files.')
    args = parser.parse_args()
    # json_path = args.json_path

    main()
