import os, sys, argparse
import numpy as np
import graphlab as gl
import pandas as pd
from create_sidedata import create_sidedata

def create_rec_model():
   # load observation data into Pandas DF
   df = pd.read_csv('data_analysis/user_reviews.csv', header=None,
                    names=['user_id', 'business_id', 'biz_name', 'stars', 'locale'])

   # store the observation data in Graphlab's SFrame type
   sf_obs = gl.SFrame( df[['user_id', 'business_id', 'stars']] )

   sf_itemdata = create_sidedata()

   # split the test data via Graphlab's recommender tailored splitter function
   train_set, test_set = gl.recommender.util.random_split_by_user(sf_obs,
                                                                  'user_id',
                                                                  'business_id',
                                                                  max_num_users=100)

   # create the recommender (will train during this step)
   rec = gl.recommender.factorization_recommender.create(
               sf_obs, #train_set,
               user_id='user_id',
               item_id='business_id',
               target='stars'
               , item_data=sf_itemdata
               )

   # save the model to file
   rec.save('frec_tmp')

   # To access training progress log...
   # rec.training_stats['progress'].print_rows(num_rows=100)
   
   return


# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':
    ''' '''
    parser = argparse.ArgumentParser(
            description='Create factorization recommender model from observation data, and save it as "frec_tmp".',)
    # parser.add_argument('json_path', type = str, help = 'Path to json data files.')
    args = parser.parse_args()
    # json_path = args.json_path

    create_rec_model()












