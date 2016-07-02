import os, sys, argparse
import json
import pandas as pd
import graphlab as gl
from collections import Counter

def create_sidedata():
    print 'running create_sidedata()'
    #-------- ITEM SIDE DATA -----
    # create Pandas DF from json business data
    fname_test = 'data_analysis/test_data/test100_business.json'
    fname_full = '../data/Yelp/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json'
    # 'data_analysis/full_data/yelp_academic_dataset_business.json'

    fname = fname_full # user full dataset
    # fname = fname_test # user small test dataset
    with open(fname) as js_file:
        js_list = [json.loads(js_line) for js_line in js_file]
        js_list_filtered = [js for js in js_list if 'Restaurants' in js['categories']]
        dfb = pd.DataFrame(js_list_filtered)

    #--- attributes ---
    # handle the attribute field
    # which contains nested json fields...
    df_att = pd.io.json.json_normalize(dfb['attributes'])
    df_att.columns = ['Attributes.'+col for col in df_att.columns]
    # NOTE: TONS of NaN/Null in attribute data!!!

    #--- categories ---
    # handle the categories field, which is a flat list of categories.
    # The data has already been filtered down by having 'Restaurants' in the field.
    # Found hint/solution here:
    # http://datascience.stackexchange.com/questions/8253/how-to-binary-encode-multi-valued-categorical-variable-from-pandas-dataframe
    
    # apply collections.counter() to get a dict of hashable objects...
    dfb_cat_dict = dfb['categories'].apply(Counter)
    df_cat_full = pd.DataFrame.from_records(dfb_cat_dict).fillna(value=0).astype(int)

    exclude_cat = \
       ['Active Life', 'Amateur Sports Teams', 'Amusement Parks', 'Antiques', 'Apartments',
        'Appliances', 'Arcades', 'Art Galleries', 'Arts & Crafts', 'Arts & Entertainment', 'Auto Repair',
        'Automotive', 'Banks & Credit Unions', 'Beauty & Spas', 'Bed & Breakfast', 'Bikes', 'Boating',
        'Books, Mags, Music & Video', 'Bookstores', 'Bowling', 'Building Supplies', 'Butcher',
        'Candy Stores', 'Car Wash', 'Caterers', 'Chocolatiers & Shops', 'Coffee & Tea Supplies',
        'Colleges & Universities', 'Convenience Stores', 'Cooking Schools', 'Country Clubs',
        'Country Dance Halls', 'DJs', 'Dance Clubs', 'Day Spas', 'Department Stores', 'Discount Store',
        'Do-It-Yourself Food', 'Dry Cleaning & Laundry', 'Education', 'Event Planning & Services',
        'Fashion', 'Festivals', 'Financial Services', 'Fitness & Instruction', 'Flea Markets',
        'Flowers & Gifts', 'Food Delivery Services', 'Food Tours', 'Furniture Stores',
        'Gas & Service Stations', 'Gay Bars', 'Golf', 'Grocery', 'Guest Houses', 'Hardware Stores',
        'Health & Medical', 'Health Markets', 'Heating & Air Conditioning/HVAC', 'Herbs & Spices',
        'Hiking', 'Hobby Shops', 'Home & Garden', 'Home Decor', 'Home Services', 'Hotels',
        'Hotels & Travel', 'Jazz & Blues', 'Karaoke', 'Kids Activities', 'Kitchen & Bath', 'Lakes',
        'Landmarks & Historical Buildings', 'Leisure Centers', 'Local Services', 'Lounges', 'Meat Shops',
        'Music Venues', 'Musicians', 'Nightlife', 'Nutritionists', 'Organic Stores', 'Parking',
        'Party & Event Planning', 'Pasta Shops', 'Patisserie/Cake Shop', 'Performing Arts',
        'Personal Chefs', 'Pet Services', 'Pets', 'Piano Bars', 'Plumbing', 'Pool Halls',
        'Public Services & Government', 'Real Estate', 'Shopping', 'Shopping Centers', 'Soccer',
        'Social Clubs', 'Specialty Schools', 'Sporting Goods', 'Sports Clubs', 'Sports Wear',
        'Street Vendors', 'Swimming Pools', 'Tea Rooms', 'Tours', 'Toy Stores', 'Travel Services',
        'Venues & Event Spaces', 'Wine Bars', 'Yoga', 'Restaurants']

    # Drop the categories in the exclusion list
    df_cat = df_cat_full.drop(exclude_cat, axis=1)

    #Combining the 'categories' and 'attributes' into item data
    df_item_data = pd.concat([dfb[['business_id', 'name']] , df_cat], axis=1) # categories for now...

    # convert to SFrame...
    sf_itemdata = gl.SFrame(df_item_data)

    return sf_itemdata