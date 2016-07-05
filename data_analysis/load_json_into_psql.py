#!/usr/bin/env python
# Adopted from http://blog.endpoint.com/2016/03/loading-json-files-into-postgresql-95.html
 
import os, sys, argparse
import json
 
try:
    import psycopg2 as pg
except:
    print "Install psycopg2"
    exit(123)
 
try:
    import progressbar
except:
    print "Install progressbar2 (by Rick van Hattem (Wolph)!"
    print "$ pip install progressbar2"
    exit(123)
  

def load_json_into_psql(json_path):

    # connect once to setup the 'temp' database
    # IF a 'temp' database already exist, IT WILL BE DELETED!!
    dbconn = pg.connect(user='deanliu', host='localhost', port='5432')
    dbconn.autocommit = True
    cursor = dbconn.cursor()
    cursor.execute('DROP DATABASE IF EXISTS temp;')
    cursor.execute('CREATE DATABASE temp;')
    cursor.close()
    dbconn.close()

    # now connect to our 'temp' database
    dbconn = pg.connect(dbname='temp', user='deanliu', host='localhost', port='5432')
    cursor = dbconn.cursor()

    create_tables(cursor)

    # look for json files in the path
    print 'Searching for json files at path: {}'.format(os.path.abspath(json_path))
    files = [f for f in os.listdir(json_path) if os.path.isfile(os.path.join(json_path, f))]
    json_files = [f for f in files if f.endswith('.json')]
    print 'Found {} files to process'.format(len(json_files))

    # load data
    for file_count, f in enumerate(json_files):
        fname = os.path.join(json_path, f)
        print 'Processing file #{}: {}'.format(file_count+1, fname)
        progress_max = os.path.getsize(fname)
        bar = progressbar.ProgressBar( max_value = progress_max, \
                                        widgets = [' [', progressbar.Timer(), '] ', \
                                                   progressbar.Bar(), \
                                                   ' (', progressbar.ETA(), ')'] )
        progress = 0
        with open(fname) as js_file:
            for js_line in js_file:
                js = json.loads(js_line)

                js_type = js.pop('type')
                if js_type == 'checkin':
                    # parse_checkin(cursor, js)
                    pass
                elif js_type == 'business':
                    parse_business(cursor, js)
                    pass
                elif js_type == 'review':
                    parse_review(cursor, js)
                    pass
                elif js_type == 'tip':
                    # parse_tip(cursor, js)
                    pass
                elif js_type == 'user':
                    parse_user(cursor, js)
                    pass
                else: # no js_typ match, FAULT!
                    pass

                progress+=len(js_line)
                # print 'progress = ', progress, '/', progress_max
                bar.update(progress)
        bar.finish()
        dbconn.commit() # <--- commit at end of each file. this line saved my butt!

    create_user_reviews_table(cursor)
    dbconn.commit()
    dbconn.close()

    return

def create_tables(cursor):
    # cursor.execute('''CREATE TABLE checkins
    #                     (id SERIAL PRIMARY KEY, data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE businesses
                        (biz_id CHAR(22) PRIMARY KEY,
                         name VARCHAR(255),
                         categories JSONB,
                         attributes JSONB,
                         hours JSONB,
                         data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE reviews
                        (review_id CHAR(22) PRIMARY KEY,
                         biz_id CHAR(22),
                         user_id CHAR(22),
                         stars NUMERIC,
                         rv_txt TEXT,
                         rv_date DATE,
                         votes JSONB);''')

    # cursor.execute('''CREATE TABLE tips
    #                     (id SERIAL PRIMARY KEY, data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE users
                        (user_id CHAR(22) PRIMARY KEY,
                         name VARCHAR(255),
                         review_count INTEGER,
                         friends JSONB,
                         data JSONB NOT NULL);''')
    return

def parse_checkin(cursor, js):
    cursor.execute('INSERT INTO checkins(data) VALUES (%s);', [json.dumps(js)] )
#   data: [ 'business_id', 'checkin_info']
    return

def parse_business(cursor, js):
    biz_id      = js.pop('business_id')
    name        = js.pop('name')
    categories  = json.dumps(js.pop('categories'))
    attributes  = json.dumps(js.pop('attributes'))
    hours       = json.dumps(js.pop('hours'))
    data        = json.dumps(js)
#   data: [ 'full_address', 'city', 'state',
#           'neighborhoods', 'latitude', 'longitude',
#           'review_count', 'stars']
    cursor.execute('INSERT INTO businesses VALUES (%s, %s, %s, %s, %s, %s);', \
                    (biz_id, name, categories, attributes, hours, data) )
    return

def parse_review(cursor, js):
    review_id   = js.pop('review_id')
    biz_id      = js.pop('business_id')
    user_id     = js.pop('user_id')
    stars       = js.pop('stars')
    rv_txt      = js.pop('text')
    rv_date     = js.pop('date')
    votes        = json.dumps(js.pop('votes'))
#   votes: [ 'funny', userful', 'cool' ]
    cursor.execute('''INSERT INTO reviews(review_id, biz_id, user_id, stars, rv_txt, rv_date, votes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);''', \
                    (review_id, biz_id, user_id, stars, rv_txt, rv_date, votes) )
    return

def parse_tip(cursor, js):
    cursor.execute('INSERT INTO tips(data) VALUES (%s);', [json.dumps(js)] )
#   data: ['business_id', 'user_id', 'date', 'likes', 'text']
    return

def parse_user(cursor, js):
    user_id = js.pop('user_id')
    name    = js.pop('name')
    rv_cnt  = js.pop('review_count')
    friends = json.dumps(js.pop('friends'))
    data    = json.dumps(js)
#   data: [ 'elite: [years] ', 'yelping_since', 'fans', 'average_stars',
#           'votes: ['funny', 'userful', 'cool'] ',
#           'compliments: ['profile', 'cute', 'funny', 'plain', writer',
#                          'note', 'photos', 'hot', 'cool', 'more'] ' ]
    cursor.execute('INSERT INTO users VALUES (%s, %s, %s, %s, %s);', \
                    (user_id, name, rv_cnt, friends, data) )
    return

def create_user_reviews_table(cursor):
    # create new table 'user_reviews' that contains ONLY restaurant reviews
    cursor.execute('''  SELECT  r.user_id AS user_id,
                                r.biz_id AS business_id,
                                r.stars AS stars
                        INTO user_reviews
                        FROM reviews AS r
                        JOIN businesses AS b
                            ON r.biz_id = b.biz_id
                        WHERE (b.categories @> '["Restaurants"]'::jsonb);
                    ''')
    # cursor.execute('''  SELECT  r.user_id AS user_id,
    #                             r.biz_id AS business_id,
    #                             b.name AS business_name,
    #                             r.stars AS stars,
    #                         concat(b.data->>'city',', ', b.data->>'state') AS locale
    #                     INTO user_reviews
    #                     FROM reviews AS r
    #                     JOIN businesses AS b
    #                         ON r.biz_id = b.biz_id
    #                     WHERE (b.categories @> '["Restaurants"]'::jsonb);
    #                 ''')

    # save user_reviews table to file
    fout = open('temp_user_reviews.csv', 'w')
    cursor.copy_expert('COPY user_reviews TO STDOUT WITH CSV HEADER', fout)
    fout.close()

    # save selected business data to file (for item_data in recommender use)
    # fout = open('temp_biz_data.csv', 'w')
    # cursor.copy_expert('''COPY (SELECT biz_id, name, categories, attributes, 
    #                             FROM businesses
    #                             ) TO STDOUT WITH CSV HEADER''', fout)
    # fout.close()

    return


# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':
    ''' '''
    parser = argparse.ArgumentParser(
            description="Load Yelp json data files into a new Postgres SQL database named 'temp'.",)
    parser.add_argument('json_path', type = str, help = 'Path to json data files.')
    args = parser.parse_args()
    json_path = args.json_path

    load_json_into_psql(json_path)

