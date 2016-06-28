#!/usr/bin/env python
# Adopted from http://blog.endpoint.com/2016/03/loading-json-files-into-postgresql-95.html
 
import os, argparse
import sys
import json
 
try:
    import psycopg2 as pg
    import psycopg2.extras
except:
    print "Install psycopg2"
    exit(123)
 
try:
    import progressbar
except:
    print "Install progressbar2"
    exit(123)
  

def load_json_into_psql(json_path):
    # json_path = 'test_data'
    # json_path = '/Users/deanliu/git/GalvanizeDSI/capstone_project/data/Yelp/yelp_dataset_challenge_academic_dataset'

    # connect once to setup the database 'temp'
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

    # look for json files in path
    print 'Searching for json files at path: {}'.format(os.path.abspath(json_path))
    files = [f for f in os.listdir(json_path) if os.path.isfile(os.path.join(json_path, f))]
    json_files = [f for f in files if f.endswith('.json')]
    print 'Found {} files to process'.format(len(json_files))

    # load data
    for file_count, f in enumerate(json_files):
        fname = os.path.join(json_path, f)
        print 'Processing file #{}: {}'.format(file_count+1, fname)
        bar = progressbar.ProgressBar(max_value = os.path.getsize(fname), 
                    widgets=[' [', progressbar.Timer(), '] ', progressbar.Bar(), ' (', progressbar.ETA(), ')'] )
        bar_progress = 0
        with open(fname) as js_file:
            for line_count, js_line in enumerate(js_file):
                bar_progress+=len(js_line)
                bar.update(bar_progress)

                js = json.loads(js_line)
                js_type = js.pop('type')

                if js_type == 'checkin':
                    parse_checkin(cursor, js)
                elif js_type == 'business':
                    parse_business(cursor, js)
                elif js_type == 'review':
                    parse_review(cursor, js)
                elif js_type == 'tip':
                    parse_tip(cursor, js)
                elif js_type == 'user':
                    parse_user(cursor, js)
                else: # no js_typ match, FAULT!
                    pass
        bar.finish()
        dbconn.commit() # <--- commit at end of each file. this line saved my butt!

    # create new table 'user_reviews' that merges reviews with business table
    # cursor.execute('''  SELECT r.data->>'user_id' AS user_id,
    #                         r.data->>'business_id' AS business_id,
    #                         b.data->>'name' AS business_name,
    #                         r.data->>'stars' AS stars,
    #                         b.data->>'review_count' AS review_count,
    #                         concat(b.data->>'city',', ', b.data->>'state') AS locale
    #                     INTO user_reviews
    #                     FROM reviews AS r
    #                     JOIN businesses AS b
    #                         ON r.data->>'business_id' = b.data->>'business_id'
    #                     WHERE (b.data->'categories' @> '["Restaurants"]'::jsonb);
    #                 ''')

    # create new able 'biz_data'

    dbconn.commit()
    dbconn.close()
    return

def create_tables(cursor):
    cursor.execute('''CREATE TABLE checkins
                        (id SERIAL PRIMARY KEY, data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE businesses
                        (biz_id CHAR(22) PRIMARY KEY,
                         name VARCHAR(255),
                         categories JSONB,
                         attributes JSONB,
                         hours JSONB,
                         data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE reviews
                        (id SERIAL PRIMARY KEY,
                         biz_id CHAR(22),
                         user_id CHAR(22),
                         stars NUMERIC,
                         rv_txt TEXT,
                         rv_date DATE,
                         data JSONB);''')

    cursor.execute('''CREATE TABLE tips
                        (id SERIAL PRIMARY KEY, data JSONB NOT NULL);''')

    cursor.execute('''CREATE TABLE users
                        (user_id CHAR(22) PRIMARY KEY,
                         name VARCHAR(255),
                         review_count INTEGER,
                         friends JSONB,
                         data JSONB NOT NULL);''')
    return

def parse_checkin(cursor, js):
    cursor.execute('INSERT INTO checkins(data) VALUES (%s);', [json.dumps(js)] )
    return

def parse_business(cursor, js):
    biz_id      = js.pop('business_id')
    name        = js.pop('name')
    categories  = json.dumps(js.pop('categories'))
    attributes  = json.dumps(js.pop('attributes'))
    hours       = json.dumps(js.pop('hours'))
    data        = json.dumps(js)
    cursor.execute('INSERT INTO businesses VALUES (%s, %s, %s, %s, %s, %s);', \
                    (biz_id, name, categories, attributes, hours, data) )
    return

def parse_review(cursor, js):
    biz_id      = js.pop('business_id')
    user_id     = js.pop('user_id')
    stars       = js.pop('stars')
    rv_txt      = js.pop('text')
    rv_date     = js.pop('date')
    data        = json.dumps(js)
    cursor.execute('''INSERT INTO reviews(biz_id, user_id, stars, rv_txt, rv_date, data)
                        VALUES (%s, %s, %s, %s, %s, %s);''', \
                    (biz_id, user_id, stars, rv_txt, rv_date, data) )
    return

def parse_tip(cursor, js):
    cursor.execute('INSERT INTO tips(data) VALUES (%s);', [json.dumps(js)] )
    return

def parse_user(cursor, js):
    user_id = js.pop('user_id')
    name    = js.pop('name')
    rv_cnt  = js.pop('review_count')
    friends = json.dumps(js.pop('friends'))
    data    = json.dumps(js)
    cursor.execute('INSERT INTO users VALUES (%s, %s, %s, %s, %s);', \
                    (user_id, name, rv_cnt, friends, data) )
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

