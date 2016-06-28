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
  

def load_json_into_psql(json_path, dbname):
    # json_path = 'test_data'
    # json_path = '/Users/deanliu/git/GalvanizeDSI/capstone_project/data/Yelp/yelp_dataset_challenge_academic_dataset'

    # dbname = 'yelp_data' # the ENTIRE Yelp academic dataset
    # dbname = 'yelp_test' # the small testing ver of Yelp academic dataset

    dbconn = pg.connect(dbname=dbname, user='deanliu', host='localhost', port='5432')
    cursor = dbconn.cursor()

    # --- check for existance of database ---
    # cursor.execute( '''
    #                 ''')


    file_counter = 0

    # --- created SQL table ---
    cursor.execute( '''CREATE TABLE IF NOT EXISTS checkins
                        (id INTEGER PRIMARY KEY, data JSONB NOT NULL);
                    ''')
    cursor.execute( '''CREATE TABLE IF NOT EXISTS businesses
                        (id INTEGER PRIMARY KEY, data JSONB NOT NULL);
                    ''')
    cursor.execute( '''CREATE TABLE IF NOT EXISTS reviews
                        (id INTEGER PRIMARY KEY, data JSONB NOT NULL);
                    ''')
    cursor.execute( '''CREATE TABLE IF NOT EXISTS tips
                        (id INTEGER PRIMARY KEY, data JSONB NOT NULL);
                    ''')
    cursor.execute( '''CREATE TABLE IF NOT EXISTS users
                        (id INTEGER PRIMARY KEY, data JSONB NOT NULL);
                    ''')


    files = [f for f in os.listdir(json_path) if os.path.isfile(os.path.join(json_path, f))]
    json_files = [f for f in files if f.endswith('.json')]

    print 'Found {} files to process'.format(len(json_files))

    for f in json_files:
        fname = os.path.join(json_path, f)

        print 'Processing file: {}'.format(fname)
        bar_max = os.path.getsize(fname)
        bar = progressbar.ProgressBar(max_value = bar_max, 
                widgets=[
                    ' [', progressbar.Timer(), '] ',
                    progressbar.Bar(),
                    ' (', progressbar.ETA(), ')',
                ])
        bar_progress = 0
        file_counter += 1

        with open(fname) as js_file:

            for line_count, js_line in enumerate(js_file):
                # print line_count, js_line
                bar_progress+=len(js_line)
                bar.update(bar_progress)
                js_type = json.loads(js_line)['type']

                if js_type == 'checkin':
                    cursor.execute("""INSERT INTO checkins(id, data) VALUES (%s, %s);""", (line_count, js_line))
                elif js_type == 'business':
                    cursor.execute("""INSERT INTO businesses(id, data) VALUES (%s, %s);""", (line_count, js_line))
                elif js_type == 'review':
                    cursor.execute("""INSERT INTO reviews(id, data) VALUES (%s, %s);""", (line_count, js_line))
                elif js_type == 'tip':
                    cursor.execute("""INSERT INTO tips(id, data) VALUES (%s, %s);""", (line_count, js_line))
                elif js_type == 'user':
                    cursor.execute("""INSERT INTO users(id, data) VALUES (%s, %s);""", (line_count, js_line))
                else: # no js_typ match, FAULT!
                    pass
        bar.finish()
        dbconn.commit() # <--- this line saved my butt!

    dbconn.commit()
    dbconn.close()


# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':
    ''' '''
    parser = argparse.ArgumentParser(
            description='Load Yelp json data files into a new Postgres SQL database.',)
    parser.add_argument('json_path',
                        type = str,
                        help = 'Path to json data files.')
    parser.add_argument('database_name',
                        type = str,
                        help = 'Database name in PSQL.')

    args = parser.parse_args()
    json_path = args.json_path
    dbname = args.database_name

    print json_path
    print dbname
