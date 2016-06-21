#!/usr/bin/env python
# Found this script from http://blog.endpoint.com/2016/03/loading-json-files-into-postgresql-95.html
#
 
import os
import sys
import logging
 
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
 
import json
 
import logging
logger = logging.getLogger()
 
PG_CONN_STRING = "dbname='test_yelp' port='5432'"
 
data_dir = "test_data"
dbconn = pg.connect(PG_CONN_STRING)
 
logger.info("Loading data from '{}'".format(data_dir))
 
cursor = dbconn.cursor()
 
counter = 0
empty_files = []
 
class ProgressInfo:
 
    def __init__(self, dir):
        files_no = 0
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(".json"):
                    files_no += 1
        self.files_no = files_no
        print "Found {} files to process".format(self.files_no)
        self.bar = progressbar.ProgressBar(maxval=self.files_no,
                                           widgets=[' [', progressbar.Timer(), '] [', progressbar.ETA(), '] ', progressbar.Bar(),])
 
    def update(self, counter):
        self.bar.update(counter)
 
pi = ProgressInfo(os.path.expanduser(data_dir))
 
for root, dirs, files in os.walk(os.path.expanduser(data_dir)):
    # print files
    for f in files:
        fname = os.path.join(root, f)
        # print fname
 
        if not fname.endswith(".json"):
            continue

        with open(fname) as js_file:
            # for line in js_file:

            # data = json.load(line)
            data = js_file.read()
            if not data:
                empty_files.append(fname)
                continue
            # print data, counter
            # print type(data)
            # import json
            # dd = json.loads(data)
            counter += 1
            pi.update(counter)

            if True:
                # --- psql ver 9.5.x ---
                cursor.execute("""
                                INSERT INTO test_yelp(data)
                                VALUES (%s)
                                ON CONFLICT ON CONSTRAINT no_overlapping_jsons DO NOTHING
                            """, (data, ))
            else:
                # --- psql ver 9.4.x ---
                cursor.execute("""
                                INSERT INTO stats_data(data)
                                SELECT %s
                                WHERE NOT EXISTS (SELECT 42
                                                  FROM stats_data
                                                  WHERE
                                                        ((data->>'metadata')::json->>'country')  = %s
                                                    AND ((data->>'metadata')::json->>'installation') = %s
                                                    AND tstzrange(
                                                            to_timestamp((data->>'start_ts')::double precision),
                                                            to_timestamp((data->>'end_ts'  )::double precision)
                                                        ) &&
                                                        tstzrange(
                                                            to_timestamp(%s::text::double precision),
                                                            to_timestamp(%s::text::double precision)
                                                        )
                                                 )
                            """, (data, str(dd['metadata']['country']), str(dd['metadata']['installation']), str(dd['start_ts']), str(dd['end_ts'])))
 
print ""
 
logger.debug("Refreshing materialized views")
cursor.execute("""REFRESH MATERIALIZED VIEW sessions""");
cursor.execute("""ANALYZE""");
 
dbconn.commit()
 
logger.info("Loaded {} files".format(counter))
logger.info("Found {} empty files".format(len(empty_files)))
if empty_files:
    logger.info("Empty files:")
    for f in empty_files:
        logger.info(" >>> {}".format(f))