/**
 * Using the Excite search log data and (fake) users data, determine
 * which age group of users (e.g. 20-29, 30-39, etc) are the most prolific
 * searchers, and which age group uses the biggest words. :-)
 */

/** 
 * Parameters - default values here; you can override with -p on the command-line.
 */
%default INPUT_PATH 's3n://mortar-example-data/tutorial/excite.log.bz2'
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/excite/searches_by_age_bucket'

-- Register python udfs
REGISTER '../udfs/python/excite.py' USING streaming_python AS excite_udfs;

-- Load up the search log data
searches = LOAD '$INPUT_PATH' 
          USING PigStorage('\t') 
             AS (user_id:chararray, timestamp:chararray, query:chararray);

-- Get rid of any searches that are blank or without a user
clean_searches = FILTER searches BY user_id IS NOT NULL AND query IS NOT NULL;

-- Load up the (fake) user data
users = LOAD 's3n://mortar-example-data/tutorial/users.txt' 
       USING PigStorage('\t') 
          AS (user_id:chararray, age:int);

-- Bucket the user ages by 20-29, 30-39, etc
users_age_buckets = FOREACH users 
                   GENERATE user_id, 
                            excite_udfs.age_bucket(age) AS age_bucket;

-- Join search data to users
joined = JOIN clean_searches BY user_id, 
              users_age_buckets BY user_id;

-- Group by age bucket
grouped = GROUP joined BY age_bucket;

-- Calculate metrics on each age bucket
age_buckets = FOREACH grouped 
             GENERATE group as age_bucket,
                      COUNT(joined) as num_searches,
                      excite_udfs.avg_word_length(joined) as avg_word_length;

-- STORE the results into S3
-- We use the pig 'rmf' command to remove any
-- existing results first
rmf $OUTPUT_PATH;
STORE age_buckets INTO '$OUTPUT_PATH' USING PigStorage('\t');
