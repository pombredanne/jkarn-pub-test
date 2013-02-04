/**
 * This script demonstrates using Mortar to parse Apache common-log-format logs.
 *
 * It takes a set of logs (the default input is a month-worth of logs from a NASA web server)
 * and calculates the for each date:
 *     1) Total # of requests for non-image URI's (non-image to filter out icons and the like)
 *     2) Total # of bytes served for non-image URI's
 *     3) Top 10 non-image URI's served for that date, sorted by either num_requests or num_bytes 
 *        depending on the ORDERING parameter.
 *
 * Pig concepts demonstrated:
 *     - Loading Apache logs
 *     - Parsing timestamps using a Python UDF
 *     - Regex matching
 *     - Grouping by multiple fields; nested foreach
 */

/** 
 * Parameters - default values here; you can override with -p on the command-line.
 */
 
%default INPUT_PATH 's3n://mortar-example-data/nasa_logs/NASA_access_log_*.gz'
%default OUTPUT_PATH 's3n://hawk-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/mortar-examples/nasa_logs'
%default ORDERING 'num_requests'    -- should be 'num_requests' or 'num_bytes'

-- Register python udfs
REGISTER '../udfs/python/nasa_logs.py' USING streaming_python AS nasa_logs;

-- Load using the Piggybank function CommonLogLoader
logs = LOAD '$INPUT_PATH' 
       USING org.apache.pig.piggybank.storage.apachelog.CommonLogLoader() 
       AS (addr: chararray, logname: chararray, user: chararray, time: chararray, 
           method: chararray, uri: chararray, proto: chararray, status: int, 
           bytes: int);

-- Extract the date from each log and prune unneeded columns
-- Note that regex escapes need to be double-escaped (ex. '\\d' instead of '\d')
logs_with_date = FOREACH logs 
                 GENERATE 
                    REGEX_EXTRACT(nasa_logs.clf_timestamp_to_iso(time), '^(\\d{4}-\\d{2}-\\d{2})T', 1) AS date: chararray, 
                    uri, bytes;

-- Filter out requests for images
relevant_logs = FILTER logs_with_date 
                BY NOT (REGEX_EXTRACT(uri, '.*\\.(.*)', 1) MATCHES 'jpg|JPG|gif|GIF|png|PNG|tiff|TIFF');

-- We want to find top uri's for each date
-- Since we cannot do a nested group as of Pig 0.9, we group by both uri and date
-- and will group again later by just date

logs_by_uri_date = GROUP relevant_logs BY (uri, date);

-- Use builtin udf's to find total # of requests and # of bytes served
uri_date_counts = FOREACH logs_by_uri_date
                  GENERATE group, 
                           COUNT(relevant_logs) AS num_requests: long, 
                           SUM(relevant_logs.bytes) AS num_bytes: long;

-- Group the uri-date totals by just date
uri_counts_by_date = GROUP uri_date_counts BY group.date;

-- Find total # requests and # bytes served for each date
uri_totals_by_date = FOREACH uri_counts_by_date
                     GENERATE group AS date: chararray,
                              SUM(uri_date_counts.num_requests) AS num_requests: long,
                              SUM(uri_date_counts.num_bytes) AS num_bytes: long,
                              uri_date_counts;

-- We use a nested FOREACH to sort the inner bag uri_date_counts for each date group
-- See the "nested_op" section in http://pig.apache.org/docs/r0.9.2/basic.html#foreach

top_uris_by_date = FOREACH uri_totals_by_date {
                       uris_ordered = ORDER uri_date_counts BY $ORDERING DESC;
                       top_uris = LIMIT uris_ordered 10;
                       GENERATE date, num_requests, num_bytes, top_uris;
                   }

-- Since we had to group twice, the schema "top_uris: {t: (group: (uri, date), num_requests, num_bytes)}"
-- is a bit ugly. cleanup_output throws out the duplicated date field, giving the simpler schema
-- "top_uris: {t: (uri, num_requests, num_bytes)}"

out = FOREACH top_uris_by_date
      GENERATE date, num_requests, num_bytes, nasa_logs.cleanup_output(top_uris);

rmf $OUTPUT_PATH;
STORE out INTO '$OUTPUT_PATH' USING PigStorage('\t');
