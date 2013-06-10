/**
 * top_density_songs_local: Find the most dense song in 
 * a very small subset of songs.  Meant for use with mortar:local commands.
 */

-- Default Parameters
%default OUTPUT_PATH '../output/top_density_songs'

-- User-Defined Functions (UDFs)
REGISTER '../udfs/python/millionsong.py' USING streaming_python AS millionsong;

-- Macros
IMPORT '../macros/millionsong.pig';

-- Load up the million song dataset
-- using our LOCAL_SONGS_FILE_SMALL() pig macro from millionsong.pig
-- to get local data, which is only 2 songs (see top_density_songs.pig for full data)
songs = LOCAL_SONGS_FILE_SMALL();

-- Use FILTER to get only songs that have a duration
filtered_songs = FILTER songs BY duration > 0;

-- Use FOREACH to run calculations on every row.
-- Here, we calculate density (sounds per second) using 
-- the the Python UDF density function from millionsong.py
song_density = FOREACH filtered_songs 
              GENERATE artist_name, 
                       title,
                       millionsong.density(segments_start, duration);

-- Get the most dense song
-- by using ORDER and then LIMIT
density_ordered = ORDER song_density BY density DESC;
top_density     = LIMIT density_ordered 1;

-- STORE the top 50 songs into S3
-- We use the pig 'rmf' command to remove any
-- existing results first
rmf $OUTPUT_PATH;
STORE top_density 
 INTO '$OUTPUT_PATH' 
USING PigStorage('\t');
