/*
 * Which US state is home to the highest concentration of coffee snobs?
 *
 * Loads up a week's-worth of tweets from the twitter-gardenhose (https://github.com/mortardata/twitter-gardenhose),
 * searches through them for telltale coffee snob phrases (single origin, la marzocco, etc) and rolls up the 
 * results by US state.
 */

-- Set the destination for our output 
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/coffee_tweets'

-- Register the python User-Defined Functions (UDFs) we will use
REGISTER '../udfs/python/twitter_places.py' USING streaming_python AS twitter_places;
REGISTER '../udfs/python/coffee.py' USING streaming_python AS coffee;

-- Import shared code from pig macros to load up the twitter dataset
IMPORT '../macros/tweets.pig';

-- Load up a single file of the JSON-formatted tweets
-- (to use all tweets, switch to ALL_TWEETS())
tweets = SINGLE_TWEET_FILE();

-- Filter to get only tweets that have a location in the US
tweets_with_place = 
    FILTER tweets 
        BY place IS NOT NULL 
       AND place#'country_code' == 'US' 
       AND place#'place_type' == 'city';

-- Parse out the US state name from the location
-- and determine whether this is a coffee tweet.
coffee_tweets = 
    FOREACH tweets_with_place
   GENERATE text, 
            place#'full_name' AS place_name,
            twitter_places.us_state(place#'full_name') AS us_state,
            coffee.is_coffee_tweet(text) AS is_coffee_tweet;

-- Filter to make sure we only include results with
-- where we found a US State
with_state = 
    FILTER coffee_tweets
        BY us_state IS NOT NULL;

-- Group the results by US state
grouped = 
    GROUP with_state 
       BY us_state;

-- Calculate the percentage of coffee tweets
-- for each state
coffee_tweets_by_state = 
    FOREACH grouped
   GENERATE group as us_state,
            SUM(with_state.is_coffee_tweet) AS num_coffee_tweets,
            COUNT(with_state) AS num_tweets,
            100.0 * SUM(with_state.is_coffee_tweet) / COUNT(with_state) AS pct_coffee_tweets;

-- filter out small states, where the population
-- of tweets is too small to get a fair reading
small_states_filtered =
    FILTER coffee_tweets_by_state
        BY num_tweets > 50000;

-- Order by percentage to get the largest
-- coffee snobs at the top
ordered_output = 
    ORDER small_states_filtered 
       BY pct_coffee_tweets DESC;

-- Remove any existing output and store the results to S3
rmf $OUTPUT_PATH;
STORE ordered_output 
 INTO '$OUTPUT_PATH'
USING PigStorage('\t');
