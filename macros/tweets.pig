/**
 * tweets: Macros for accessing the tweets stored in mortar's twitter-gardenhose-mortar bucket.
 * The schema for these tweets comes from https://dev.twitter.com/docs/platform-objects/tweets
 */

-- 1% of all tweets for the last week
DEFINE ALL_TWEETS()
RETURNS tweets {
    $tweets = LOAD 's3n://twitter-gardenhose-mortar/tweets' 
            USING org.apache.pig.piggybank.storage.JsonLoader(
                'coordinates:map[], created_at:chararray, current_user_retweet:map[], entities:map[], favorited:chararray, id_str:chararray, in_reply_to_screen_name:chararray, in_reply_to_status_id_str:chararray, place:map[], possibly_sensitive:chararray, retweet_count:int, source:chararray, text:chararray, truncated:chararray, user:map[], withheld_copyright:chararray, withheld_in_countries:{t:(country:chararray)}, withheld_scope:chararray');
};

-- a single tweet file, useful for testing small runs of your script
DEFINE SINGLE_TWEET_FILE()
RETURNS tweets {
    $tweets = LOAD 's3n://twitter-gardenhose-mortar/tweets/1360413804867.json'
            USING org.apache.pig.piggybank.storage.JsonLoader(
                'coordinates:map[], created_at:chararray, current_user_retweet:map[], entities:map[], favorited:chararray, id_str:chararray, in_reply_to_screen_name:chararray, in_reply_to_status_id_str:chararray, place:map[], possibly_sensitive:chararray, retweet_count:int, source:chararray, text:chararray, truncated:chararray, user:map[], withheld_copyright:chararray, withheld_in_countries:{t:(country:chararray)}, withheld_scope:chararray');
};
