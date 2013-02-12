/*
 * Finds which words show up most frequently in tweets expressing positive and negative
 * sentiments relative to the word's frequency in the total corpus of tweets.
 *
 * Tweets are filtered to include only those which match the regular expression that 
 * that parameter SEARCH_PATTERN is set to. By default, the filter accepts all tweets.
 *
 * Words are filtered so that only those with length >= MIN_WORD_LENGTH are counted.
 * 
 * All text is converted to lower case before being analyzed.
 * Words with non-alphabetic characters in the middle of them are ignored ("C3P0"), 
 * but words with non-alphabetic characters on the edges simply have them stripped ("totally!!!" -> "totally")
 *
 * Suggested improvements (left as exercises to the reader):
 *     Make the word-counts weighted by tweet relevance (how many SEARCH_PATTERN matches it has)
 *                               and by tweet influence (# of retweets + 1, for example)
 *     Aggregate by state (see coffee_tweets.pig for an example) -- "what makes NY happy vs CA?"
 *     Find trending words over time
 */

%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment'

%default SEARCH_PATTERN '^.*\\$'
%default MIN_WORD_LENGTH '5'

-- Load Python UDF's and Pig macros

REGISTER '../udfs/python/words.py' USING streaming_python AS words_lib;
REGISTER '../udfs/python/twitter_sentiment.py' USING streaming_python AS twitter_sentiment;

IMPORT '../macros/tweets.pig';
IMPORT '../macros/words.pig';
IMPORT '../macros/utils.pig';

-- Load tweets

-- Set to SINGLE_TWEET_FILE() for a small dataset to do a test run on (~20 MB)
tweets = ALL_TWEETS();

-- Filter tweets to only look at those that match the search pattern,
-- counting the number of regex matches as a "relevance score"

tweets_with_relevance   =   FOREACH tweets 
                            GENERATE text, twitter_sentiment.relevance(text, '$SEARCH_PATTERN') AS relevance;
relevant_tweets         =   FILTER tweets_with_relevance BY (relevance > 0);

-- Split the text of each tweet into words and calculate a sentiment score

tweets_tokenized        =   FOREACH relevant_tweets GENERATE words_lib.words_from_text(text) AS words;
tweets_with_sentiment   =   FOREACH tweets_tokenized 
                            GENERATE words, words_lib.sentiment(words) AS sentiment: double;

SPLIT tweets_with_sentiment INTO
    positive_tweets IF (sentiment > 0.0),
    negative_tweets IF (sentiment < 0.0);

-- Find the frequency of each word of at least MIN_WORD_LENGTH letters in all the tweets
-- (frequency = the probability that a random word in the corpus is the given word)

tweet_word_totals       =   WORD_TOTALS(tweets_tokenized, $MIN_WORD_LENGTH);
tweet_word_frequencies  =   WORD_FREQUENCIES(tweet_word_totals);

-- Find the frequencies of words that show up in tweets expressing positive sentiment, 
-- and divide them by the frequencies of those words in the entire tweet corpus
-- to find the relative frequency of each word. Take the top 100 of these
-- positively associated words.

pos_word_totals         =   WORD_TOTALS(positive_tweets, $MIN_WORD_LENGTH);
pos_word_frequencies    =   WORD_FREQUENCIES(pos_word_totals);
pos_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(pos_word_frequencies, tweet_word_frequencies);
top_pos_associations    =   TOP_N(pos_rel_frequencies, rel_frequency, 100, 'DESC');

-- Do the same with negative words.

neg_word_totals         =   WORD_TOTALS(negative_tweets, $MIN_WORD_LENGTH);
neg_word_frequencies    =   WORD_FREQUENCIES(neg_word_totals);
neg_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(neg_word_frequencies, tweet_word_frequencies);
top_neg_associations    =   TOP_N(neg_rel_frequencies, rel_frequency, 100, 'DESC');

rmf $OUTPUT_PATH/positive;
rmf $OUTPUT_PATH/negative;
STORE top_pos_associations INTO '$OUTPUT_PATH/positive' USING PigStorage('\t');
STORE top_neg_associations INTO '$OUTPUT_PATH/negative' USING PigStorage('\t');
