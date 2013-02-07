/*
 * Calculates sentiment analysis metrics for tweets
 * 
 * All text is converted to lower case before being analyzed.
 * Words with non-alphabetic characters in the middle of them are ignored ("C3P0"), 
 * but words with non-alphabetic characters on the edges simply have them stripped ("totally!!!" -> "totally")
 *
 * Metrics provided:
 *
 *     relevance:       Number of matches in the text to a regex search pattern 
 *                      (you can use the regex OR operator '|' to define a simple keyword set)
 *
 *     influence:       Number of retweets + 1 (so it can be used as a multiplicative factor safely)
 *
 *     sentiment:       A number indicating how positive/negative the text
 *                      Greater than zero if the text expresses a positive sentiment, less than zero if the opposite
 *                      Each +/- word contributes 1 to the score by default, but the sentiment analysis also
 *                      attempts to also account for intensifier words ("very") and negations ("not bad")
 *
 *     desire_flag:     1 if the text expresses a desire, 0 otherwise
 *                      Desires are currently "I want", "I need", "I'd like", "I'd love", and "I wish"
 *
 *     opinion_flag:    1 if the text expresses an opinion, 0 otherwise
 *                      Opinions are currently "I think"
 *
 *     weight:          A mathematical combination of the other metrics to estimate
 *                      the overall importance of this tweet.
 *                      Edit the function "tweet_weight" in twitter_sentiment.py to define your own weight metric
 *
 * As an example, this script uses the "sentiment" metric to classify tweets as positive or negative, 
 * finds the word frequency distributions for positive/negative tweets
 * and compares it to the word frequency distribution of the English language
 * (as generated from the Google Books corpus, top 150k words only),
 * available at s3://mortar-example-data/ngrams/books/20120701/eng-all/dictionary_150k.txt
 *
 * The results are the words have the highest ratio of their frequency in positive/negative tweets
 * to their frequency in the English language in general.
 */

%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment'

-- If a tweet's text does not match this regex pattern, the tweet will be skipped
-- This is useful if you are looking for sentiments about a particular topic
-- The default is to match anything (i.e. look for general sentiments, like about life and stuff)

%default SEARCH_PATTERN '^.*\\$'

-- Load Python udfs and Pig macros

REGISTER '../udfs/python/twitter_sentiment.py' USING streaming_python AS twitter_sentiment;
IMPORT '../macros/tweets.pig';

-- Load tweets

tweets = SINGLE_TWEET_FILE();
-- tweets = ALL_TWEETS();

-- Filter tweets to only look at those that match the search pattern,
-- counting the number of regex matches as a "relevance score"

tweets_with_relevance   =   FOREACH tweets GENERATE 
                                text,
                                twitter_sentiment.relevance(text, '$SEARCH_PATTERN') AS relevance,
                                (retweet_count + 1) AS influence;
relevant_tweets         =   FILTER tweets_with_relevance BY (relevance > 0);

-- Calculate sentiment metrics
--     sentiment: is the text positive/favorable or negative/unfavorable?
--     desire_flag: 1 if the text likely expresses a desire (ex. "I wish I had some cookies" or "I'd like a pony for Christmas please")
--     opinion_flag: 1 if the text likely expresses an opinion (ex. "I think cookies are an always food" or "You shouldn't be so spoiled")
--     weight: an estimate of how important the tweet is, taking into account all the other metrics

tweets_with_sentiment   =   FOREACH relevant_tweets GENERATE 
                                *, 
                                -- eventually, a literacy score
                                twitter_sentiment.sentiment(text) AS sentiment: float,
                                twitter_sentiment.is_desire(text) AS desire_flag: int,
                                twitter_sentiment.is_opinion(text) AS opinion_flag: int;

tweets_with_weight      =   FOREACH tweets_with_sentiment GENERATE
                                twitter_sentiment.tweet_weight(relevance, influence, sentiment, desire_flag, opinion_flag) AS weight,
                                relevance, influence, sentiment, desire_flag, opinion_flag, 
                                text;

-- Find the number of occurrences for each word in tweets with positive sentiment
-- We ignore words not found in an english dictionary (generated from the google books ngrams corpus)
-- We also ignore words with less than four letters

positive_tweets         =   FILTER tweets_with_weight BY (sentiment > 0.0);
pos_tweet_word_counts   =   FOREACH positive_tweets GENERATE FLATTEN(twitter_sentiment.significant_english_word_count(text));
pos_words               =   GROUP pos_tweet_word_counts BY word;
pos_word_totals         =   FOREACH pos_words GENERATE 
                                group AS word, 
                                SUM(pos_tweet_word_counts.occurrences) AS occurrences;

-- Find the word frequency distribution of these words 
-- (normalizing occurrences against the total number of words)

all_pos_word_totals         =   GROUP pos_word_totals ALL;
pos_word_stats              =   FOREACH all_pos_word_totals GENERATE 
                                    COUNT(pos_word_totals) AS num_words,
                                    SUM(pos_word_totals.occurrences) AS num_occurrences;
pos_word_totals_with_stats  =   CROSS pos_word_totals, pos_word_stats;
pos_word_frequencies        =   FOREACH pos_word_totals_with_stats GENERATE
                                    pos_word_totals::word AS word,
                                    pos_word_totals::occurrences AS occurrences,
                                    (double)pos_word_totals::occurrences / (double)pos_word_stats::num_occurrences AS frequency: double;

-- Rank words by their frequency in positive tweets divided by their frequency in the english language

pos_word_rel_frequencies    =   FOREACH pos_word_frequencies
                                GENERATE *, twitter_sentiment.rel_word_frequency_to_english(word, frequency) AS rel_frequency;
positive_associations       =   ORDER pos_word_rel_frequencies BY rel_frequency DESC;
top_positive_associations   =   LIMIT positive_associations 100;


-- Same thing but with tweets with negative sentiment
-- TODO: make macros for word-count, word-frequency-normalization and word-associations

negative_tweets             =   FILTER tweets_with_weight BY (sentiment < 0.0);
neg_tweet_word_counts       =   FOREACH negative_tweets GENERATE FLATTEN(twitter_sentiment.significant_english_word_count(text));
neg_words                   =   GROUP neg_tweet_word_counts BY word;
neg_word_totals             =   FOREACH neg_words GENERATE 
                                group AS word, 
                                SUM(neg_tweet_word_counts.occurrences) AS occurrences;

all_neg_word_totals         =   GROUP neg_word_totals ALL;
neg_word_stats              =   FOREACH all_neg_word_totals GENERATE 
                                COUNT(neg_word_totals) AS num_words,
                                SUM(neg_word_totals.occurrences) AS num_occurrences;
neg_word_totals_with_stats  =   CROSS neg_word_totals, neg_word_stats;
neg_word_frequencies        =   FOREACH neg_word_totals_with_stats GENERATE
                                    neg_word_totals::word AS word,
                                    neg_word_totals::occurrences AS occurrences,
                                    (double)neg_word_totals::occurrences / (double)neg_word_stats::num_occurrences AS frequency: double;

neg_word_rel_frequencies    =   FOREACH neg_word_frequencies
                                GENERATE *, twitter_sentiment.rel_word_frequency_to_english(word, frequency) AS rel_frequency;
negative_associations       =   ORDER neg_word_rel_frequencies BY rel_frequency DESC;
top_negative_associations   =   LIMIT negative_associations 100;

rmf $OUTPUT_PATH/positive;
rmf $OUTPUT_PATH/negative;
STORE top_positive_associations INTO '$OUTPUT_PATH/positive' USING PigStorage('\t');
STORE top_negative_associations INTO '$OUTPUT_PATH/negative' USING PigStorage('\t');
