/*
 * Finds which words show up most frequently in tweets expressing positive and negative
 * sentiments relative to the word's frequency in the English language.
 *
 * Tweets are filtered to include only those which match the regular expression that 
 * that parameter SEARCH_PATTERN is set to. By default, the filter accepts all tweets.
 *
 * English word frequencies were generated from the Google Books corpus, top ~90k words only),
 * available at s3://mortar-example-data/ngrams/books/20120701/eng-all/dictionary_89609.txt
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

-- Python udfs

REGISTER '../udfs/python/words.py' USING streaming_python AS words_lib;
REGISTER '../udfs/python/twitter_sentiment.py' USING streaming_python AS twitter_sentiment;

-- Pig macros

IMPORT '../macros/tweets.pig';
IMPORT '../macros/words.pig';
IMPORT '../macros/utils.pig';

-- Load tweets

-- Set to SINGLE_TWEET_FILE() for a small dataset to do a test run on (~20 MB)
tweets = ALL_TWEETS();

-- Filter tweets to only look at those that match the search pattern,
-- counting the number of regex matches as a "relevance score"

tweets_with_relevance   =   FOREACH tweets GENERATE 
                                text,
                                twitter_sentiment.relevance(text, '$SEARCH_PATTERN') AS relevance;
relevant_tweets         =   FILTER tweets_with_relevance BY (relevance > 0);
tweets_tokenized        =   FOREACH relevant_tweets GENERATE words_lib.words_from_text(text) AS words;
tweets_with_sentiment   =   FOREACH tweets_tokenized GENERATE 
                                words, 
                                words_lib.sentiment(words) AS sentiment: double;

SPLIT tweets_with_sentiment INTO
    positive_tweets IF (sentiment > 0.0),
    negative_tweets IF (sentiment < 0.0);

-- Find the number of occurrences for each word in tweets expressing positive sentiments
-- We ignore non-english words and words with less then four letters

pos_tweet_word_counts   =   FOREACH positive_tweets GENERATE FLATTEN(words_lib.significant_english_word_count(words));
pos_words               =   GROUP pos_tweet_word_counts BY word;
pos_word_totals         =   FOREACH pos_words GENERATE 
                                group AS word, 
                                SUM(pos_tweet_word_counts.occurrences) AS occurrences;

-- Find the word frequency distribution of these words (normalizing occurrences against the total number of words)

pos_word_frequencies    =   WORD_FREQUENCIES(pos_word_totals);

-- Rank words by their frequency in positive tweets divided by their frequency in the english language

pos_word_rel_frequencies    =   FOREACH pos_word_frequencies
                                GENERATE *, words_lib.rel_word_frequency_to_english(word, frequency) AS rel_frequency;
top_positive_associations   =   TOP_N(pos_word_rel_frequencies, rel_frequency, 100, 'DESC');

-- Same thing except with tweets expressing negative sentiments

neg_tweet_word_counts       =   FOREACH negative_tweets GENERATE FLATTEN(words_lib.significant_english_word_count(words));
neg_words                   =   GROUP neg_tweet_word_counts BY word;
neg_word_totals             =   FOREACH neg_words GENERATE 
                                    group AS word, 
                                    SUM(neg_tweet_word_counts.occurrences) AS occurrences;
neg_word_frequencies        =   WORD_FREQUENCIES(neg_word_totals);
neg_word_rel_frequencies    =   FOREACH neg_word_frequencies
                                GENERATE *, words_lib.rel_word_frequency_to_english(word, frequency) AS rel_frequency;
top_negative_associations   =   TOP_N(neg_word_rel_frequencies, rel_frequency, 100, 'DESC');

rmf $OUTPUT_PATH/positive;
rmf $OUTPUT_PATH/negative;
STORE top_positive_associations INTO '$OUTPUT_PATH/positive' USING PigStorage('\t');
STORE top_negative_associations INTO '$OUTPUT_PATH/negative' USING PigStorage('\t');
