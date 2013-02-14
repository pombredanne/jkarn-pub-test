/*
 * Finds trending topics (currently single-words only) by month for a corpus
 * of articles from technology news sites (gigaom, techcrunch, allthingsd).
 *
 * The corpus was extracted from the Common Crawl (hosted on S3) 
 * using an index by domain maintained by Triv.io. 
 * Mortar has contributed to the index project a script which copies the results 
 * from searching the index for a list of domains to an S3 location of your choosing:
 * [LINK TO SCRIPT]
 *
 * Warning: as the crawl contains a lot of data, this script will moderately
 * substantial amount of node-hours to complete. We recommend running it with
 * a cluser size of [RECOMMENDED CLUSTER SIZE]
 */

-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/fivethirtyeight_crawl/*.gz'
%default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/4914.gz'
-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/4916.gz'
-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/*.gz'
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/common_crawl';

-- Only text inside <p> elements from the html is considered by the script
-- However, sometimes <p> elements are used for short messages in addition to articles.
-- To address this, this script only considers paragraphs with at least MIN_WORDS_PER_PARAGRAPH.

%default MIN_WORDS_PER_PARAGRAPH '10'

-- Of the extracted words, only words of at least MIN_WORD_LENGTH letters are considered.
-- Smaller words are still counted towards the number of words in a paragraph however.

%default MIN_WORD_LENGTH '5'

-- How many trending words to list for each month in the final output

%default MAX_NUM_TRENDING_WORDS_PER_MONTH '25'

-- TODO: FIGURE OUT JAR SITUATION

REGISTER 's3n://mortar-dogfood-data/piggybank.jar';
REGISTER 's3n://mortar-dogfood-data/httpcore-4.2.2.jar';
REGISTER 's3n://mortar-dogfood-data/jsoup-1.7.2.jar';

-- Load Python udf's and Pig macros

REGISTER '../udfs/python/words.py' USING streaming_python AS words_lib;
REGISTER '../udfs/python/common_crawl.py' USING streaming_python AS common_crawl;

IMPORT '../macros/words.pig';

-- Load common-crawl webpages

pages   =   LOAD '$INPUT_PATH' 
            USING org.commoncrawl.pig.ArcLoader()
            AS (
                date: chararray, length: long, type: chararray, 
                status_code: int, ip_address: chararray, 
                url: chararray, html: chararray
            );

-- Extract an article date from each page's url, ex. 'http://techcrunch.com/2013/02/13/melodrama' -> ('2013', '02', '13')
-- Extract paragraphs with at least MIN_WORDS_PER_PARAGRAPH words from the html and tokenize them

pages_tokenized             =   FOREACH pages GENERATE 
                                    url, 
                                    common_crawl.get_article_date_from_url(url) AS date, 
                                    words_lib.words_from_html(html, 'true', $MIN_WORDS_PER_PARAGRAPH) AS words;

-- Get rid of any pages for which we couldn't find a date

pages_filtered              =   FILTER pages_tokenized BY (date is not null);

-- Get word counts for each page, excluding words with less than MIN_WORD_LENGTH letters

word_counts                 =   FOREACH pages_filtered 
                                GENERATE url, date, FLATTEN(words_lib.significant_word_count(words, $MIN_WORD_LENGTH));

-- Group these counts by month ('yyyy-mm') and find the totals across all pages for each month

word_counts_by_month        =   GROUP word_counts BY (word, date.year, date.month);
word_totals_per_month       =   FOREACH word_counts_by_month GENERATE
                                    group.$0 AS word, 
                                    CONCAT(group.$1, CONCAT('-', group.$2)) AS month: chararray, 
                                    SUM(word_counts.occurrences) AS occurrences;

-- Normalize the word counts against the total number of words in each month, resulting in a word frequency
-- (frequency = probability that a random word in the corpus is the given one)
-- Reflatten the word-month-frequency triples

all_words_by_month          =   GROUP word_totals_per_month BY month;
corpus_total_per_month      =   FOREACH all_words_by_month GENERATE 
                                    group AS month,  
                                    SUM(word_totals_per_month.occurrences) AS occurrences;
words_with_corpus_total     =   JOIN word_totals_per_month BY month, corpus_total_per_month BY month;
word_frequencies_per_month  =   FOREACH words_with_corpus_total GENERATE
                                    word_totals_per_month::word AS word, 
                                    word_totals_per_month::month AS month, 
                                    (double)word_totals_per_month::occurrences / (double)corpus_total_per_month::occurrences 
                                        AS frequency: double;

-- Group frequencies by word and order chronologically
-- Then find the "velocity" (whether the frequency of the word is increasing or descreasing, 
-- taking into account both absolute changes and relative changes to the month before and
-- combining them using a weighting formula) of the word for each month.

frequencies_by_word         =   GROUP word_frequencies_per_month BY word;
freqs_by_word_ordered       =   FOREACH frequencies_by_word {
                                    ordered = ORDER word_frequencies_per_month BY month ASC;
                                    GENERATE group AS word, ordered.(month, frequency) AS trend;
                                }
word_velocity_over_time     =   FOREACH freqs_by_word_ordered
                                GENERATE common_crawl.word_velocity_over_time(word, trend);

-- Flatten these word velocities and regroup by month
-- Find the highest-velocity words (trending topics) for each month

word_velocities             =   FOREACH word_velocity_over_time GENERATE FLATTEN(word_velocities);
positive_velocities         =   FILTER word_velocities BY (velocity > 0.0);
pos_velocities_by_month     =   GROUP positive_velocities BY month PARALLEL 1;
trending_words_by_month     =   FOREACH pos_velocities_by_month {
                                    ordered_velocities = ORDER positive_velocities BY velocity DESC;
                                    top_velocities = LIMIT ordered_velocities $MAX_NUM_TRENDING_WORDS_PER_MONTH;
                                    GENERATE group AS month, top_velocities.(word, frequency, abs_vel, rel_vel, velocity) AS trending_words;
                                }

-- Remove any existing output and store to S3

rmf $OUTPUT_PATH;
STORE trending_words_by_month INTO '$OUTPUT_PATH' USING PigStorage('\t');
