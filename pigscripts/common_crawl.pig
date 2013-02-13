-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/fivethirtyeight_crawl/*.gz'
%default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/4914.gz'
-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/*.gz'
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/common_crawl';

%default MIN_WORD_LENGTH '4'
%default MAX_NUM_TRENDING_WORDS_PER_MONTH '25'

REGISTER 's3n://mortar-dogfood-data/piggybank.jar';
REGISTER 's3n://mortar-dogfood-data/httpcore-4.2.2.jar';
REGISTER 's3n://mortar-dogfood-data/jsoup-1.7.2.jar';

REGISTER '../udfs/python/words.py' USING streaming_python AS words_lib;
REGISTER '../udfs/python/common_crawl.py' USING streaming_python AS common_crawl;

IMPORT '../macros/words.pig';
 
pages   =   LOAD '$INPUT_PATH' 
            USING org.commoncrawl.pig.ArcLoader()
            AS (
                date: chararray, length: long, type: chararray, 
                status_code: int, ip_address: chararray, 
                url: chararray, html: chararray
            );

pages_tokenized             =   FOREACH pages GENERATE 
                                    url, 
                                    common_crawl.get_article_date_from_url(url) AS date, 
                                    words_lib.words_from_html(html, 'true') AS words;
pages_filtered              =   FILTER pages_tokenized BY (url is not null AND date is not null);

word_counts                 =   FOREACH pages_filtered 
                                GENERATE url, date, FLATTEN(words_lib.significant_word_count(words, $MIN_WORD_LENGTH));

word_counts_by_month        =   GROUP word_counts BY (word, date.year, date.month);
word_totals_per_month       =   FOREACH word_counts_by_month GENERATE
                                    group.$0 AS word, 
                                    CONCAT((chararray)group.$1, CONCAT('-', (chararray)group.$2)) AS month: chararray, 
                                    SUM(word_counts.occurrences) AS occurrences;

all_words_by_month          =   GROUP word_totals_per_month BY month;
corpus_total_per_month      =   FOREACH all_words_by_month GENERATE 
                                    group AS month,  
                                    SUM(word_totals_per_month.occurrences) AS occurrences;
words_with_corpus_total     =   JOIN word_totals_per_month BY month, corpus_total_per_month BY month;
word_frequencies_per_month  =   FOREACH words_with_corpus_total GENERATE
                                    word_totals_per_month::word AS word, 
                                    word_totals_per_month::month AS month, 
                                    word_totals_per_month::occurrences AS occurrences, 
                                    (double)word_totals_per_month::occurrences / (double)corpus_total_per_month::occurrences 
                                        AS frequency: double;

all_frequencies             =   GROUP word_frequencies_per_month ALL;
timespan                    =   FOREACH all_frequencies GENERATE
                                    MIN(word_frequencies_per_month.month) AS start_month, 
                                    MAX(word_frequencies_per_month.month) AS end_month;

frequencies_by_word         =   GROUP word_frequencies_per_month BY word;
freqs_by_word_with_span     =   CROSS frequencies_by_word, timespan;
freqs_over_timespan         =   FOREACH freqs_by_word_with_span GENERATE
                                    frequencies_by_word::group AS word, 
                                    common_crawl.fill_timespan(
                                        frequencies_by_word::word_frequencies_per_month, 
                                        timespan::start_month, 
                                        timespan::end_month
                                    ) AS word_frequency_over_timespan;

word_velocities_over_time   =   FOREACH freqs_over_timespan 
                                GENERATE common_crawl.word_velocities_over_time(word_frequency_over_timespan.(word, month, frequency));

word_velocities             =   FOREACH word_velocities_over_time GENERATE FLATTEN(word_velocities);
word_velocities_by_month    =   GROUP word_velocities BY month;
trending_words_by_month     =   FOREACH word_velocities_by_month {
                                    ordered_velocities = ORDER word_velocities BY velocity DESC;
                                    top_velocities = LIMIT ordered_velocities $MAX_NUM_TRENDING_WORDS_PER_MONTH;
                                    GENERATE group AS month, top_velocities.word AS trending_words;
                                }

rmf $OUTPUT_PATH;
STORE trending_words_by_month INTO '$OUTPUT_PATH' USING PigStorage('\t');
