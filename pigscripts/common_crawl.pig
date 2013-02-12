%default INPUT_PATH 's3n://mortar-example-data/common-crawl/fivethirtyeight_crawl/6284.gz'
-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/4914.gz'
-- %default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/*.gz'
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/common_crawl';

%default ENGLISH_DICTIONARY_PATH 's3n://mortar-example-data/ngrams/books/20120701/eng-all/dictionary_89609.txt'

%default MIN_WORD_LENGTH '5'

REGISTER 's3n://mortar-dogfood-data/piggybank.jar';
REGISTER 's3n://mortar-dogfood-data/httpcore-4.2.2.jar';
REGISTER 's3n://mortar-dogfood-data/jsoup-1.7.2.jar';

REGISTER '../udfs/python/words.py' USING streaming_python AS words_lib;

IMPORT '../macros/utils.pig';
IMPORT '../macros/words.pig';
 
pages   =   LOAD '$INPUT_PATH' 
            USING org.commoncrawl.pig.ArcLoader()
            AS (
                date: chararray, length: long, type: chararray, 
                status_code: int, ip_address: chararray, 
                url: chararray, html: chararray
            );

english_word_frequencies    =   LOAD '$ENGLISH_DICTIONARY_PATH'
                                USING PigStorage('\t')
                                AS (word: chararray, occurrences: chararray, frequency: double);

pages_tokenized             =   FOREACH pages 
                                GENERATE url, words_lib.words_from_html(html) AS words;
pages_with_sentiment        =   FOREACH pages_tokenized 
                                GENERATE *, words_lib.sentiment(words) AS sentiment;

sentiments                  =   FOREACH pages_with_sentiment GENERATE sentiment;
all_sentiments              =   GROUP sentiments ALL;
avg_sentiment               =   FOREACH all_sentiments GENERATE AVG(sentiments.sentiment) AS sentiment;

pages_with_avg_sentiment    =   CROSS pages_with_sentiment, avg_sentiment;
pages_with_rel_sentiment    =   FOREACH pages_with_avg_sentiment GENERATE
                                    pages_with_sentiment::url AS url,
                                    pages_with_sentiment::words AS words,
                                    pages_with_sentiment::sentiment - avg_sentiment::sentiment AS rel_sentiment;

SPLIT pages_with_rel_sentiment INTO
    positive_pages IF (rel_sentiment > 0.0),
    negative_pages IF (rel_sentiment < 0.0);

page_word_totals        =   WORD_TOTALS(pages_tokenized, $MIN_WORD_LENGTH);
page_word_frequencies   =   WORD_FREQUENCIES(page_word_totals);
page_rel_freq_inv       =   RELATIVE_WORD_FREQUENCIES(english_word_frequencies, page_word_frequencies);
page_rel_frequncies     =   FOREACH page_rel_freq_inv 
                            GENERATE word, occurrences, 1.0 / rel_frequency AS rel_frequency;
top_page_associations   =   TOP_N(page_rel_frequncies, rel_frequency, 1000, 'DESC');

pos_word_totals         =   WORD_TOTALS(positive_pages, $MIN_WORD_LENGTH);
pos_word_frequencies    =   WORD_FREQUENCIES(pos_word_totals);
pos_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(pos_word_frequencies, page_word_frequencies);
top_pos_associations    =   TOP_N(pos_rel_frequencies, rel_frequency, 100, 'DESC');

neg_word_totals         =   WORD_TOTALS(negative_pages, $MIN_WORD_LENGTH);
neg_word_frequencies    =   WORD_FREQUENCIES(neg_word_totals);
neg_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(neg_word_frequencies, page_word_frequencies);
top_neg_associations    =   TOP_N(neg_rel_frequencies, rel_frequency, 100, 'DESC');

rmf $OUTPUT_PATH/page_associations;
rmf $OUTPUT_PATH/positive_associations;
rmf $OUTPUT_PATH/negative_associations;
rmf $OUTPUT_PATH/average_sentiment;

STORE top_page_associations INTO '$OUTPUT_PATH/page_associations' USING PigStorage('\t');
STORE top_pos_associations INTO '$OUTPUT_PATH/positive_associations' USING PigStorage('\t');
STORE top_neg_associations INTO '$OUTPUT_PATH/negative_associations' USING PigStorage('\t');
STORE avg_sentiment INTO '$OUTPUT_PATH/average_sentiment' USING PigStorage('\t');
