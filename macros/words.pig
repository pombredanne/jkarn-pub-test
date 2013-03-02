/*
 * words_rel: {t: (words: {t: (word: chararray)})}
 * min_length: int
 * ==>
 * word_totals: {t: (word: chararray, occurrences: long)}
 */
DEFINE WORD_TOTALS(words_rel, min_length)
RETURNS word_totals {
    words_flat          =   FOREACH $words_rel GENERATE FLATTEN(words) AS word;
    significant_words   =   FILTER words_flat BY SIZE(word) >= $min_length;
    words_grouped       =   GROUP significant_words BY word;
    $word_totals        =   FOREACH words_grouped GENERATE 
                                group AS word, 
                                COUNT(significant_words) AS occurrences;
};

/*
 * word_counts: {t: (word: chararray, occurrences: long)}
 * ==> 
 * word_frequencies: {t: (word: chararray, occurrences: long, frequency: double)}
 */
DEFINE WORD_FREQUENCIES(word_counts)
RETURNS word_frequencies {
    all_words               =   GROUP $word_counts ALL;
    corpus_total            =   FOREACH all_words GENERATE SUM($word_counts.occurrences) AS occurrences;
    $word_frequencies       =   FOREACH $word_counts GENERATE
                                    $0 AS word, $1 AS occurrences,
                                    (double)$1 / (double)corpus_total.occurrences AS frequency: double;
};

/*
 * subset: {t: (word: chararray, occurrences: long, frequency: double)}
 * corpus: {t: (word: chararray, occurrences: long, frequency: double)}
 * min_corpus_frequency: double
 * ==>
 * rel_frequencies: {
 *                    t: (word: chararray, subset_occurrences: long, corpus_occurrences: long, 
 *                        subset_frequency: double, corpus_frequency: double, rel_frequency: double)
 *                  }
 */
DEFINE RELATIVE_WORD_FREQUENCIES(subset, corpus, min_corpus_frequency)
RETURNS rel_frequencies {
    joined              =   JOIN $subset BY word, $corpus BY word;
    filtered            =   FILTER joined BY ($corpus::frequency > $min_corpus_frequency);
    $rel_frequencies    =   FOREACH filtered GENERATE
                                $subset::word AS word,
                                $subset::occurrences AS subset_occurrences,
                                $corpus::occurrences AS corpus_occurrences,
                                $subset::frequency AS subset_frequency, 
                                $corpus::frequency AS corpus_frequency, 
                                $subset::frequency / $corpus::frequency AS rel_frequency;
};
