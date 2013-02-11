DEFINE WORD_FREQUENCIES(word_counts)
RETURNS word_frequencies {
    all_words           =   GROUP $word_counts ALL;
    stats               =   FOREACH all_words GENERATE SUM($word_counts.occurrences) AS num_occurrences;
    words_with_stats    =   CROSS $word_counts, stats;
    $word_frequencies   =   FOREACH words_with_stats GENERATE
                                $0 AS word, $1 AS occurrences,
                                (double)$1 / (double)$2 AS frequency: double;
};
