DEFINE WORD_TOTALS(words_rel, min_length)
RETURNS word_totals {
    word_counts         =   FOREACH $words_rel GENERATE FLATTEN(words_lib.significant_word_count(words, $min_length));
    words               =   GROUP word_counts BY word;
    $word_totals        =   FOREACH words GENERATE 
                                group AS word, 
                                SUM(word_counts.occurrences) AS occurrences;
};

DEFINE WORD_FREQUENCIES(word_counts)
RETURNS word_frequencies {
    all_words           =   GROUP $word_counts ALL;
    stats               =   FOREACH all_words GENERATE SUM($word_counts.occurrences) AS num_occurrences;
    words_with_stats    =   CROSS $word_counts, stats;
    $word_frequencies   =   FOREACH words_with_stats GENERATE
                                $0 AS word, $1 AS occurrences,
                                (double)$1 / (double)$2 AS frequency: double;
};

DEFINE RELATIVE_WORD_FREQUENCIES(subset, corpus, min_corpus_frequency)
RETURNS rel_frequencies {
    joined              =   JOIN $subset BY word, $corpus BY word;
    filtered            =   FILTER joined BY ($corpus::frequency > $min_corpus_frequency);
    $rel_frequencies    =   FOREACH filtered GENERATE
                                $subset::word AS word,
                                $corpus::occurrences AS occurrences,
                                $subset::frequency / $corpus::frequency AS rel_frequency;
};
