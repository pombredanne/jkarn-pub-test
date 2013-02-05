from pig_util import outputSchema

@outputSchema('age_bucket:chararray')
def age_bucket(age):
    """
    Get the age bucket (e.g. 20-29, 30-39) for an age.
    """
    # round the age down to the nearest ten
    low_age = age - (age % 10)
    high_age = low_age + 9
    print 'Original age: %s, low_age: %s, high_age: %s' % (age, low_age, high_age)
    return '%s - %s' % (low_age, high_age)

@outputSchema('avg_word_length:double')
def avg_word_length(bag):
    """
    Get the average word length in each search.
    """
    num_chars_total = 0
    num_words_total = 0
    for tpl in bag:
        query = tpl[2]
        words = query.split(' ')
        num_words = len(words)
        num_chars = sum([len(word) for word in words])

        num_words_total += num_words
        num_chars_total += num_chars

        # deal with strangely-encoded searches
        # before printing out
        print '%s, %s' % (num_words, num_chars)

    return float(num_chars_total) / float(num_words_total)
