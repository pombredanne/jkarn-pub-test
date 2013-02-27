from __future__ import unicode_literals
import re
from collections import defaultdict
from pig_util import outputSchema

# Decorator to help udf's handle null input like Pig does (just ignore it and return null)
def null_if_input_null(fn):
    def wrapped(*args, **kwargs):
        for arg in args:
            if arg is None:
                return None
        for k, v in kwargs.items():
            if v is None:
                return None
        return fn(*args, **kwargs)

    wrapped.__name__ = fn.__name__
    wrapped.__doc__ = fn.__doc__
    wrapped.__dict__.update(fn.__dict__)

    return wrapped

non_english_character_pattern = re.compile("[^a-z']")

# Accepts strings consisting of 1 or more characters in [a-z']
# (the apostrophe is so that contraction words such as don't are accepted)
def is_alphabetic(s):
    return len(s) > 0 and not bool(non_english_character_pattern.search(s))

whitespace_pattern = re.compile('\\s+')
word_with_punctuation_pattern = re.compile("^[^a-z']*([a-z']+)[^a-z']*$")

# Tokenizes a string into bag of single-element tuples, each containing a single word.
# Strips casing and punctuation (ex. "Totally!!!" -> "totally").
# Excludes words which are not accepted by is_alphabetic after being stripped of punctuation.
@outputSchema("words: {t: (word: chararray)}")
@null_if_input_null
def words_from_text(text):
    return [(word, ) for word in 
            [re.sub(word_with_punctuation_pattern, '\\1', word) 
             for word in re.split(whitespace_pattern, text.lower())
            ] if is_alphabetic(word)]

# Finds the number of occurrences of each unique word in a bag of single-element word tuples, 
# ignoring words of length < min_length
@outputSchema("word_counts: {t: (word: chararray, occurrences: long)}")
@null_if_input_null
def significant_word_count(words_bag, min_length):
    word_list = [t[0] for t in words_bag if len(t) > 0 and len(t[0]) >= min_length]
    count = defaultdict(int)
    for word in word_list:
        count[word] += 1
    return count.items()
