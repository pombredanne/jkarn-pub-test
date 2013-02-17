import re
from HTMLParser import HTMLParser
from pig_util import outputSchema
from math import sqrt

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

split_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{4})/(\d{2})/(\d{2})/.*')
unified_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{8})/.*')

# 'http://techcrunch.com/2013/02/13/melodrama' -> ('2013', '02', '13')
# 'http://allthingsd.com/20130213/business_money_yay' -> ('2013', '02', '13')
@outputSchema("date: (year: chararray, month: chararray, day: chararray)")
@null_if_input_null
def get_article_date_from_url(url):
    try:
        split_date = re.search(split_date_in_url_pattern, url)
        if split_date:
            parts = split_date.groups()
            return (parts[0], parts[1], parts[2])

        unified_date = re.search(unified_date_in_url_pattern, url)
        if unified_date:
            date = unified_date.group(1)
            return (date[0:4], date[4:6], date[6:8])
    except:
        return None

    return None

# The history of word frequency for each month stored in the relation "freqs_by_word_ordered" in common_crawl_trending_topics.pig
# skips months where that word did not occur. Therefore, when calculating a word's "frequency velocity", we test whether input tuples
# are from consecutive months: if they are, we look at them relative to each other; if they are not, we know that the actual
# preceding month had 0 occurrences of the word.
def consecutive_months(last_year, last_month_of_year, year, month_of_year):
    if (year == last_year and month_of_year == last_month_of_year + 1) or (year == last_year + 1 and last_month_of_year == 12 and month_of_year == 1):
        return True
    return False

# cur = 4.0, prev = 2.0 -> returns 2.0
# cur = 2.0, prev = 4.0 -> returns -2.0
# This keeps proportional increases and decreases on the same weighting scale 
def word_relative_velocity(cur, prev):
    if cur > 0.0 and prev > 0.0:
        # should never be negative even without the abs, but who knows what weirdness floating point arithmetic can cause
        ratio = abs(1.0 + ((cur - prev) / prev))
        return sqrt(ratio) if cur >= prev else -sqrt(1.0/ratio)
    else:
        return None

# Word frequencies are usually very small, so we use a multiplier
# to make weights easoer to interpret when debugging
def word_velocity_weight(abs_vel, rel_vel):
    mult = 1000000.0
    return mult * abs_vel * rel_vel * (-1.0 if abs_vel < 0.0 else 1.0)

# A word's "velocity" is a metric we define to describe how it's frequency is changing over time
# We take into account both absolute changes in frequency and relative changes of frequency
@outputSchema("word_velocities: {t: (word: chararray, month: chararray, frequency: double, abs_vel: double, rel_vel: double, velocity: double)}")
@null_if_input_null
def word_velocity_over_time(word, trend):
    velocities = []

    last_year = 0
    last_month_of_year = 1
    last_frequency = 0.0

    for t in trend:
        year = int(t[0][0:4])
        month_of_year = int(t[0][5:7])
        
        if consecutive_months(last_year, last_month_of_year, year, month_of_year):
            absolute_velocity = t[1] - last_frequency
            relative_velocity = word_relative_velocity(t[1], last_frequency)
        else:
            absolute_velocity = t[1]
            relative_velocity = 1.0

        velocities.append((
                            word, t[0], t[1],
                            absolute_velocity,
                            relative_velocity,
                            word_velocity_weight(absolute_velocity, relative_velocity)
                         ))

        last_year = year
        last_month_of_year = month_of_year
        last_frequency = t[1]

    return velocities
