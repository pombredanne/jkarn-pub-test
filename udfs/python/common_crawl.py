import re
from math import sqrt
from pig_util import outputSchema

split_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{4})/(\d{2})/(\d{2})/.*')
unified_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{8})/.*')

@outputSchema("date: (year: int, month: int, day: int)")
def get_article_date_from_url(url):
    if not url:
      return None

    try:
        split_date = re.search(split_date_in_url_pattern, url)
        if split_date:
            parts = split_date.groups()
            return (int(parts[0]), int(parts[1]), int(parts[2]))

        unified_date = re.search(unified_date_in_url_pattern, url)
        if unified_date:
            date = unified_date.group(1)
            return (int(date[0:4]), int(date[4:6]), int(date[6:8]))
    except:
        return None

    return None

@outputSchema("word_frequency_over_timespan: {t: (word: chararray, month: chararray, occurrences: long, frequency: double)}")
def fill_timespan(word_frequencies, start_month, end_month):
    word = word_frequencies[0][0]
    start_year, start_month = int(start_month[0:4]), int(start_month[5:7])
    end_year, end_month = int(end_month[0:4]), int(end_month[5:7])
    freq_dict = { t[1]:t for t in word_frequencies }

    timespan = ['%04d-%02d' % (year, month) for year in range(start_year, end_year+1) for month in range(1, 13)]
    timespan = timespan[start_month-1:len(timespan)-end_month]

    return [freq_dict[month] if month in freq_dict else (word, month, 0L, 0.0) for month in timespan]

def word_relative_velocity(cur, prev):
    if cur > 0.0 and prev > 0.0:
        ratio = 1.0 + ((cur - prev) / prev)
        return ratio if cur >= prev else -1.0/ratio
    else:
        return None

def word_velocity_weight(abs_vel, rel_vel):
    mult = 10000.0
    if rel_vel:
        return mult * sqrt(abs(abs_vel)) * rel_vel
    else:
        return mult * mult * abs_vel

@outputSchema("word_velocities: {t: (word: chararray, month: chararray, frequency: double, velocity: double)}")
def word_velocities_over_time(word_frequencies):
    absolute_velocities = [
                            (word_frequencies[i][2] - word_frequencies[i-1][2])
                            for i in xrange(1, len(word_frequencies))
                          ]
    relative_velocities = [
                            word_relative_velocity(word_frequencies[i][2], word_frequencies[i-1][2])
                            for i in xrange(1, len(word_frequencies))
                          ]

    return  [
              (
                word_frequencies[i][0], word_frequencies[i][1], word_frequencies[i][2],
                word_velocity_weight(absolute_velocities[i-1], relative_velocities[i-1])
              )
              for i in xrange(1, len(word_frequencies))
            ]
