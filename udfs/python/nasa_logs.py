import re
from datetime import datetime, timedelta
from pig_util import outputSchema

months_dict = { 'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12 }
clf_timestamp_pattern = re.compile('(\d{2})/(.{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s+([+-]\d{4})')

# Convert a Common Log Format timestamp, ex. 01/Jul/1995:00:00:01 -0400
# into an ISO-8601 timestamp at UTC, ex. 1995-06-30T22:00:01Z
@outputSchema("iso_time: chararray")
def clf_timestamp_to_iso(timestamp):
    parts = clf_timestamp_pattern.search(timestamp).group(1, 2, 3, 4, 5, 6, 7);

    year = int(parts[2])
    month = months_dict[parts[1]]
    day = int(parts[0])
    hour = int(parts[3])
    minute = int(parts[4])
    second = int(parts[5])

    dt = datetime(year, month, day, hour, minute, second)

    tz = parts[6]
    tzMult = 1 if tz[0] == '+' else -1
    tzHours = tzMult * int(tz[1:3])
    tzMinutes = tzMult * int(tz[3:5])

    dt += timedelta(hours=tzHours, minutes=tzMinutes)
    return dt.isoformat()

# Get rid of duplicated date field in the output of nasa_logs.pig. See comment in pigscript.
@outputSchema("top_uris: {t: (uri: chararray, num_requests: long, num_bytes: long)}")
def cleanup_output(top_uris):
    return [(t[0][0], t[1], t[2]) for t in top_uris]
