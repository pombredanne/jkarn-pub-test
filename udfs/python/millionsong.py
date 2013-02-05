from pig_util import outputSchema

# 
# This is where we write python funtions that we can call from pig.
# Pig needs to know the schema of the function, which we specify using
# the @outputSchema decorator
#
@outputSchema('density:double')
def density(segments_start, duration):
    """
    Calculate the density of a song.  We do this by computing the
    number of song segments per second. 
    """
    # segements_start is an array of times that each sound in the song starts
    # grab the length of it to get the number of sounds
    num_segments = len(segments_start.split(","))

    # print the number of segments for debugging
    # this will appear whenever we ILLUSTRATE to help us debug
    print 'Number of segments: %s, duration: %s' % (num_segments, duration)

    # calculate the segments per second
    return num_segments / duration
