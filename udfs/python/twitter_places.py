from pig_util import outputSchema

@outputSchema('us_state:chararray')
def us_state(full_place):
    """
    Parse the full place name (e.g. Wheeling, WV) to the state name (WV).
    """
    # find the last comma in the string
    last_comma_idx = full_place.rfind(',')
    if last_comma_idx > 0:
        # grab just the state name
        state = full_place[last_comma_idx+1:].strip() 
        print 'Found state %s in full_place: %s' % (state, full_place)
        return state
    else:
        print 'No state in full_place: %s' % full_place
        return None
