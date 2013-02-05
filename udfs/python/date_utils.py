from pig_util import outputSchema

@outputSchema('decade:chararray')
def decade(year):
    """
    Get the decade, given a year.
    
    e.g. for 1998 -> '1990s'
    """
    try:
        base_decade_year = int(year) - (int(year) % 10)
        decade_str = '%ss' % base_decade_year
        print 'input year: %s, decade: %s' % (year, decade_str)
        return decade_str
    except ValueError:
        return None
