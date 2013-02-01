from pig_util import outputSchema

COFFEE_SNOB_PHRASES = set((\
    'espresso', 'cappucino', 'macchiato', 'latte', 'cortado', 'pour over', 'barista', 
    'flat white', 'siphon pot', 'woodneck', 'french press', 'arabica', 'chemex', 
    'frothed', 'la marzocco', 'mazzer', 'la pavoni', 'nespresso', 'rancilio silvia', 'hario',
    'intelligentsia', 'counter culture', 'barismo', 'sightglass', 'blue bottle', 'stumptown',
    'single origin', 'coffee beans', 'coffee grinder', 'lavazza', 'coffeegeek'\
))

@outputSchema('is_coffee_tweet:int')
def is_coffee_tweet(text):
    """
    Is the given text indicative of coffee snobbery?
    """
    if not text:
        return 0
    
    lowercased = text.lower()
    return 1 if any((True for phrase in COFFEE_SNOB_PHRASES if phrase in lowercased)) else 0
