import re
from pig_util import outputSchema

# Calculate how "relevant" a text is to a regex pattern,
# defining relevance as the number of matches in the text to that pattern
@outputSchema("relevance: int")
def relevance(text, pattern_str):
    if text:
        pattern = re.compile(pattern_str)
        return len(re.findall(pattern, text))
    else:
        return 0
