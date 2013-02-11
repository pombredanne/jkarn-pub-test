from __future__ import absolute_import, division, print_function, unicode_literals

import re

from pig_util import outputSchema

@outputSchema("relevance: int")
def relevance(text, pattern_str):
    if text:
        pattern = re.compile(pattern_str)
        return len(re.findall(pattern, text))
    else:
        return 0
