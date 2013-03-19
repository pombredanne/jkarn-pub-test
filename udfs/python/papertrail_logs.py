from pig_util import outputSchema

@outputSchema('message:chararray')
def tab_flatten(str):
    str = str.replace('\t', ': ')
    return str[11:]