from pig_util import outputSchema
    
@outputSchema('null_to_zero:double')
def null_to_zero(value):
    if value is None:
        return 0
    return value
    
@outputSchema('create_penalty:double')    
def create_penalty(value):
    if value is None:
        return 300
    return value