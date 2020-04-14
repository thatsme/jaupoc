import json

def dict_to_binary(the_dict):
    mstring = json.dumps(the_dict)
    mbinary = ' '.join(format(ord(letter), 'b') for letter in mstring)
    return mbinary

def binary_to_dict(the_binary):
    jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
    d = json.loads(jsn)  
    return d

def dict_to_string(the_dict):
    mstring = json.dumps(the_dict)
    return mstring

def string_to_dict(the_string):
    d = json.loads(the_string)  
    return d
