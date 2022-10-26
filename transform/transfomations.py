from datetime import datetime

def join_2_strings(string1, string2):
    return f"{string1} {string2}"

def obt_gender(gen):
    if gen == 'M':
        return 'MASCULINO'
    elif gen == 'F':
        return 'FEMENINO'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    return datetime.strptime(date_string, '%d-%b-%y')

def str_2_int(string1):
    if type(string1) is str:
        num = int(string1)
    elif type(string1) is int:
        num = string1
    else:
        num = "NaN"
    return num

def str_2_float(string1):
    if type(string1) is str:
        num = float(string1)
    elif type(string1) is float:
        num = string1
    else:
        num = "NaN"
    return num