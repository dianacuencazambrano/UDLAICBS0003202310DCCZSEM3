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
    return datetime.strptime(date_string, '%Y-%m-%d')