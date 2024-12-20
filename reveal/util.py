from datetime import datetime, date
from difflib import SequenceMatcher
import os
import json


dump_dir = "error_dumps"

def sqft_to_mq(size: int) -> int:
    """
    sqft(size) -> mq
    """
    return int(size * 0.0929)


def mq_to_sqft(size: float) -> int:
    """
    mq(size) -> sqft
    """
    return int(size / 0.0929)

def nullsafe_to_float(content: str|None) -> float|None:
    if content is None:
        return None
    strip_content = content.strip() 
    if strip_content == '' or strip_content == 'null':
        return None
    return float(content)


def nullsafe_to_int(content: str|None) -> int|None:
    if content is None:
        return None
    strip_content = content.strip() 
    if strip_content == '' or strip_content == 'null':
        return None
    if not content.isdigit():
        return None
    return int (content)


def dump_error_file(content: str, extension: str) -> str:
    '''
        dump the content in a file into th error_dumps directory and return the filename
    '''
    dump_filename = f"{datetime.now().strftime('%s')}.{extension}"
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_file_path = os.path.join(dump_dir, dump_filename)
    with open(dump_file_path, "w") as dump_file:
        dump_file.write(content)
    return dump_file_path 

def is_empty_list(l: list):
    return l is None or len(l) == 0

def date_DMY_to_iso(instance_date:str) -> str:
    tokenized = instance_date.split("-")
    return f"{tokenized[2]}-{tokenized[1]}-{tokenized[0]}"

def bedrooms_pulse_to_propertyfinder(pulse_bedrooms: str) -> str:
    if pulse_bedrooms == 'Studio':
        return "studio"
    if pulse_bedrooms == '1 B/R' or pulse_bedrooms == 'Single Room':
        return "1"
    if pulse_bedrooms == '2 B/R':
        return "2"
    if pulse_bedrooms == '3 B/R':
        return "3"
    if pulse_bedrooms == '4 B/R':
        return "4"
    if pulse_bedrooms == '5 B/R':
        return "5"
    if pulse_bedrooms == '6 B/R':
        return "6"
    if pulse_bedrooms == 'PENTHOUSE':
        return "7+"
    return f"{pulse_bedrooms} Not Supported"    

def bedrooms_propertyfinder_to_pulse(propertyfinder_rooms: str)  -> str:
    if propertyfinder_rooms.isdigit():
        return f"{propertyfinder_rooms} B/R"
    if propertyfinder_rooms == "studio":
        return "Studio"
    if propertyfinder_rooms == "7+":
        return "7 B/R"
    return propertyfinder_rooms

  

