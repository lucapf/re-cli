from datetime import datetime
import os


dump_dir = "error_dumps"

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

def date_DMY_to_iso(instance_date:str) -> str:
    tokenized = instance_date.split("-")
    return f"{tokenized[2]}-{tokenized[1]}-{tokenized[0]}"



    
