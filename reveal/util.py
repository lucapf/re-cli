from datetime import datetime
import os


dump_dir = "error_dumps"

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



    
