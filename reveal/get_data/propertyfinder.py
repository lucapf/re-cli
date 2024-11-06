from reveal import ( logging, util)
import requests
import re
import json
from typing import Optional 
import datetime


headers = {

        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
get_property_pattern = '"searchResult":(.*),"_nextI18Next"'
get_property_detail_pattern = '"__NEXT_DATA__".*>(.*)</script><div id="__next">'



def __get_ads( page_number: int) -> Optional[str]:
        url = f"https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=nd&page={page_number}"  # newest
        response = requests.get(url, headers=headers)
        logging.info(f"response status_code: {response.status_code}")
        if response.status_code != 200:
            logging.warn(f"wrong status code {response.status_code} page {page_number}  url {url} content {util.dump_error_file(response.text, 'html')} ")
            return None 
        else:
            return response.text

def __extract(html_content: str) ->Optional[dict]:
    extracted_data = re.search(get_property_pattern, html_content)
    if extracted_data is None:
        logging.warn(f"cannot extract property info. Pls check the file dump {util.dump_error_file(html_content, 'html')} ")
        return None
    extracted_data = extracted_data.group(1)
    return json.loads(extracted_data)

def __save(raw_data:dict) -> bool:
    '''
    save data into the propertyfinder table.
    If all ads are already in the databae return True
    otherwise False
    '''
    return False

def get_ads():
    for current_page in range(1,300):
        logging.info(f"extract page {current_page}")
        raw_data = __get_ads(current_page)
        if raw_data is None:
            logging.warn("wrong status code, exit")
            break
        json_data = __extract(raw_data)
        if json_data is None:
            logging.warn("no data")
            break
        if __save(json_data):
            logging.info("completed")
            break




       
