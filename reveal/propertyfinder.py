from reveal import ( database_util, logging, util)
import requests
import re
import json
from typing import Optional 


headers = {

        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
get_property_pattern = '"searchResult":(.*),"_nextI18Next"'
get_property_detail_pattern = '"__NEXT_DATA__".*>(.*)</script><div id="__next">'

def get_ads(max_pages: int = 300) -> int:
    new_items = 0
    for current_page in range(1,max_pages):
        logging.info(f"extract page {current_page}")
        raw_data = __get_ads(current_page)
        if raw_data is None:
            logging.warn("wrong status code, exit")
            break
        json_data = _extract_data(raw_data)
        if json_data is None:
            logging.warn(f"{current_page} no data - abort")
            break
        json_data = _filter_out_non_properties(json_data)
        if json_data is None:
            logging.info(f"{current_page} workable no data - continue")
            continue
        page_added_items = _save(json_data) 
        new_items += page_added_items
        if page_added_items == 0:
            logging.info(f"all fetched data already present  - ingestion completed addded {new_items} items")
            return new_items 
    return new_items

def __get_ads( page_number: int) -> Optional[str]:
        url = f"https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=nd&page={page_number}"  # newest
        response = requests.get(url, headers=headers)
        logging.info(f"response status_code: {response.status_code}")
        if response.status_code != 200:
            logging.warn(f"wrong status code {response.status_code} page {page_number}  url {url} content {util.dump_error_file(response.text, 'html')} ")
            return None 
        else:
            return response.text

def _extract_data(html_content: str) ->Optional[list[dict]]:
    '''
    list of property objects from the html content
    '''
    extracted_data = re.search(get_property_pattern, html_content)
    if extracted_data is None:
        logging.warn(f"cannot extract property info. Pls check the file dump {util.dump_error_file(html_content, 'html')} ")
        return None
    extracted_data = extracted_data.group(1)
    json_data = json.loads(extracted_data)
    if json_data.get("listings")  is None:
        return None
    else:
        return json_data["listings"]

def _filter_out_non_properties(property_data:list|None) -> list|None:
    if property_data is None:
        return None
    pf_filtered = list()
    for l in property_data:
        if l.get("listing_type") == "property":
            if l["property"].get("property_type") != 'Land':
                pf_filtered.append(l)
    return pf_filtered 

def _map_db_fields(a: dict) -> dict:
     item = dict()
     item["id"] =  a["property"]["id"]
     item["type"] =  a["property"]["property_type"]
     item["price"] =  int(a["property"]["price"]["value"])
     item["size"] =  int(a["property"]["size"]["value"])
     item["bedrooms"] =  a["property"]["bedrooms"]
     item["bathrooms"] =  a["property"]["bathrooms"]
     item["price_per_sqft"] = float(item["price"] / item["size"])
     for l in a["property"]["location_tree"]:
         key = str(l["type"]).lower()
         value = l["name"]
         item[key] = value 
     item["location_slug"] =  a["property"]["location"].get("slug")
     item["location_name"] =  a["property"]["location"].get("full_name")
     coordinates  = a["property"]["location"].get("coordinates")
     if coordinates is not None:
         item["latitude"] =  coordinates.get("lat")
         item["longitude"] =  coordinates.get("lon")
     item["listed_date"] =  a["property"]["listed_date"]
     item["url"] =  a["property"]["share_url"]
     return item
     
def _save(ads:list) -> int:
    '''
    save data into the propertyfinder table.
    Return the number added elements
    '''
    insert_template = "insert into propertyfinder ({columns}) values ({values}) on conflict do nothing"
    conn = database_util.get_connection() 
    existing_items = database_util.fetchone( \
            "select count(*) from propertyfinder", None, conn)[0]
    for a in ads:
        ads_summary = _map_db_fields(a)
        sql_insert=insert_template.format(
                columns = ','.join(ads_summary.keys()), 
                values= ','.join(["?"]*len(ads_summary.keys())))
        logging.debug(f"insert statement: {sql_insert} -- values: {ads_summary.values()}")
        database_util.execute_insert_statement(sql_insert,tuple(ads_summary.values()), conn ) 
    conn.commit()

    new_items = database_util.fetchone( \
            "select count(*) from propertyfinder", None, conn)[0]
    return new_items - existing_items
