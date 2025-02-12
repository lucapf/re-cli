from reveal import ( database_util, logging ,util, job)
from reveal.job import JobExecution
import requests
from requests.exceptions import ConnectionError
import re
import json
from psycopg  import Connection
from typing import Optional 
import time
import traceback
import os


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36" 
          }
get_property_pattern = '"searchResult":(.*),"_nextI18Next"'
get_property_detail_pattern = '"__NEXT_DATA__".*>(.*)</script><div id="__next">'
ads_dir = "ads"

def get_ads(max_pages: int, job_execution: JobExecution) -> int:
    new_items = 0
    try:
        if not os.path.exists(ads_dir):
            os.mkdir(ads_dir)
        for current_page in range(1,max_pages):
            job.progress(job_execution, f"process page {current_page}/ {max_pages}")
            raw_data = __get_ads(current_page)
            if raw_data is None:
                job_execution.error("wrong status code, exit")
                break
            json_data = _extract_data(raw_data, current_page)
            if json_data is None:
                job_execution.error(f"{current_page} no data - abort")
                break
            json_data = _filter_out_non_properties(json_data)
            with open(f"{ads_dir}/ads_{current_page}.json", "w") as ad_file:
                json.dump(json_data, ad_file)

            if json_data is None:
                job.progress(job_execution,f"{current_page} workable no data - continue")
                continue
            page_added_items = _save(json_data) 
            new_items += page_added_items
            if page_added_items == 0 and current_page != 2: # skip page #2 due strange issue in Proprtyfinder 
                job.progress(job_execution,f"all fetched data already present  - ingestion completed addded {new_items} items")
                return new_items 
            job.progress(job_execution,"start sync")
            _sync()
            job_execution.success(f"execution complete, added {new_items} ads")
    except Exception:
        logging.err(f"cannot process ads : {traceback.format_exc()}")
        job_execution.error("cannot process the ads pls check logs")
    finally:
        job.complete(job_execution)
    
    return new_items

def __get_ads( page_number: int) -> Optional[str]:
        url = f"https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=nd&page={page_number}"  # newest
        try: 
            response = requests.get(url, headers=headers)
        except ConnectionError:
            logging.err("connection error occurred, waiting and retry")
            time.sleep(2)
            response = requests.get(url, headers=headers)
            
        logging.info(f"request: {url} - response status_code: {response.status_code}")
        if response.status_code != 200:
            logging.warn(f"wrong status code {response.status_code} page {page_number}  url {url} content {util.dump_error_file(response.text, 'html')} ")
            return None 
        else:
            return response.text

def _extract_data(html_content: str, page_number: int) ->Optional[list[dict]]:
    '''
    list of property objects from the html content
    '''
    extracted_data = re.search(get_property_pattern, html_content)
    if extracted_data is None:
        logging.warn(f"cannot extract property info. Pls check the file dump {util.dump_error_file(html_content, 'html')} ")
        return None
    extracted_data = extracted_data.group(1)
    json_data = json.loads(extracted_data)
    # with open(f"ads_{page_number}.json", "w") as dump_file:
    #     dump_file.write(json.dumps(json_data))
    if json_data.get("listings")  is None:
        return None
    else:
        return json_data["listings"]

def _filter_out_non_properties(property_data:list|None) -> list|None:
    if property_data is None:
        return None
    pf_filtered = list()
    for pd in property_data:
        if pd.get("listing_type") == "property":
            if pd["property"].get("property_type") != 'Land':
                pf_filtered.append(pd)
    return pf_filtered 

def _map_db_fields(a: dict) -> dict:
     item = dict()
     item["id"] =  a["property"]["id"]
     item["type"] =  a["property"]["property_type"]
     item["price"] =  int(a["property"]["price"]["value"])
     item["size"] =  int(a["property"]["size"]["value"])
     item["bedrooms"] =  a["property"]["bedrooms"]
     item["bathrooms"] =  a["property"]["bathrooms"]
     item["description"] = a["property"]["description"]
     item["price_sqft"] = float(item["price"] / item["size"])
     for ltree in a["property"]["location_tree"]:
         key = str(ltree["type"]).lower()
         value = ltree["name"]
         item[key] = value 
     item["location_slug"] =  a["property"]["location"].get("slug")
     item["location_name"] =  a["property"]["location"].get("full_name")
     coordinates  = a["property"]["location"].get("coordinates")
     if coordinates is not None:
         item["latitude"] =  coordinates.get("lat")
         item["longitude"] =  coordinates.get("lon")
     item["listed_date"] =  a["property"]["listed_date"]
     item["url"] =  a["property"]["share_url"]
     item["completion_status"] =  a["property"]["completion_status"]
     return item
     
def clean():
    database_util.execute_insert_statement('delete from propertyfinder_tower_mapping')
    database_util.execute_insert_statement('delete from propertyfinder')

def _sync(conn: Connection| None = None):
    connection = conn
    if connection is None:
        connection = database_util.get_connection()
    sync_towers="""
        insert into propertyfinder_tower_mapping (community, tower) 
            select distinct community, tower 
            from propertyfinder 
            where community in (select pf_community from propertyfinder_pulse_area_mapping)
            on conflict do nothing
    """
    database_util.execute_insert_statement(sync_towers, None, connection)
    logging.debug("propertyfinder tower mapping synced")
    if conn is None:
        connection.commit()
        connection.close()


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
                values= ','.join(["%s"]*len(ads_summary.keys())))
        # logging.debug(f"insert statement: {sql_insert} -- values: {ads_summary.values()}")
        database_util.execute_insert_statement(sql_insert,tuple(ads_summary.values()), conn ) 
    conn.commit()
    then_items = database_util.fetchone( \
            "select count(*) from propertyfinder", None, conn)[0]
    added_items = then_items - existing_items
    conn.close()
    logging.debug(f"existing: {existing_items}, then: {then_items} added items: {added_items}")
    return added_items 
