import csv
import os
from typing import List, Optional
from datetime import datetime
from psycopg import sql
import requests
from reveal import logging,database_util, util

pulse_transactions_csv_url = "https://www.dubaipulse.gov.ae/dataset/3b25a6f5-9077-49d7-8a1e-bc6d5dea88fd/resource/a37511b0-ea36-485d-bccd-2d6cb24507e7/download/transactions.csv"
transaction_file_name = f"transactions_{datetime.now().strftime('%Y-%m-%d')}.csv"
COLUMNS = [
            'transaction_id', 
            'procedure_id', 
            'trans_group_id', 
            'trans_group',
            'procedure_name', 
            'instance_date', 
            'property_type_id', 
            'property_type',
            'property_sub_type_id', 
            'property_sub_type', 
            'property_usage',
            'reg_type_id', 
            'reg_type', 
            'area_id', 
            'area_name', 
            'building_name',
            'project_number', 
            'project_name', 
            'master_project', 
            'rooms',
            'has_parking', 
            'procedure_area', 
            'actual_worth', 
            'size_sqft',
            'price_sqft',
            'meter_sale_price',
            'rent_value', 
            'meter_rent_price',
            'bedrooms'
          ]

def download_transaction() -> str|None:
    response = requests.get(pulse_transactions_csv_url)
    if response.status_code == 200:
        with open(transaction_file_name, "w") as transaction_file:
            transaction_file.write(response.text)
            return transaction_file_name
    return None

 

def load(transaction_file_path: str) -> int|None:
    '''
        read transactions from the dictionary
    '''
    if not os.path.exists(transaction_file_path):
        logging.err(f"path {transaction_file_path} does not exists")
        return None
    with open(transaction_file_path, "r") as pulse_transaction_file:
        transaction_reader = csv.DictReader(pulse_transaction_file)
        logging.info("CSV read")
        return insert(transaction_reader)

def clean():
    conn = database_util.get_connection()
    cursor = conn.cursor()
    cursor.execute("delete from pulse")
    conn.commit()

def insert(transactions: Optional[csv.DictReader]) -> int:

    if transactions is None:
        logging.info("insert pulse transaction. No data provided.")
        return 0
    conn = database_util.get_connection()
    before = database_util.fetchone("select count(*) from pulse",None, conn)[0]
    logging.info(f"existing elements {before}")
    cursor = conn.cursor()
    counter = 0
    sql_insert_statement = None
    pulse_transactions: List[tuple]  = []
    for t in transactions:
#        if not t['procedure_area'].isdigit():
#            logging.debug(f"procedure area format ${t['procedure_area']} not valid, skip ")
#            continue
#        if not t['actual_worth'].isdigit():
#            logging.debug(f"price format ${t['actual_worth']} not valid, skip ")
#            continue

        procedure_area = float(t['procedure_area'])
        price = float(t['actual_worth'])
        if t['procedure_name_en'] != "Sell":
            continue
        if procedure_area < 10:
            logging.warn(f"procedure area value not valid {procedure_area}")
            continue
        size_sqft =  util.mq_to_sqft(procedure_area)
        if size_sqft == 0.0 or size_sqft is None:
            logging.warn(f"size_sqft = 0! procedure area {procedure_area} transaction_id: {t['transaction_id']}") 
            continue
        price_sqft= int(price / size_sqft )
        #logging.debug(f"SQFT  price_sqft: {price_sqft} size_sqft: {size_sqft} original price: {price}")
        pulse_transactions.append(( 
            str(t['transaction_id']), 
            util.nullsafe_to_int(t['procedure_id']),
            util.nullsafe_to_int(t['trans_group_id']),
            str(t['trans_group_en']),
            t['procedure_name_en'],
            util.date_DMY_to_iso(t["instance_date"]),
            util.nullsafe_to_int(t['property_type_id']), 
            t['property_type_en'],
            util.nullsafe_to_int(t['property_sub_type_id']), 
            t['property_sub_type_en'], 
            t['property_usage_en'],
            util.nullsafe_to_int(t['reg_type_id']),
            t['reg_type_en'],
            util.nullsafe_to_int(t['area_id']),
            t['area_name_en'],
            t['building_name_en'],
            util.nullsafe_to_int(t['project_number']),
            t['project_name_en'],
            t['master_project_en'],
            t['rooms_en'],
            util.nullsafe_to_int(t['has_parking']),
            procedure_area,
            price,
            size_sqft,
            price_sqft,
            util.nullsafe_to_float(t['meter_sale_price']),
            util.nullsafe_to_float(t['rent_value']),
            util.nullsafe_to_float(t['meter_rent_price']),
            util.bedrooms_pulse_to_propertyfinder(t['rooms_en']) 
            ))
        counter +=1
        if (counter % 50000 == 0):
            logging.debug(f"processed {counter}")
    logging.debug("data ready")
    sql_insert_statement = f"insert into pulse ({','.join(COLUMNS)}) values ({','.join(['%s']*len(COLUMNS))}) on conflict do nothing"
    logging.debug(f"sqlstatement: {sql_insert_statement}")
    logging.debug(f"pulse: {len(pulse_transactions)}")

    cursor.executemany(sql.SQL(sql_insert_statement),(pulse_transactions,))
    database_util.execute_insert_statement("update pulse set building_name = null where replace(building_name,' ','')=''", None, conn)
    conn.commit()
    logging.debug("completed!")
    after =database_util.fetchone("select count(*) from pulse",None, conn)[0]
    sync_towers = """
            insert into pulse_tower_mapping (master_project, building_name) 
                    select master_project, building_name 
                    from pulse 
                    where master_project in (select distinct pulse_master_project from propertyfinder_pulse_area_mapping) and building_name is not null
            on conflict do nothing
    """
    database_util.execute_insert_statement(sync_towers, None, conn)
    conn.close()
    new_transactions = after - before
    logging.info(f"{transaction_file_name} added {new_transactions} total: {new_transactions}")
    return new_transactions



def _map_transaction (transactions: list[dict]) ->list[int]:
    return list({v["transaction_id"] for v in transactions})



