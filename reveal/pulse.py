import csv
import os
from sqlite3.dbapi2 import Connection
from typing import List, Optional
from datetime import datetime
import requests
from reveal import logging,database_util, util

pulse_transactions_csv_url = "https://www.dubaipulse.gov.ae/dataset/3b25a6f5-9077-49d7-8a1e-bc6d5dea88fd/resource/a37511b0-ea36-485d-bccd-2d6cb24507e7/download/transactions.csv"
transaction_file_name = f"transactions_{datetime.now().strftime('%Y-%m-%d')}.csv"

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

def insert(transactions: Optional[csv.DictReader]) -> int:
    if transactions is None:
        logging.info("insert pulse transaction. No data provided.")
        return 0
    before=database_util.fetchone("select count(*) from pulse")[0]
    logging.info(f"existing elements {before}")
    conn = database_util.get_connection()
    cursor = conn.cursor()
    counter = 0
    sql_insert_statement = None
    for t in transactions:
        if sql_insert_statement == None: 
            sql_insert_statement = "insert into pulse ({columns}) values({values}) on conflict do nothing" \
            .format(columns=','.join(t.keys()), values=','.join(['?']*len(t.keys())))

        t["instance_date"] = util.date_DMY_to_iso(t["instance_date"])
        counter +=1
        cursor.execute(sql_insert_statement, tuple(t.values()))
        if (counter % 10000 == 0):
            logging.debug(f"commit {counter}")
    logging.debug(f"completed {counter}")
    conn.commit()
    after =database_util.fetchone("select count(*) from pulse",None, conn)[0]
    new_transactions = after - before
    logging.info(f"{transaction_file_name} added {new_transactions} total: {new_transactions}")
    return new_transactions



def _map_transaction (transactions: list[dict]) ->list[int]:
    return list({v["transaction_id"] for v in transactions})



