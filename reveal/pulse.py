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

 

def load(transaction_file_path: str) -> Optional[List[dict]]:
    '''
        read transactions from the dictionary
    '''
    transaction_dictionary = list()
    if not os.path.exists(transaction_file_path):
        logging.err(f"path {transaction_file_path} does not exists")
        return None
    with open(transaction_file_path, "r") as pulse_transaction_file:
        transaction_reader= csv.DictReader(pulse_transaction_file)
        for entry in transaction_reader:
            entry["instance_date"] = util.date_DMY_to_iso(entry["instance_date"])
            transaction_dictionary.append(entry)
    return transaction_dictionary

def insert(transactions: Optional[list[dict]]) -> int:
    if transactions is None:
        logging.info("insert pulse transaction. No data provided.")
        return 0
    ids = _map_transaction(transactions)
    existing_ids_sql ="select transaction_id from pulse where transaction_id in ({seq})"
    existing_ids = database_util.fetch( \
            existing_ids_sql.format(seq=','.join(['?']*len(ids))), tuple(ids))
    if len(ids) == len(existing_ids):
        logging.info("all ids provided are already present")
        return 0
    con = database_util.get_connection()
    for t in transactions:
        if t["transaction_id"] not in existing_ids:
            _insert_transaction(t, con)
    con.commit()
    return len(ids) - len(existing_ids)

def _insert_transaction(t:dict, conn: Connection):
    sql_insert_statement = "insert into pulse ({columns}) values({values}) " \
        .format(columns=','.join(t.keys()), values=','.join(['?']*len(t.keys())))
    database_util.execute_insert_statement(sql_insert_statement, tuple(t.values()))


def _map_transaction (transactions: list[dict]) ->list[int]:
    return list({v["transaction_id"] for v in transactions})



