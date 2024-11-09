import csv
import os
from sqlite3.dbapi2 import Connection
from typing import List, Optional
from reveal import logging,database_util, util

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
    existing_ids_sql ="select transaction_id from pulse_data where transaction_id in ({seq})"
    existing_ids = database_util.execute_query_statement( \
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
    sql_insert_statement = "insert into pulse_data ({columns}) values({values}) " \
        .format(columns=','.join(t.keys()), values=','.join(['?']*len(t.keys())))
    database_util.execute_insert_statement(sql_insert_statement, tuple(t.values()))


def _map_transaction (transactions: list[dict]) ->list[int]:
    return list({v["transaction_id"] for v in transactions})



