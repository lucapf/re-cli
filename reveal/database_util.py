from sqlite3.dbapi2 import Connection
from reveal import (logging, db_schema)
from typing import Any, List, Optional, Set
import hashlib
import sqlite3


migrations = "create table migrations(statement_sha text primary key)"
database_file = "reveal.sqlite3"

def _connect():
    logging.debug("connect")
    return sqlite3.connect(database_file)

def get_connection():
    return _connect()

def init_database():
    with _connect() as conn:
        cursor = conn.cursor()
        statement = "select name from sqlite_master where type='table' and name='migrations'"
        if cursor.execute(statement).fetchone() == None:
            logging.debug(f"migration table does not exists")
            cursor.execute(migrations)
        for s in db_schema.sql_statements:
            statement_sha  = hashlib.sha1(s.encode("UTF-8")).hexdigest()
            logging.debug(f"sha:{statement_sha} statement ${s} ")
            sql_query  = f"select statement_sha from migrations where statement_sha='{statement_sha}'"
            if cursor.execute(sql_query).fetchone() is None:
                cursor.execute(s)
                statement= f"insert into migrations (statement_sha) values ('{statement_sha}')"
                cursor.execute(statement)
        conn.commit()

def execute_query_statement(sqlStatement: str, size:Optional[int] = None ) -> List[Any]:
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(sqlStatement)
        if size is None:
            return  cursor.fetchall() 
        else:
            return cursor.fetchmany(size)

def execute_insert_statement(sql_insert_template: str,values: Optional[tuple] = None,  conn: Optional[Connection] = None, do_commit: bool = True):
    if conn is None:
        conn = _connect()
    if values is None:
        conn.execute(sql_insert_template)
    else:
        conn.execute(sql_insert_template, values)
    if do_commit:
        conn.commit()
