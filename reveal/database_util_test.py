import unittest
import os
import random, string
from datetime import datetime
from reveal import logging
from reveal import database_util

insert_statement = "insert into propertyfinder_areas(area) values (?)"

class DatabaseUtilTest(unittest.TestCase):

    def test_create_database(self):
        if (os.path.exists(database_util.database_file)):
            os.remove(database_util.database_file)
        database_util.init_database()
        check_migrations_exists = "select name from sqlite_master where type='table' and name='migrations'"
        row = database_util.execute_query_statement(check_migrations_exists)
        logging.info(f"table name {row[0][0]}")
        self.assertIsNotNone(row)
        areas = tuple(["test"]) 
        database_util.execute_insert_statement(insert_statement, areas)
        check_data = "select area from propertyfinder_areas"
        row = database_util.execute_query_statement(check_data)
        self.assertEqual(len(row), 1)
        database_util.init_database()
        check_data = "select area from propertyfinder_areas"
        row = database_util.execute_query_statement(check_data)
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], areas[0])

    def test_bulk_insert(self):
        areas = []
        letters = string.ascii_lowercase
        elements = 10000
        for i in range(elements):
            word = (''.join(random.choice(letters) for j in range(10)))
            areas.append([word])
        start  = datetime.timestamp(datetime.now()) 
        database_util.execute_insert_statement("delete from propertyfinder_areas")
        connection = database_util.get_connection()
        for w in areas:
            database_util.execute_insert_statement(insert_statement, w, connection, False)
        connection.commit()
        end = datetime.timestamp(datetime.now())
        logging.info(f"completed in {end - start} milliseconds")
        query = "select count(*) from propertyfinder_areas"
        row = database_util.execute_query_statement(query)
        self.assertEqual(elements, row[0][0])



    
