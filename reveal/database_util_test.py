import unittest
import os
import random, string
from datetime import datetime
from reveal import logging
from reveal import database_util

insert_statement = "insert into propertyfinder_areas(area) values (?)"

class DatabaseUtilTest(unittest.TestCase):

    def test_1_create_database(self):
        conn = database_util.get_connection()
        self.assertIsNotNone(conn)
        database_util.init_database()
        

