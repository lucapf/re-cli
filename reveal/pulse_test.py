import unittest
from reveal import database_util, pulse

# transactions_test_data = "test_data/Transactions.csv"
transactions_test_data = "Transactions.csv"
data = [ {"transaction_id": 0 , "procedure_id":  "abc"}, 
         {"transaction_id": 1 , "procedure_id":  "abc"}, 
         {"transaction_id": 2 , "procedure_id":  "abc"}
       ]

class TestPulse(unittest.TestCase):

    def test_load(self):
       # pulse.clean()
       pulse_transactions = pulse.load(transactions_test_data) 
       self.assertIsNotNone(pulse_transactions)
       if pulse_transactions is not None: # only to remove the automatic test. If None the test already stopped
           self.assertEqual(pulse_transactions, 999)

    def test_get_ids(self):
        ids = pulse._map_transaction(data)
        self.assertIsNotNone(ids)
        self.assertEqual(len(ids),3)
        self.assertEqual(list(ids)[2], 2)

    def test_insert_transations(self):
        database_util.execute_insert_statement("delete from pulse")
        new_items_count = pulse.insert(data)
        self.assertEqual(new_items_count, len(data))
        new_items_count = pulse.insert(data)
        self.assertEqual(new_items_count,0)

