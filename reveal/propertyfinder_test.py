import json
from typing import Optional
import unittest

from reveal import logging, util
from reveal.get_data import propertyfinder



class test_propertyfinder(unittest.TestCase):
   
    def test_extract(self):
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract(test_file.read())
            with open("test_data/propertyfinder.json", "w") as json_file:
                json_file.write(json.dumps(property_data, indent=2))
            logging.info(f"data: {property_data}")
            self.assertIsNotNone(property_data)
            if property_data is not None: # only to avoid semantic error
                self.assertEqual(len(property_data["listings"]),30)

    def test_filter_out_non_properties(self) -> Optional[dict]:
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract(test_file.read())
        pp  = propertyfinder._extract_non_properties(property_data)
        self.assertIsNotNone(pp)
        self.assertEqual(len(pp["listings"]), 25)



            

