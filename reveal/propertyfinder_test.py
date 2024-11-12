import json
from typing import Optional
import unittest

from reveal import database_util, logging, util, propertyfinder

class test_propertyfinder(unittest.TestCase):
   
    def test_extract(self):
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract_data(test_file.read())
            with open("test_data/propertyfinder.json", "w") as json_file:
                json_file.write(json.dumps(property_data, indent=2))
            # logging.info(f"data: {property_data}")
            self.assertIsNotNone(property_data)
            if property_data is not None: # only to avoid semantic error
                self.assertEqual(len(property_data),30)

    def test_filter_out_non_properties(self): 
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract_data(test_file.read())
        pp  = propertyfinder._filter_out_non_properties(property_data)
        self.assertIsNotNone(pp)
        if pp is not None: # always true
            self.assertEqual(len(pp), 25)

    def test_mapping_properties(self):
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract_data(test_file.read())
            self.assertIsNotNone(property_data)
            if property_data is not None: #always true
                pf_property = property_data[0]
                db_fields = propertyfinder._map_db_fields(pf_property)
                self.assertIsNotNone(db_fields)
                self.assertEqual(len(db_fields), 16)
                self.assertEqual(db_fields["id"],"12423130" )
                self.assertEqual(db_fields["type"],"Apartment")
                self.assertEqual(db_fields["price"],1978000)
                self.assertEqual(db_fields["size"],1152)
                self.assertEqual(db_fields["bedrooms"],2)
                self.assertEqual(db_fields["bathrooms"],3)
                self.assertEqual(db_fields["city"],"Dubai")
                self.assertEqual(db_fields["community"],"Damac Lagoons")
                self.assertEqual(db_fields["subcommunity"],"Lagoon Views")
                self.assertEqual(db_fields["location_slug"],"damac-lagoons-lagoon-views")
                self.assertEqual(db_fields["location_name"],"Lagoon Views, Damac Lagoons, Dubai")
                self.assertEqual(db_fields["listed_date"],"2024-09-19T18:12:26Z")
                self.assertEqual(db_fields["url"], "https://www.propertyfinder.ae/en/plp/buy/apartment-for-sale-dubai-damac-lagoons-lagoon-views-12423130.html")
                self.assertEqual(db_fields["latitude"], 25.005964279174805)
                self.assertEqual(db_fields["longitude"],55.22930145263672)
                del(pf_property["property"]["location"]["coordinates"]["lat"])
                del(pf_property["property"]["location"]["coordinates"]["lon"])
                db_fields = propertyfinder._map_db_fields(pf_property)
                self.assertIsNone(db_fields["latitude"])
                self.assertIsNone(db_fields["longitude"])
                del(pf_property["property"]["location"]["coordinates"])
                db_fields = propertyfinder._map_db_fields(pf_property)
                self.assertIsNone(db_fields.get("latitude"))

    def test_save(self):
        database_util.init_database()
        database_util.execute_insert_statement('delete from propertyfinder')
        with open("test_data/propertyfinder.html", "r") as test_file:
            property_data = propertyfinder._extract_data(test_file.read())
            property_data = propertyfinder._filter_out_non_properties(property_data)
            self.assertIsNotNone(property_data)
            if property_data is not None: #boilerplate
                self.assertEqual(len(property_data), 24)
                saved_elements = propertyfinder._save(property_data)
                self.assertEqual(saved_elements, 24)
                saved_elements = propertyfinder._save(property_data)
                self.assertEqual(saved_elements, 0)

