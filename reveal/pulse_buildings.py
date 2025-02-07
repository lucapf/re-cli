import csv
import os
from reveal import logging, database_util

__csv_file_name = "Buildings.csv"
numeric_columns = ["area_id", "zone_id", 
           "land_number","land_sub_number",
           "floors","rooms",
           "car_parks", "built_up_area", 
           "actual_area", "property_type_id",
           "parcel_id", "is_free_hold", "is_lease_hold", 
           "is_registered",
           "master_project_id", "land_type_id"
           ]
boolean_columns = ["is_free_hold", "is_lease_hold", "is_registered"]
data_columns = ["creation_date"]
insert_template = "insert into pulse_buildings ({columns}) values({values})"  

def load():
    if not os.path.exists(__csv_file_name):
        logging.err (f"csv file {__csv_file_name} not present")
    with open (__csv_file_name, "r") as csv_file:
        conn = database_util.get_connection()
        for r in csv.DictReader(csv_file):
            for c in r.keys():
                if r[c] =="" or r[c]=="null":
                    r[c] = None
                    continue
            for c in numeric_columns:
                if r[c] is not None:
                    if (r[c].isdigit()):
                        r[c] = int(r[c])
                    else:
                        r[c] = float(r[c])
                    continue
            for c in boolean_columns:
                r[c] = True if r[c] == 1 else False
            if r["creation_date"] is not None:
                value = r["creation_date"].split("-")
                r["creation_date"] = f"{value[1]}/{value[0]}/{value[2]}"

            sql_insert = insert_template.format(
                columns=','.join(r.keys()),
                values = ','.join(["%s"]*len(r.keys()))
                )
            database_util.execute_insert_statement(sql_insert,tuple(r.values()),conn)
            conn.commit()

