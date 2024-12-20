from reveal import (logging, db_schema)
from typing import List,  Dict
from reveal import pulse, database_util, util 
from reveal.config import Config
from dateutil import parser
import psycopg
from psycopg import Connection
from string import Template
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import logging



class PulseTransaction(object):

    def __init__(self, data: tuple, keys: List[str] = pulse.COLUMNS):
        self.transaction_id = data[0]
        self.procedure_id  = data[1]
        self.trans_group_id = data[2]
        self.trans_group = data[3]
        self.procedure_name = data[4]
        self.instance_date = data[5]
        self.property_type_id = data[6]
        self.property_type = data[7]
        self.property_sub_type_id = data[8]
        self.property_sub_type = data[9]
        self.property_usage = data[10]
        self.reg_type_id = data[11]
        self.reg_type = data[12]
        self.area_id = data[13] 
        self.area_name = data[14]
        self.building_name = data[15]
        self.project_number = data[16]
        self.project_name = data[17]
        self.master_project = data[18]
        self.rooms = data[19]
        self.has_parking = data[20]
        self.procedure_area = float(data[21])
        self.actual_worth  = float(data[22])
        self.meter_sale_price = data[23]
        self.rent_value = data[24]
        self.meter_rent_price = data[25]
        self.rooms = util.bedrooms_pulse_to_propertyfinder(self.rooms)
        self.price =self.actual_worth
        self.procedure_area_sqft = util.mq_to_sqft(self.procedure_area)
        self.size = float(self.procedure_area_sqft)
        self.price_sqft = self.actual_worth / self.procedure_area_sqft

    def to_dict(self) -> dict:
        return self.__dict__

    def get(self, key: str) -> str:
        return self.__dict__[key]

    def __str__(self)-> str:
        return  str(self.__dict__)


class Report(object):
    '''
     container all data per area report
    ''' 

    supported_sizes = ["studio", "1", "2", "3"]

    def __init__(self):
        self.community: str
        self.num_ads: int = 0
        self.avg_size: float = 0.0
        self.avg_price_sqft: float = 0.0
        self.by_bedrooms_report: Dict[str,PerTypeReport] = dict()



class PerTypeReport(object):
    def __init__(self):
        self.bedrooms: str
        self.num_ads: int = 0
        self.avg_price_sqft: float = 0.0
        self.avg_size: float = 0.0
        self.ads: List[PropertyReport] = list()

    def to_json(self):
        logging.info(f"to_dict: {self.__dict__.keys()}")
        d = self.__dict__.copy()
        ads = self.ads
        d.pop("ads")
        j = json.loads(json.dumps(d))
        ads = list() 
        for a in self.ads:
            ads.append(a.to_dict())
        # j.update({"ads":self.ads[0].to_dict()})
        j.update({"ads": ads})
        return j 

class PerPeriodStatistics(object):

    def __init__(self):
        self.inteval: int
        self.max_price: int
        self.avg_price: int
        self.min_price: int
        self.max_price_sqft: int
        self.avg_price_sqft: int
        self.min_price_sqft: int
        self.current_vs_avg_perc: int

    def to_dict(self):
        return self.__dict__.copy()


class PropertyReport(object):
    db_columns =  [
                   "id","community", "tower", "url",
                   "location_name","size", 
                   "price","price_per_sqft","listed_date",
                   "type", "bathrooms", 
                   "bedrooms","latitude", "longitude" 
                  ]

    def __init__(self):
        self.id: str = ''
        self.community: str = ''
        self.tower: str = ''
        self.url: str = ''
        self.score: float = 0.0
        self.location: str = ''
        self.size: int = 0
        self.price_sqft: float = 0.0
        self.price: float = 0.0
        self.listed_date: date = datetime.now()
        self.type: str = ''
        self.bathrooms: int = 0
        self.bedrooms: str = ''
        self.latitude: float = 0.0
        self.longitude: float = 0.0
        self.tower_sales: List[PulseTransaction] = list()
        self.spikes: List[PulseTransaction] = list()
        self.per_period_statistics = {"30": PerPeriodStatistics(), 
                                      "60": PerPeriodStatistics(),
                                      "90": PerPeriodStatistics(),
                                      "180": PerPeriodStatistics(),
                                      "180+": PerPeriodStatistics()
                                     }
        self.score: float = 0.0

    def to_dict(self):
        d = self.__dict__.copy()
        d.pop("tower_sales")
        d["tower_sales"] = list()
        if self.tower_sales is not None:
            for t in self.tower_sales:
                d["tower_sales"].append(t.to_dict())
        d.pop("spikes")
        d["spikes"] = list()
        if self.spikes is not None:
            for s in self.spikes:
                d["spikes "].append(s.to_dict())
        d.pop("per_period_statistics")
        if self.per_period_statistics is not None:
            per_period_statistics = dict()
            for k in self.per_period_statistics.keys():
                if self.per_period_statistics[k] is not None:
                    per_period_statistics[k] = self.per_period_statistics[k].to_dict()
            d["per_period_statistics"] = per_period_statistics
        return d

    def tostring():
        return self.__dict__

class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj,datetime) or isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, PropertyReport):
            return obj.to_dict()
        elif isinstance(obj, PulseTransaction):
            return obj.to_dict()
        elif isintance(obj,PerPeriodStatistics):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


def get_ads(community: str, config: Config) -> (List[PropertyReport], Connection):
    max_price = config.report_max_ads_price()
    supported_sizes = ','.join(
            list(map(lambda x: f"'{x}'",Report.supported_sizes)))
    database_columns = ",".join(PropertyReport.db_columns)
    sql = Template("""
        select $columns 
        from propertyfinder
        where  
                community = %(community)s
            and user_discarded = false 
            and tower is not null
            and bedrooms in ( $supported_sizes )
            and price < %(max_price)s
            and tower not in (
                    select tower from discarded_towers 
                    where community = %(community)s 
                      and discarded_to >= now()
                              ) order by tower
                 """).substitute({ 
                          "columns": database_columns,
                          "supported_sizes": supported_sizes
                                  })
    logging.debug(f" query {sql} - params community: {community} supported_sizes: {supported_sizes}")
    conn = database_util.get_connection()
    rows = database_util.fetch_map(sql, {
                            'community': community,
                            'max_price': max_price,
                                        }, conn)
    logging.debug(f"sql {sql} community: {community} max_price: {max_price} found: {len(rows)} elements")
    propertyReportList: List[PropertyReport] = list()
    for r in rows:
        p  = PropertyReport()
        for i in range(len(PropertyReport.db_columns)):
            column_name = PropertyReport.db_columns[i] \
                               if PropertyReport.db_columns[i]!= "price_per_sqft" \
                               else "price_sqft"
            p.__dict__[column_name] = r[i]
        propertyReportList.append(p)
    return propertyReportList, conn





def transaction_by_tower(community: str, tower :str, conn, conf: Config) ->List[PulseTransaction] | None:
    max_sales_days = conf.report_max_sales_days()
    from_date = datetime.now() - timedelta(max_sales_days) 
    
    '''
    decode propertyfinder building address to pulse and retrieve transaction
    '''
    sql=Template("""
        select 
            $columns
        from 
            pulse p join propertyfinder_pulse_mapping pfpm on  
                (
                    pfpm.pulse_master_project = p.master_project and
                    pfpm.pulse_building_name = p.building_name
                )
        where 
                pfpm.propertyfinder_community = %s  
            and pfpm.propertyfinder_tower = %s 
            and procedure_name = 'Sell'
            and p.trans_group='Sales' 
            and p.trans_group='Sales' 
            and p.instance_date > %s
            and p.property_type='Unit'
            and p.reg_type = 'Existing Properties'
                 """
        ).substitute({"columns": ','.join(list(map(lambda c: f"p.{c}", pulse.COLUMNS)))})
    data_tuple = (community, tower, from_date)
    transaction_row = database_util.fetch(sql,data_tuple,None, conn)
    transaction: List[PulseTransaction] = list()
    logging.debug(f"sql: {sql}")
    for row in transaction_row:
        transaction.append(PulseTransaction(row, pulse.COLUMNS))
    logging.debug(f" community {community} - tower: {tower} - fetched: {len(transaction)} transaction") 
    # for t in transaction:
    #     logging.debug(t)
    #
    return transaction 

def _save_per_period_ad(a: PropertyReport, conn):
    if a.tower_sales is None:
        return
    for p in a.tower_sales:
        database_util.execute_insert_statement("""
           insert into report_propertyfinder (
                                              propertyfinder_id, 
                                              pulse_transaction_id, 
                                              score, 
                                              community
                                              )
                                      values (%s,%s, %s, %s) 
                                               """, 
           (a.id, p.transaction_id,a.score, a.community ), conn)

     
def _save_per_period_statistics(a: PropertyReport,p: PerPeriodStatistics, conn):
    if p is None: 

        return
    values = ( a.id, 
               p.interval,
               p.max_price,
               p.avg_price,
               p.min_price,
               p.max_price_sqft,
               p.avg_price_sqft,
               p.min_price_sqft,
               p.current_vs_avg_perc,
               a.community,
               a.price_sqft
             )
    database_util.execute_insert_statement("""
        insert into report_per_period_statistics(
            propertyfinder_id, 
            interval, 
            max_price,
            avg_price,
            min_price,  
            max_price_sqft, 
            avg_price_sqft, 
            min_price_sqft,
            current_vs_avg_perc,
            community,
            ad_price_sqft
            )
        values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) 
                                          """, values, conn) 


def save_report(p: Report, conn:Connection):
    database_util.execute_insert_statement("""
        insert into report (community, num_ads, avg_size, avg_price_sqft)
                    values (%s, %s, %s,%s) 
                                           """, 
        (p.community, p.num_ads, p.avg_size, p.avg_price_sqft),conn)
    for v in p.by_bedrooms_report.values(): 
        for a in v.ads:
            _save_per_period_ad(a, conn)
            for p in a.per_period_statistics.values():
                _save_per_period_statistics(a, p, conn)
               
def clean_report():
    conn = database_util.get_connection()
    database_util.execute_insert_statement("delete from report_per_period_statistics", None, conn)
    database_util.execute_insert_statement("delete from report_propertyfinder", None, conn)
    database_util.execute_insert_statement("delete from report")



