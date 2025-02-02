from reveal import (logging)
from typing import List,  Dict, Tuple
from reveal import pulse, database_util, util 
from reveal.config import Config
from psycopg import Connection
from string import Template
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import time



class PulseTransaction(object):

    def __init__(self, data: tuple ):

        self.instance_date: date 
        self.transaction_id: str
        self.rooms: str
        for i in range(len(pulse.COLUMNS)):
            column_name = pulse.COLUMNS[i]
            self.__dict__[column_name] =  data[i]
        self.berooms = util.bedrooms_pulse_to_propertyfinder(self.rooms)
        self.procedure_area = int(self.procedure_area)
        self.actual_worth = float(self.actual_worth)
        self.size_sqft = int(self.size_sqft)
        self.price_sqft = float(self.price_sqft)

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
        self.created_at: datetime 
        self.min_score: int = 0
        self.spike_threshold_perc: int = 0
        self.max_price: int = 0            
        self.sales_delta_perc: int = 0

class PerTypeReport(object):
    def __init__(self):
        self.bedrooms: str
        self.num_ads: int = 0
        self.avg_price_sqft: float = 0.0
        self.avg_size: float = 0.0
        self.avg_size_sqft: float = 0.0
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
        self.interval: str 
        self.max_price: int
        self.avg_price: int 
        self.min_price: int
        self.max_price_sqft: int
        self.avg_price_sqft: int
        self.min_price_sqft: int
        self.current_vs_avg_perc: int
        self.sale_transaction: int

    def to_dict(self):
        return self.__dict__.copy()


class PropertyReport(object):
    db_columns =  [
                   "id","community", "tower", "url",
                   "location_name","size", 
                   "price","price_sqft","listed_date",
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
        self.tower_sales: List[PulseTransaction]|None = list()
        self.spikes: List[PulseTransaction]|None = list()
        self.per_period_statistics: Dict[str, PerPeriodStatistics|None] = \
                                    { "30": PerPeriodStatistics(), 
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
                pps = self.per_period_statistics[k] 
                if pps is not None:
                    per_period_statistics[k] = pps.to_dict()
            d["per_period_statistics"] = per_period_statistics
        return d

    def tostring(self):
        return self.__dict__

class ComplexEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o,datetime) or isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, PropertyReport):
            return o.to_dict()
        elif isinstance(o, PulseTransaction):
            return o.to_dict()
        elif isinstance(o,PerPeriodStatistics):
            return o.to_dict()
        return json.JSONEncoder.default(self, o)


def get_ads(community: str, config: Config) -> Tuple[List[PropertyReport], Connection]:
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
                               if PropertyReport.db_columns[i]!= "price_sqft" \
                               else "price_sqft"
            p.__dict__[column_name] = r[i]
        propertyReportList.append(p)
    return propertyReportList, conn





def transaction_by_tower(community: str, tower :str, conn, conf: Config) ->List[PulseTransaction] | None:
    '''
    decode propertyfinder building address to pulse and retrieve transaction
    '''
    max_sales_days = conf.report_max_sales_days()
    from_date = datetime.now() - timedelta(max_sales_days) 
    
    sql=Template("""
        select 
            $columns
        from 
            pulse p join propertyfinder_pulse_mapping pfpm on  
                (
                    pfpm.pulse_master_project = p.master_project
                and pfpm.pulse_building_name = p.building_name
                )
        where 
                pfpm.propertyfinder_community = %s  
            and pfpm.propertyfinder_tower = %s 
            and procedure_name = 'Sell'
            and p.trans_group='Sales' 
            and p.instance_date > %s
            and p.property_type='Unit'
            and p.reg_type = 'Existing Properties'
                 """
        ).substitute({"columns": ','.join(list(map(lambda c: f"p.{c}", pulse.COLUMNS)))})
    data_tuple = (community, tower, from_date)
    transaction_row = database_util.fetch(sql,data_tuple,None, conn)
    transaction: List[PulseTransaction] = list()
    # logging.debug(f"sql: {sql}")
    for row in transaction_row:
        transaction.append(PulseTransaction(row))
    # logging.debug(f" community {community} - tower: {tower} - fetched: {len(transaction)} transaction") 
    return transaction 

def _save_ad_sale(a: PropertyReport, conn):
    if a.tower_sales is None:
        return
    for p in a.tower_sales:
        __save_ad_sale_data(a, p, False, conn)
    if a.spikes is not None:
        for p in a.spikes:
            __save_ad_sale_data(a, p, True, conn)

def __save_ad_sale_data(a:PropertyReport,p: PulseTransaction, is_spike: bool, conn):
        database_util.execute_insert_statement("""
           insert into report_propertyfinder (
                                              propertyfinder_id, 
                                              pulse_transaction_id, 
                                              score, 
                                              community,
                                              is_spike
                                              )
                                      values (%s,%s, %s, %s, %s) 
            on conflict (propertyfinder_id, pulse_transaction_id)  do update set score=%s, is_spike=%s
                                               """, 
           (a.id, p.transaction_id,a.score, a.community, is_spike, a.score, is_spike), conn)

     
def _save_per_period_statistics(a: PropertyReport,p: PerPeriodStatistics|None, conn):
    if p is None: 
        return

    values = {
       "id": a.id, 
       "interval": p.interval,
       "max_price": p.max_price,
       "avg_price": p.avg_price,
       "min_price": p.min_price,
       "max_price_sqft": p.max_price_sqft,
       "avg_price_sqft": p.avg_price_sqft,
       "min_price_sqft": p.min_price_sqft,
       "current_vs_avg_price": p.current_vs_avg_perc,
       "community": a.community,
       "ad_price_sqft": a.price_sqft,
       "sale_transaction": p.sale_transaction
     }
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
            ad_price_sqft, 
            sale_transaction
            )
        values ( %(id)s, 
                %(interval)s,
                %(max_price)s,
                %(avg_price)s, 
                %(min_price)s, 
                %(max_price_sqft)s, 
                %(avg_price_sqft)s, 
                %(min_price_sqft)s,
                %(current_vs_avg_price)s, 
                %(community)s, 
                %(ad_price_sqft)s,
                %(sale_transaction)s 
        ) on conflict (propertyfinder_id,"interval") 
        do update set 
            max_price =%(max_price)s, 
            avg_price =%(avg_price)s, 
            min_price =%(min_price)s,
            max_price_sqft=%(max_price_sqft)s, 
            avg_price_sqft= %(avg_price_sqft)s,
            min_price_sqft = %(min_price_sqft)s, 
            current_vs_avg_perc= %(current_vs_avg_price)s,
            ad_price_sqft=%(ad_price_sqft)s, 
            sale_transaction = %(sale_transaction)s
          """, values, conn) 


def save_report(p: Report, conn:Connection):
    configuration_entries = database_util.fetch("""
        select int_value, key from configuration 
        where key in (  'report.spike_threshold_perc', 
                        'report.relevant_properties_min_score',
                        'report.max_price', 'report.delta_perc'
                     ) """,None ,None, conn)

    values = {
                "community": p.community, 
                "num_ads": p.num_ads, 
                "avg_size": p.avg_size, 
                "created_at": date.fromtimestamp(time.time()),
                "avg_price_sqft": p.avg_price_sqft, 
                "min_score": p.min_score,
                "spike_threshold_perc": p.spike_threshold_perc, 
                "max_price": p.max_price,
                "sales_delta_perc": p.sales_delta_perc,
            }
    for c in configuration_entries:
        if c[1] == 'report.spike_threshold_perc':
            values['spike_threshold_perc'] = c[0]
        if c[1] == 'report.relevant_properties_min_score':
            values['min_score'] = c[0]
        if c[1] == 'report.max_price':
            values['max_price'] = c[0]
        if c[1] == 'report.delta_perc':
            values['sales_delta_perc']= c[0]

    database_util.execute_insert_statement("""
        insert into report (community, num_ads, avg_size, created_at , avg_price_sqft,min_score, spike_threshold_perc, max_price, sales_delta_perc)
                    values (%(community)s, %(num_ads)s, %(avg_size)s, %(created_at)s, %(avg_price_sqft)s, %(min_score)s, %(spike_threshold_perc)s, %(max_price)s, %(sales_delta_perc)s ) 
        on conflict (community) 
       do update set num_ads = %(num_ads)s, avg_size = %(avg_size)s, created_at = %(created_at)s, avg_price_sqft = %(avg_price_sqft)s, 
                     min_score=%(min_score)s, spike_threshold_perc = %(spike_threshold_perc)s, max_price=%(max_price)s, 
                     sales_delta_perc=%(sales_delta_perc)s""", values,conn)
    for v in p.by_bedrooms_report.values(): 
        for a in v.ads:
            _save_ad_sale(a, conn)
            if a.per_period_statistics is not None:
                for pps in a.per_period_statistics.values():
                    _save_per_period_statistics(a, pps, conn)
               
def clean_report():
    conn = database_util.get_connection()
    database_util.execute_insert_statement("delete from report_per_period_statistics", None, conn)
    database_util.execute_insert_statement("delete from report_propertyfinder", None, conn)
    database_util.execute_insert_statement("delete from report")
