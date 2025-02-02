from reveal import report_dao, logging, util
from reveal.config import Config
from datetime import datetime, timedelta 
from reveal.report_dao import (
                                Report,                                     
                                PulseTransaction, 
                                PerTypeReport,
                                ComplexEncoder, 
                                PropertyReport, 
                                PerPeriodStatistics
                              )
from typing import Dict, List, Tuple 
import json

def build_report():
    BuildReport().build_community_report('Dubai Marina')
    
def filter_by_delta_size( sales: List[PulseTransaction], ads_size: int, config: Config)  -> List[PulseTransaction]:
    delta = config.report_delta_perc()
    if  sales is None:
        return None
    delta_size = ads_size * (delta / 100)
    min_size = ads_size - delta_size
    max_size = ads_size + delta_size 
    return list(filter(lambda s: s.size_sqft > min_size and s.size_sqft < max_size, sales ))

def remove_spikes(sales: List[PulseTransaction], 
                  config: Config) -> Tuple[ List[PulseTransaction]|None, List[PulseTransaction]|None]:
                                                    
    if sales is None or len(sales) == 0:
        return (None, None)
    threshold = config.report_spike_threshold_perc() / 100
    logging.debug(f"spikes threshold: ${threshold}")
    avg_price = sum(map(lambda x: x.price_sqft, sales)) / len(sales)
    max_sale_price = avg_price * (1 + threshold)
    min_sale_price = avg_price * (1 - threshold)
    spikes = list(filter(lambda t: t.price_sqft > max_sale_price or t.price_sqft < min_sale_price,sales))
    logging.debug(f"spikes found: ${len(spikes)} - max: ${max_sale_price} min: ${min_sale_price}")
    sales  = list(set(sales) - set(spikes))
    return (sales, spikes)



class BuildReport(object):

    def __init__(self):
        self.community_transaction = dict()
        self.config = Config()
        self.sale_size_delta = self.config.report_delta_perc()

    def get_tower_transaction(
            self, community: str, tower: str, bedrooms: str, conn
                             ) -> list[PulseTransaction]|None:
        if tower not in self.community_transaction.keys():
             self.community_transaction[tower] = report_dao. \
                                 transaction_by_tower(community, tower, conn, self.config)
        sales = self.community_transaction[tower]
        return list(filter(lambda t: t.bedrooms == bedrooms, sales)) if sales is not None else None

    def per_period_statistics(self, a: PropertyReport) -> PropertyReport|None:
        for p in a.per_period_statistics.keys():
          a.per_period_statistics[p] = \
                  self.calculate_statistics( p, a.tower_sales, a.price_sqft, a.price_sqft)



    def calculate_statistics(self, interval: str, tower_sales: List[PulseTransaction]| None, 
                             ad_price_sqft: float, price_sqft: float )  -> PerPeriodStatistics|None:
        from_date = datetime.now() - timedelta(int(interval)) \
                        if interval.isdigit() else datetime.strptime('2000-01-01', '%Y-%m-%d')
        from_date = from_date.date()
        if tower_sales is None: 
            return None
        if util.is_empty_list(tower_sales): 
            return None
        logging.debug(f"from: {from_date}  - list {len(tower_sales)}")
        per_period_sales = list(filter(lambda s: s.instance_date > from_date, tower_sales))
        if util.is_empty_list(per_period_sales):
            return None
        p = PerPeriodStatistics()
        p.interval = interval
        price_list = list(map(lambda x: int(x.actual_worth), per_period_sales))
        logging.debug(f"price_list: {price_list}")
        p.max_price = max(price_list)
        p.avg_price = int(sum(price_list) / len(price_list))
        p.min_price = min(price_list)
        price_sqft_list = list(map(lambda x : x.price_sqft, per_period_sales))
        p.max_price_sqft = int(max(price_sqft_list)) 
        p.avg_price_sqft = int(sum(price_sqft_list) / len(price_sqft_list))
        p.min_price_sqft = int(min(price_sqft_list))
        p.current_vs_avg_perc =  int((ad_price_sqft / p.avg_price_sqft  - 1) * 100 )
        p.sale_transaction = len(per_period_sales)
        logging.info(f"price_sqft: {ad_price_sqft} avg: {p.avg_price_sqft}")
        return p

    def calculate_score(self, a: PropertyReport):
        values = map(lambda kv: kv[1], a.per_period_statistics.items())
        non_null_values = list(filter(None, values))
        if len(non_null_values)>0:
                return sum(map(lambda v: v.current_vs_avg_perc ,non_null_values))/len(non_null_values)
        return 0


    def minify_report_data(self,r: Report) -> dict:
        minified_report = {'id': r.num_ads,'avg_size': r.avg_size, 
                           'avg_price_sqft':  r.avg_price_sqft,
                           } 
        per_bedrooms_report = dict()
        for k,v in r.by_bedrooms_report.items():
            for a in v.ads:
                i = {
                     'id': a.id, 'score': a.score, 
                     'price': a.price, 'price_sqft': a.price_sqft, 
                     'listed_date': a.listed_date
                     }
                for s in a.per_period_statistics.keys():
                    v = a.per_period_statistics[s]
                    if v is None:
                        continue
                    si = {"max_sqft": v.max_price_sqft, 
                          "min_sqft": v.min_price_sqft, 
                          "avg_sqft": v.avg_price_sqft, 
                          "avg_price_sqft": v.avg_price_sqft,
                          "current_vs_avg_perc": v.current_vs_avg_perc}
                    i[s] = si
                if a.tower_sales is None:
                    continue
                i["sales"] = list(map(
                    lambda x: {"price": x.actual_worth, 
                               "price_sqft": x.price_sqft, 
                               "instance_date": x.instance_date},a.tower_sales))
                per_bedrooms_report.update({k: i}) 
            minified_report.update({'by_bedrooms_report': per_bedrooms_report})
        return minified_report

    def build_community_report(self, community: str) -> report_dao.Report:
        report = report_dao.Report()
        ads, conn = report_dao.get_ads(community, self.config)
        logging.debug(f"found {len(ads)} ads")
        bedrooms_ads_dic: Dict[str, PerTypeReport] = dict()
        report.community = community
        report.num_ads = len(ads)
        report.avg_price_sqft = sum(map(lambda a: a.price_sqft, ads)) / report.num_ads 
        report.avg_size = sum(map(lambda a: a.size, ads)) / report.num_ads
        for a in ads:
            sales = self.get_tower_transaction(community, a.tower, a.bedrooms, conn)
            if sales is None:
                continue
            logging.info(f"found: {len(sales)} compatibile transactions")
            sales = filter_by_delta_size(sales, a.size, self.config)
            a.tower_sales, a.spikes = remove_spikes(sales, self.config)
            self.per_period_statistics(a)
            if a.bedrooms not in bedrooms_ads_dic.keys():
                bedrooms_ads_dic[a.bedrooms] = PerTypeReport()
            ads_report = bedrooms_ads_dic[a.bedrooms]
            ads_report.ads.append(a)
            a.score = self.calculate_score(a)
            logging.info(f"ad: {a.id} sales {0 if a.tower_sales is None else len(a.tower_sales)} score: {a.score}")
        for r in bedrooms_ads_dic.values():
            r.num_ads = len(r.ads)
            r.avg_size = int(sum(map(lambda a: a.size,r.ads)) / r.num_ads)
            r.avg_size_sqft = int(sum(map(lambda a: a.price_sqft, r.ads)) / r.num_ads)
        report.by_bedrooms_report = bedrooms_ads_dic
        report_dao.save_report(report, conn)
        with open("output.json", "w") as data_file:
            data_file.write(
                json.dumps(self.minify_report_data(report), cls=ComplexEncoder, indent=2)
                           )
        conn.close()
        return report
        
        
    def clean_report(self):
        report_dao.clean_report()
        
        


