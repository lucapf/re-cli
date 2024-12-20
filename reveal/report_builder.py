from reveal import report_dao, logging, util
from reveal.config import Config
from datetime import datetime, timedelta, date
from reveal.report_dao import (
                                Report,                                     
                                PulseTransaction, 
                                PerTypeReport,
                                ComplexEncoder, 
                                PropertyReport, 
                                PerPeriodStatistics
                              )
from typing import List 
import json

def build_report():
    r = BuildReport().build_community_report('Dubai Marina')
    
def filter_by_delta_size( sales: List[PulseTransaction], ads_size: int, config: Config)  -> List[PulseTransaction]:
    delta = config.report_delta_perc()
    if  sales is None:
        return None
    delta_size = ads_size * (delta / 100)
    min_size = ads_size - delta_size
    max_size = ads_size + delta_size 
    return list(filter(lambda s: s.size > min_size and s.size < max_size, sales ))

def remove_spikes(sales: List[PulseTransaction], 
                  config: Config) -> ( List[PulseTransaction], 
                                       List[PulseTransaction]
                                                    ):
    if sales is None or len(sales) == 0:
        return (None, None)
    threshold = config.report_spike_threshold_perc() / 100
    avg_price = sum(map(lambda x: x.price, sales)) / len(sales)
    max_sale_price = avg_price * (1 + threshold)
    min_sale_price = avg_price * (1 - threshold)
    spikes = list(filter(lambda t: t.price > max_sale_price and t.price < min_sale_price,sales))
    sales  = list(set(sales) - set(spikes))
    return (sales, spikes)



class BuildReport(object):

    def __init__(self):
        self.community_transaction = dict()
        self.config = Config()
        self.sale_size_delta = self.config.report_delta_perc()

    def get_tower_transaction(self, community: str, tower: str, bedrooms: str, conn
                             ) -> list[PulseTransaction]:
        if not tower in self.community_transaction.keys():
             self.community_transaction[tower] = report_dao. \
                                 transaction_by_tower(community, tower, conn, self.config);
        sales = self.community_transaction[tower]
        return list(filter(lambda t: t.rooms == bedrooms, sales)) if not sales is None else None

    def per_period_statistics(self, a: PropertyReport) -> PropertyReport:
        for p in a.per_period_statistics.keys():
          a.per_period_statistics[p] = \
                  self.calculate_statistics(
                           p, a.tower_sales, a.price_sqft, a.price_sqft
                                            )



    def calculate_statistics(self, interval: str, tower_sales: List[PulseTransaction], 
                             ad_price_sqft: float, price_sqft: float )  -> PerPeriodStatistics:
        from_date = datetime.now() - timedelta(int(interval)) \
                        if interval.isdigit() else datetime.strptime('2000-01-01', '%Y-%m-%d')
        from_date = from_date.date()
        if util.is_empty_list(tower_sales): 
            return None
        logging.debug(f"from: {from_date}  - list {len(tower_sales)}")
        per_period_sales = list(filter(lambda s: s.instance_date > from_date, tower_sales))
        if util.is_empty_list(per_period_sales):
            return None
        p = PerPeriodStatistics()
        p.interval = interval
        price_list = list(map(lambda x: int(x.price), per_period_sales))
        logging.debug(f"price_list: {price_list}")
        p.max_price = max(price_list)
        p.avg_price = sum(price_list) / len(price_list)
        p.min_price = min(price_list)
        price_sqft_list = list(map(lambda x : x.price_sqft, per_period_sales))
        p.max_price_sqft = max(price_sqft_list) 
        p.avg_price_sqft = sum(price_sqft_list) / len(price_sqft_list)
        p.min_price_sqft = min(price_sqft_list)
        p.current_vs_avg_perc =  int((ad_price_sqft / p.avg_price_sqft  - 1) * 100 )
        logging.info(f"price_sqft: {ad_price_sqft} avg: {p.avg_price_sqft}")
        return p

    def calculate_score(self, a: PropertyReport):
        return max(map(lambda kv: 
                        kv[1].current_vs_avg_perc if kv[1] is not None else -100 
                        ,a.per_period_statistics.items()))



    def minify_report_data(self,r: Report) -> dict:
        minified_report = {'id': r.num_ads,'avg_size': r.avg_size, 
                           'avg_price_sqft':  r.avg_price_sqft,
                           } 
        per_bedrooms_report = dict()
        for k,v in r.by_bedrooms_report.items():
            l = list()
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
            if a.bedrooms in report_dao.Report.supported_sizes:
                sales = self.get_tower_transaction(community, a.tower, a.bedrooms, conn)
                sales = filter_by_delta_size(sales, a.size, self.config)
                a.tower_sales, a.spikes = remove_spikes(sales, self.config)
                self.per_period_statistics(a)
                if not a.bedrooms in bedrooms_ads_dic.keys():
                    bedrooms_ads_dic[a.bedrooms] = PerTypeReport()
                ads_report = bedrooms_ads_dic[a.bedrooms]
                ads_report.ads.append(a)
                a.score = self.calculate_score(a)

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
                
        


