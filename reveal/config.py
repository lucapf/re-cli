from reveal import database_util, logging
from typing import Dict,Any, List

class Config(object):

    def __init__(self):
        self.initialized:bool = False
        self.configuration: Dict[str, Any] = dict()

    def __get_value(self,config_key):
        if not self.initialized:
            self.__initialize_values()
            self.initialized= True
        return self.configuration[config_key]

    def __initialize_values(self):
        db_configuration = database_util.fetch(
                "select key, int_value, str_value from configuration"
                                              )
        logging.debug(f"configuration: {db_configuration}")
        for c in db_configuration:
            self.configuration[c[0]] = c[1] if c[1] is not None else c[2]

    def report_delta_perc(self)-> int:
        return int(self.__get_value('report.delta_perc'))

    def matcher_threshold_score(self) -> int:
        return int(self.__get_value('matcher.threshold.score'))

    def report_max_ads_price(self) -> int:
        return int(self.__get_value('report.max_price'))

    def report_spike_threshold_perc(self) -> int:
        return int(self.__get_value('report.spike_threshold_perc'))

    def report_max_sales_days(self) -> int:
        return int(self.__get_value('report.max_sales_days'))

    def boosted_keywords(self) -> List[str]:
        return self.__get_value('boosted.words').split(',')
    
    def boosted_keywords_score(self) -> int:
        return int(self.__get_value('boosted.words.score'))

    def penalized_keywords(self) -> List[str]:
        return self.__get_value('penalized.words').split(',')
    
    def penalized_keywords_score(self) -> int:
        return int(self.__get_value('penalized.words.score'))
    
    def threshold_size_1br(self) -> int:
        return int(self.__get_value('threshold.size.1br'))

    def threshold_size_score(self) -> int:
        return int(self.__get_value('threshold.size.score'))
