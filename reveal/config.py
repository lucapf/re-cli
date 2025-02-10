from reveal import database_util, logging
from typing import Dict,Any

class Config(object):
    configuration: Dict[str, Any] = dict()
    initialized = False

    def __init__(self):
        self.__initialize_values()

  
    def __get_value(self,config_key):
        return self.configuration[config_key]

    def __initialize_values(self):
        db_configuration = database_util.fetch(
                "select key, int_value, str_value from configuration"
                                              )
        logging.debug(f"configuration: {db_configuration}")
        for c in db_configuration:
            Config.configuration[c[0]] = c[1] if c[1] is not None else c[2]
        initialized = True

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
