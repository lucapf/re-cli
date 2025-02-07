from typing import Dict
from reveal import database_util

def link_stats(community:str)->Dict[str, float]:
   totalAds = database_util.fetchone("""select count(id) as totalAds from propertyfinder where community=%s and completion_status = 'completed'""", (community,) )[0]
   linked = database_util.fetchone("select count(id) as linkedAds from propertyfinder where community=%s and completion_status = 'completed' and tower in (select propertyfinder_tower from propertyfinder_pulse_mapping)", (community,) )[0]
   return {'totalAds': totalAds, 'linked': linked, 'coverage': linked/totalAds }

