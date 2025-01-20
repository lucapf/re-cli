from reveal import ( database_util, logging, util)
from typing import List,Tuple
from difflib import SequenceMatcher


def __fetch_pulse_buildings(master_project: str, conn)->List[str]:
    sql="""
        select building_name 
    from pulse_tower_mapping 
        where master_project=%s 
            and building_name is not null 
            and  building_name != ''
        """
    return __fetch(sql, master_project, conn)

def __fetch_propertyfinder_buildings(community: str, conn)->List[str]:
    sql="""
        select tower 
        from propertyfinder_tower_mapping 
        where community=%s 
            and  community is not null 
            and community !=''
        """
    return __fetch(sql, community, conn)

def __fetch(sql:str, value:str, conn) -> List[str]:
    value_tuple = value,
    building_name = database_util.fetch(sql, value_tuple, None, conn);
    if building_name is None or len(building_name) ==  0:
        return None
    return list(filter(lambda y: y is not None, 
                map(lambda x: str(x[0]) if len(x)>0 and x[0] !=''  else None,building_name )))

def _score(sample_i: str, candidates: List[str]|None) -> List[Tuple[float, str]] | None:
    if candidates is None or sample_i == 'None' or sample_i == '': 
        return None
    sample = sample_i.lower()
    matched_towers = list()
    for f_i in candidates:
        f = f_i.lower()
        if sample == f:
            matched_towers.append((1, f_i))
        if sample in f or f in sample:
            matched_towers.append((0.9, f_i))
    if len(matched_towers) == 0:
        return None
    return matched_towers
        # common_chars = 0
        # sequence_matches = SequenceMatcher(None, sample, f).get_matching_blocks()
        # for b in sequence_matches:
        #     common_chars +=b.size
        #

def remove_link(community: str):
    community_tuple = community,
    database_util.execute_insert_statement (
        'delete from propertyfinder_pulse_mapping where propertyfinder_community=%s',
        community_tuple, None, True)
    

def match(community: str) :
    '''
        match areas and store into propertyfinder_pulse_mapping.
        No interactive mode
    '''
    conn = database_util.get_connection()
    sql_insert="""
            insert into propertyfinder_pulse_mapping 
                (propertyfinder_community, propertyfinder_tower, pulse_master_project, pulse_building_name) 
            values(%s,%s, %s, %s) on conflict do nothing
        """
    sql = '''
            select name, pf_community, pulse_master_project 
            from propertyfinder_pulse_area_mapping where pf_community=%s
          '''
    community_tuple  = community,
    for areas in database_util.fetch(sql, community_tuple, None, conn):
        propertyfinder_buildings = __fetch_propertyfinder_buildings(areas[1], conn)
        pulse_buildings = __fetch_pulse_buildings(areas[2], conn)

        for pf_building in propertyfinder_buildings:
            candidates = _score(pf_building, pulse_buildings)
            if candidates is None:
                continue
            for matched in candidates:
                values = (areas[1],pf_building, areas[2],matched[1] )
                logging.debug(f"store: {pf_building} - {matched}  - values {values}")

                database_util.execute_insert_statement(sql_insert,values, conn )

        # logging.debug(f"areas: {areas}")
        # logging.debug(f"propertyfinder: {propertyfinder_buildings}")
        # logging.debug(f"pulse: {pulse_buildings}")
    conn.close()
