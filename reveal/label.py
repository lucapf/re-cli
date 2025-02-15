from reveal import (database_util )
from psycopg import Connection


def add_label_to_property(key: str,  property_id: str, c:Connection = database_util.get_connection() ):
    query = """
    select count(id) as count from property_labels
    where propertyfinder_id = %s and key = %s"""
    count = database_util.fetchone(query, tuple([str(property_id), key]), c)[0]
    if count == 0:
        insert ="""insert into property_labels(  key, propertyfinder_id) values( %s, %s)"""
        database_util.execute_insert_statement(insert,tuple([ key, str(property_id)]), c, True )

    
