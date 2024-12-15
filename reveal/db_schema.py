sql_statements = [
"""
create table if not exists pulse(
    transaction_id varchar(25) primary key,
    procedure_id smallint ,
    trans_group_id smallint,
    trans_group varchar(12),
    procedure_name varchar(50),
    instance_date date,
    property_type_id smallint,
    property_type varchar(12),
    property_sub_type_id numeric,
    property_sub_type varchar(20),
    property_usage varchar(50),
    reg_type_id smallint,
    reg_type varchar(20),
    area_id smallint,
    area_name varchar(50),
    building_name varchar(100),
    project_number integer,
    project_name varchar(100),
    master_project varchar(50),
    rooms varchar(12),
    has_parking smallint,
    procedure_area numeric(14,2),
    actual_worth numeric(14,2),
    meter_sale_price numeric(12,2),
    rent_value numeric(12,2),
    meter_rent_price numeric(12,2)
)""",
"""
create table propertyfinder_areas(
    area text primary key
        )
""",
"""
create table if not exists propertyfinder (
        id text primary key,
        type text,
        price integer,
        size integer,
        bedrooms text,
        bathrooms integer,
        price_per_sqft real,
        city text,
        community text,
        subcommunity text,
        tower text,
        location_slug text,
        location_name text,
        listed_date text,
        url text,
        price_history text,
        file_path text,
        score real,
        user_discarded integer,
        description  text,
        latitude real, 
        longitude real

    )
"""

]
