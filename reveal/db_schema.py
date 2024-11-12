sql_statements = [
"""
create table if not exists pulse(
    transaction_id text primary key,
    procedure_id text,
    trans_group_id text,
    trans_group_ar text,
    trans_group_en text,
    procedure_name_ar text,
    procedure_name_en text,
    instance_date text,
    property_type_id text,
    property_type_ar text,
    property_type_en text,
    property_sub_type_id text,
    property_sub_type_ar text,
    property_sub_type_en text,
    property_usage_ar text,
    property_usage_en text,
    reg_type_id text,
    reg_type_ar text,
    reg_type_en text,
    area_id text,
    area_name_ar text,
    area_name_en text,
    building_name_ar text,
    building_name_en text,
    project_number text,
    project_name_ar text,
    project_name_en text,
    master_project_en text,
    master_project_ar text,
    nearest_landmark_ar text,
    nearest_landmark_en text,
    nearest_metro_ar text,
    nearest_metro_en text,
    nearest_mall_ar text,
    nearest_mall_en text,
    rooms_ar text,
    rooms_en text,
    has_parking text,
    procedure_area text,
    actual_worth text,
    meter_sale_price text,
    rent_value text,
    meter_rent_price text,
    no_of_parties_role_1 text,
    no_of_parties_role_2 text,
    no_of_parties_role_3 text
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
