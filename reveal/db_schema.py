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
create table if not exists propertyfinder (
        id text primary key,
        type varchar(15),
        price integer,
        size integer,
        bedrooms text,
        bathrooms varchar(2),
        price_per_sqft real,
        city varchar(15),
        community varchar(100),
        subcommunity varchar(100),
        tower text,
        location_slug varchar(200),
        location_name varchar(100),
        listed_date date,
        url varchar(250),
        price_history jsonb,
        file_path varchar(250),
        score numeric(6,3),
        user_discarded integer,
        description  text,
        latitude numeric(18,16), 
        longitude numeric(18,16) 
    )
""",
"""
ALTER TABLE propertyfinder ADD COLUMN IF NOT EXISTS completion_status varchar (18);
""",
"""
CREATE TABLE IF NOT EXISTS dashboard.propertyfinder_pulse_area_mapping (
	"name" varchar NOT NULL,
	pf_community varchar(100) NOT NULL,
	pulse_master_project varchar(50) NOT NULL,
	CONSTRAINT propertyfinder_pulse_area_mapping_pk PRIMARY KEY (pf_community),
	CONSTRAINT propertyfinder_pulse_area_mapping_unique UNIQUE ("name")
);

""",
"""
CREATE TABLE IF NOT EXISTS dashboard.propertyfinder_tower_mapping (
	community varchar(100) NOT NULL,
	tower varchar(150) NULL,
	CONSTRAINT propertyfinder_pulse_tower_mapping_pk PRIMARY KEY (community)
)
""",
"""
CREATE TABLE IF NOT EXISTS dashboard.pulse_tower_mapping (
	pulse_master_project varchar(50) NOT NULL,
	building_name varchar(100) NULL,
	CONSTRAINT pulse_tower_mapping_pk PRIMARY KEY (pulse_master_project)
)
""",
"""
insert into configuration (label,key, int_value, description)values('matcher threshold score','matcher.threshold.score', 90,'threshold for matching tower names' )
"""
,
"""
insert into propertyfinder_pulse_area_mapping (name, pf_community, pulse_master_project)
        values ('Jumeirah Lake Towers','Jumeirah Lake Towers','Jumeirah Lakes Towers') on conflict do nothing;
insert into propertyfinder_pulse_area_mapping (name, pf_community, pulse_master_project)
        values ('Dubai Marina','Dubai Marina','Dubai Marina') on conflict do nothing;
""",
"""
CREATE TABLE IF NOT EXISTS dashboard.property_labels (
	id int8 DEFAULT nextval('reveal_sequence') NOT NULL,
	propertyfinder_id varchar(8) NOT NULL,
	"key" varchar(40) NOT NULL,
	CONSTRAINT labels_pk PRIMARY KEY (id),
	CONSTRAINT labels_propertyfinder_fk FOREIGN KEY (propertyfinder_id) REFERENCES dashboard.propertyfinder(id) ON DELETE CASCADE
)
""",
"""
ALTER TABLE propertyfinder ADD COLUMN IF NOT EXISTS description text;
""", 
"""
insert into configuration (label,key, str_value, description)values('Penalized keywords','penalized.words', 'Upgraded, Renovated, Immaculate, Premium interiors, Brand new','if in the description, score will be lowered of pernalized score' ) on conflict do nothing;
insert into configuration (label,key, int_value, description)values('Penalized keywords score','penalized.words.score', -5,'Penalized words score' ) on conflict do nothing;
insert into configuration (label,key, str_value, description)values('Boosted keywords','boosted.words','Motivated Seller, Investor deal, Best layout, large layout, big layout ,Huge layout, Marina view,  sea view, lake view, distressed', 'if in the, score will be increased of boosted score'  ) on conflict do nothing;
insert into configuration (label,key, int_value, description)values('Boosted keywords score','boosted.words.score', -5,'Boosted keywords score' ) on conflict do nothing;
insert into configuration (label,key, int_value, description)values('1 Br threshold size','threshold.size.1br', 1000,'if 1 bedroom > this value, property is a good candidate for upgrade (2 br)' ) on conflict do nothing;
insert into configuration (label,key, int_value, description)values('Treshold score','threshold.size.score', 10,'boost the score if might be upgraded' ) on conflict do nothing;
""",
"""
insert into propertyfinder_pulse_area_mapping (name, pf_community, pulse_master_project,image) values ('JBR','Jumeirah Beach Residence','Jumeirah Beach Residence','/manager/images/jbr.jpg') on conflict do nothing;
"""



]
