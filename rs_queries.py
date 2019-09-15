import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

incidents_table_drop = ("""DROP TABLE IF EXISTS incidents;""")
priority_table_drop = ("""DROP TABLE IF EXISTS priority;""")
time_table_drop = ("""DROP TABLE IF EXISTS time;""")
fact_incidents_table_drop = ("""DROP TABLE IF EXISTS fact_incidents;""")


incidents_table_create = ("""
                         
                         CREATE TABLE IF NOT EXISTS incidents(               
                                                                incident_type_id varchar ,
                                                                incident_description varchar                                                                
                                                                );
                
""")
priority_table_create = ("""CREATE TABLE IF NOT EXISTS priority( 
                                                                priority_key varchar ,
                                                                priority_level varchar
                                                                );
                                """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(create_time varchar NOT NULL PRIMARY KEY,
                                                           start_time timestamp ,
                                                           year int,
                                                           month int, 
                                                           hour int,
                                                           day int, 
                                                           weekday int, 
                                                           week int                                                           
                                                           );
""")

fact_incidents_table_create = ("""
                            CREATE TABLE IF NOT EXISTS fact_incidents(incidents_id BIGINT NOT NULL PRIMARY KEY,
                                                                      create_time varchar,
                                                                      start_time timestamp,
                                                                      end_time varchar,
                                                                      agency varchar,
                                                                      location varchar,
                                                                      area_id varchar, 
                                                                      beat varchar, 
                                                                      priority_key varchar, 
                                                                      incident_type_id varchar,
                                                                      eventnumber varchar,
                                                                      year int,
                                                                      month int
                                                                       );
                         """)
incidents_copy = ("""COPY incidents FROM {}
                           iam_role  '{}'
                          FORMAT AS PARQUET;""").format(config.get("files","INCIDENTS"), config.get("IAM_ROLE","ARN"))
priority_copy = ("""COPY priority FROM {}
                           iam_role  '{}'
                          FORMAT AS PARQUET;""").format(config.get("files","PRIORITY"), config.get("IAM_ROLE","ARN"))
time_copy = ("""COPY time FROM {}
                           iam_role  '{}'
                          FORMAT AS PARQUET;""").format(config.get("files","TIME"), config.get("IAM_ROLE","ARN"))
fact_incidents_copy = ("""COPY fact_incidents FROM {}
                           iam_role  '{}'
                          FORMAT AS PARQUET;""").format(config.get("files","FACT_INCIDENTS"), config.get("IAM_ROLE","ARN"))


quality_incidents = (""" Select count(*) tablecount from incidents;""")
quality_priority = (""" Select count(*) tablecount from priority;""")
quality_time = (""" Select count(*) tablecount from time;""")
quality_fact_incidents = (""" Select count(*) tablecount from fact_incidents;""")

drop_table_queries = [incidents_table_drop,priority_table_drop,time_table_drop,fact_incidents_table_drop]

create_table_queries = [incidents_table_create, priority_table_create,  time_table_create, fact_incidents_table_create]

copy_table_queries = [incidents_copy,priority_copy,time_copy,fact_incidents_copy]

quality_table_queries = [quality_incidents,quality_priority,quality_time,quality_fact_incidents]