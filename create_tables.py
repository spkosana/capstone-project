import configparser
import psycopg2
from rs_queries import create_table_queries, drop_table_queries


def redshift_setup(con,cur,qdict):
    """
    Input : Takes cursor(curr) , connection(con) , queries dictionary (qdict)
    
    """
    for drop,create in qdict.items():
        cur.execute(drop)
        con.commit()
        cur.execute(create)
        con.commit()
        


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    '''
    Getting all the parameter values from the config file and passing to create connection and cursor objects. 
    ''' 
    host = config.get("CLUSTER","HOST")
    dbname = config.get("CLUSTER","DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = config.get("CLUSTER", "DB_PASSWORD")
    port = config.get("CLUSTER", "DB_PORT")
    
    try:
        con = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password} port={port}")
        con.set_session(autocommit=True) 
        cur = con.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    
    '''
    Invoking drop_tables function with the above create cursor object and connection object and 
    invoking create_tables functions which execute create queries from the list to create required tables. 
    '''
	rs_table_setup = dict(zip(drop_table_queries,create_table_queries))
	redshift_setup(con=con, cur=cur, qdict=rs_table_setup)

    con.close()


if __name__ == "__main__":
    main()