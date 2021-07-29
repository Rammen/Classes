import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops our tables is they exists
    Mendatory for this project if, by example, we want to restart from skratch
        because we made some changes in the tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('Old tables were dropped')


def create_tables(cur, conn):
    """
    This functions used queries to create tables based on our star-schema
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('New tables were created')


def main():
    """
    In this pipeline, we delete olds tables (if they exists) and then replace
    them with new empty tables based on our star-schema. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print('---> Running create_tables.py is completed\n')


if __name__ == "__main__":
    main()