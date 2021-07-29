import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This functions take data from S3 and copy them into staging tables in Redshift
    """
    
    q=1
    print('Staging data: 0/{} loaded'.format(len(copy_table_queries)))
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
       
        print('Staging data: {}/{} loaded'.format(q, len(copy_table_queries)))
        q=q+1
        
    print('Loading data into staging tables completed')


def insert_tables(cur, conn):
    """
    This function use data from the staging tables and transform them into a star schema
    """
    q=1
    print('New tables: 0/{} created'.format(len(insert_table_queries)))
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
        print('New tables: {}/{} created'.format(q, len(insert_table_queries)))
        q=q+1
    print('Transforming data and inserting into tables completed')


def main():
    """
    This ETL process takes data from 2 sources located in S3 (logs and songs).
    It then loads those data into staging tables in Redshift.
    Finally, from the staging tables, we load the data in our new star-schema
        that will be used for OLAP queries. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print('---> Running etl.py is completed\n')

    conn.close()


if __name__ == "__main__":
    main()