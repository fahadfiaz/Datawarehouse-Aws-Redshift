import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function injest the data stored in S3 bucket into the Amazon Redshift staging tables
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function load and transform the data from staging tables into the dimensional tables
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     This function build an ETL pipeline that extracts the data from S3 bucket, 
     stages them in Amazon Redshift, and transforms data into a set of dimensional tables 
     for the analytics team in order to find hidden insights from the data
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()