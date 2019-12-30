import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This method will loop through sql for loading data from s3 into staging tables.

        Parameters :
            cur :  Cursor to execute create table queries
            conn:  Connection to commit once queries are executed

        Return :
            None

    """
    print("Loading into staging tables")
    for query in copy_table_queries:
        print("Query : ", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        This method should be called once data is available in staging tables.  ETL scripts will be 
        run and populate the star schema.

        Parameters :
            cur :  Cursor to execute create table queries
            conn:  Connection to commit once queries are executed

        Return :
            None

    """
    print("Loading into schema tables")
    for query in insert_table_queries:
        print("Query : ", query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Program to run ETL Job.  Initilizes and connects to AWS Redshift 
    and calls methods to load data into staging table and ETL to STAR Schema.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
