import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This method will loop through list of drop table sql and execute them.

        Parameters :
            cur :  Cursor to execute drop table queries
            conn:  Connection to commit once queries are executed

        Return :
            None
    """
    print("Dropping tables")
    for query in drop_table_queries:
        print("Dropping tables :", query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This method will loop through list of create table sql and execute them.

        Parameters :
        cur :  Cursor to execute create table queries
        conn:  Connection to commit once queries are executed

        Return :
            None
    """
    print("Creating tables")
    for query in create_table_queries:
        print("Creating tables", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
