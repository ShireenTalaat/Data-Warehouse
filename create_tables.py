import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop any existing table 
    
    Input :
    cur  : Execute PostgreSQL command in a database session
    conn : Connects to the database
    
    
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables dropped")


def create_tables(cur, conn):
    """
    CREATE both the songs and logs staging tables.
    
    
    input :
    cur  : Execute PostgreSQL command in a database session
    conn : Connects to the database
    
    Returns: 
    The created Dimensional & fact tables
    """
    
    
    print("Creating tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables created")
    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()




