import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time



def load_staging_tables(cur, conn):
    """
    Load the staging_tables using SQL copy commands 
    
     Inputs:
     cur  : Execute PostgreSQL command in a database session
     conn : Connects to the database
    
     output: 
     staging_events and song_events tables are loaded with JSON data in the S3 Bucket 
    """
    for query in copy_table_queries:
        t=time()
        cur.execute(query)
        conn.commit()
        copyTime=time()-t
        print(" {} load time : {} sec\n".format(query,copyTime))
        
        
def insert_tables(cur, conn):
    """
    Insert data into the final tables using SQL insert commands 
    
      Inputs:
      cur  : Execute PostgreSQL command in a database session
      conn : Connects to the database
    
      output: 
      Dimensional and fact tables 
    """
    for query in insert_table_queries:
        t=time()
        cur.execute(query)
        conn.commit()
        copyTime=time()-t
        print(" {} load time : {} sec\n".format(query,copyTime))


def main():
    """
    - Run the configparser instance to read the configuration file 'dwh.cfg'
      - Establich the connection to the postgres database in S3 by providing host, database name and credentials in 'dwh.cfg'
      - Instanciate a cursor to Execute PostgreSQL command in a database session
      - Call load_staging_tables & insert_tables create above 
      - Close the connection.
    
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


