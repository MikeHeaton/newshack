import sqlite3
import sys
import PARAMETERS
import newspaper

def connect_database(database_name):
    print('Database: connecting to\"{}\"...'.format(database_name), end=" ")
    conn = sqlite3.connect(database_name)

    c = conn.cursor()
    print("connected.")
    return conn, c

def with_database_connection(database_name=PARAMETERS.DATABASE_LOCATION,
                            commit_on_exit=True):
    def handle_database_connection(func):
        def connect_and_call(*args, **kwargs):
            conn, c = connect_database(database_name)
            try:
                things_to_return = func(conn, c, *args, **kwargs)
            finally:
                if commit_on_exit:
                    print("Database: committing changes... ", end=" ")
                    conn.commit()
                    print("committed.")

                print("Database: closing connection... ", end=" ")
                conn.close()
                print("closed.")
            return things_to_return
        return connect_and_call
    return handle_database_connection
