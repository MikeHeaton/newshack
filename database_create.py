import sqlite3
import os
import PARAMETERS

if not os.path.isfile('./data/articles_database.db'):
    print("Creating database... ", end=" ")
    conn = sqlite3.connect("./data/articles_database.db")
    print("created.")
else:
    print("Database already exists, connecting...", end=" ")
    conn = sqlite3.connect("./data/articles_database.db")
    print("connected.")

c = conn.cursor()

tables_list =list(c.execute('SELECT name FROM sqlite_master WHERE type = "table"'))
print("Table list: {}".format(tables_list))

if ('raw_data',) not in tables_list:
    print("Creating table 'raw_data'...", end=" ")
    c.execute("CREATE TABLE raw_data (" +
                ' text, '.join(PARAMETERS.RAW_DATA_FIELDS) +
                ')')
    print("created.")
else:
    print("'raw_data' table already exists.")

if ('articles',) not in tables_list:
    print("Creating table 'articles'...", end=" ")
    c.execute("CREATE TABLE articles (" +
                ' text, '.join(PARAMETERS.ARTICLES_FIELDS) +
                ')')
    print("created.")
else:
    print("'articles' table already exists.")

print("Committing changes...", end=" ")
conn.commit()
print("committed.")

print("Closing database...", end=" ")
conn.close()
print("closed.")
