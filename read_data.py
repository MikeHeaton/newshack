import sqlite3
import sys
import datetime
import newspaper
import re
from gensim.models import doc2vec
import pandas as pd

def download_save_article(article, database_connection):
    """Fields in database:
          (url text,
          title text,
          text text,
          keywords text,
          tags text,
          publish_date text
          fetch_date text
          tld text)'''"""
    try:
        article.download()
        article.parse()

        top_level_domain = re.search(r".(?P<tld>\w+).co", article.url)
        top_level_domain = (top_level_domain.group("tld")
                                if top_level_domain is not None else
                            "")

        if len(article.text) > 0:
            database_connection.execute("INSERT INTO articles (url, title, text, keywords, tags," \
                    " publish_date, fetch_date, tld) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (article.url,
                            article.title.replace("'", ""),
                            article.text.replace("'", ""),
                            str(article.keywords),
                            str(article.tags),
                            article.publish_date,
                            # Date & time
                            str(datetime.datetime.now())[:16].replace(" ", "_"),
                            top_level_domain
                            ))
            return True

    except:
        return False

def connect_database(database_name):
    print("Connecting to database {}".format(database_name))
    conn = sqlite3.connect(database_name)

    def database_on_delete(self):
        print("Closing connection... ", end=" ")
        self.close()
        print("done.")

    conn.__del__ = database_on_delete
    c = conn.cursor()
    print("Database connected.")
    return conn, c

def with_database_connection(func):
    def connect_and_call(*args, **kwargs, database_name="./data/articles_database.db"):
        conn, c = connect_database(database_name)
        try:
            things_to_return = func(conn, c, *args, **kwargs)
        finally:
            conn.close()
        return things_to_return
    return connect_and_call

def download_publication(   url,
                            database_name="./data/articles_database.db",
                            maxarticles=None):
    conn, c = connect_database(database_name)

    print("Reading source {}...".format(url))
    paper = newspaper.build(url, memoize_articles=False)
    print("Finished.")

    articles_to_download = paper.articles[:int(maxarticles) if maxarticles else None]

    success_counter = 0
    for n, article in enumerate(articles_to_download):
        success = download_save_article(article, conn)
        if not success:
            print("Article {} not available: {}".format(n, article.url))
        else:
            success_counter += 1

    print("Downloaded {} of {} articles successfully.".format(success_counter, n))

    conn.commit()
    print("Database committed.")
    #conn.close()
    print("Database closed.")

    return 0

def read_all_articles(database_name="./data/articles_database.db"):
    conn, c = connect_database(database_name)

    articles_dataframe = pd.DataFrame(list(conn.execute("SELECT * from articles")))

    conn.close()

    # Add 'words' column
    articles_dataframe['words'] = [re.findall(r"[\w']+|[.,/\"!?;\:]", row['text'])
                                                for row in articles_dataframe]

    # Add 'doc objects' column
    articles_dataframe['docobjects'] = [doc2vec.LabeledSentence(
                                                    row['words'], [row['url']])
                                                for row in articles_dataframe]

    print("Dataset loaded; {} unique words over {} documents.".format(
        len(set([w for words in articles_dataframe['words'] for w in words])),
        len(articles_dataframe)))

    return articles_dataframe



if __name__ == "__main__":
    maxarticles = sys.argv[2] if len(sys.argv) > 2 is not None else 0
    download_publication(   sys.argv[1],
                            database_name="./data/articles_database.db",
                            maxarticles= maxarticles)
