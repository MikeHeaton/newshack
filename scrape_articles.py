import sqlite3
import sys
import datetime
import newspaper
import re
import pandas as pd

import PARAMETERS
import database_connect

def download_save_singlearticle(article, database_connection):
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

    except:
        return False

    else:
        top_level_domain = re.search(r".(?P<tld>\w+).co", article.url)
        top_level_domain = (top_level_domain.group("tld")
                            if top_level_domain is not None else
                            "")

        if len(article.text) > 0:
            database_connection.execute("INSERT INTO articles (url, title, text, keywords, tags," \
                    " publish_date, fetch_date, tld) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (article.url if article.url is not None else "",
                            article.title.replace("'", "") if article.title is not None else "",
                            article.text.replace("'", "") if article.text is not None else "",
                            str(article.keywords) if article.keywords is not None else "",
                            str(article.tags) if article.tags is not None else "",
                            article.publish_date if article.publish_date is not None else "",

                            # Date & time
                            str(datetime.datetime.now())[:16].replace(" ", "_") ,
                            top_level_domain
                            ))
        return True


@database_connect.with_database_connection()
def download_publication(conn, c, url, maxarticles=None):
    print("Reading source {}...".format(url))
    paper = newspaper.build(url, memoize_articles=False)
    print("Finished.")
    articles_to_download = paper.articles[:int(maxarticles) if maxarticles
                                          else None]
    success_counter = 0

    print("Commencing download of {} articles.".format(len(articles_to_download)))
    for n, article in enumerate(articles_to_download):
        success = download_save_singlearticle(article, conn)
        if not success:
            print("Article {} not available: {}".format(n, article.url))
        else:
            success_counter += 1

    print("Downloaded {} of {} articles successfully.".format(success_counter, n))

    return 0


if __name__ == "__main__":
    maxarticles = sys.argv[2] if len(sys.argv) > 2 is not None else 0
    download_publication(   sys.argv[1],
                            #database_name="./data/articles_database.db",
                            maxarticles= maxarticles)
