import sqlite3
import sys
import re
import pandas as pd
import database_connect
import PARAMETERS
from gensim.models import doc2vec

def read_from_database():

    @database_connect.with_database_connection(commit_on_exit=False)
    def dataframe_from_database(conn, c):
        return pd.DataFrame(list(conn.execute("SELECT * from articles")),
                            columns=PARAMETERS.ARTICLES_FIELDS)

    articles_dataframe = dataframe_from_database()
    print("{} documents loaded from database.".format(len(articles_dataframe)))

    # Add 'words' column
    articles_dataframe['words'] = [re.findall(r"[\w']+|[.,/\"!?;\:]",
                                    articles_dataframe.loc[i]['text'].lower())
                                    for i in articles_dataframe.index]

    # Add 'doc objects' column
    articles_dataframe['docobjects'] = [doc2vec.LabeledSentence(
                                            articles_dataframe.loc[i]['words'],
                                            [articles_dataframe.loc[i]['url']])
                                        for i in articles_dataframe.index]

    print("Dataset loaded; {} unique words over {} documents.".format(
        len(set([w for words in articles_dataframe['words'] for w in words])),
        len(articles_dataframe)))

    return articles_dataframe


if __name__ == "__main__":
    all_data = read_from_database()
    print(all_data['category'].value_counts())
