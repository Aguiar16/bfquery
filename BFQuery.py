import os
import pandas as pd
from google.cloud import bigquery
import glob
from IPython.display import display

# os.chdir("/mydir")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "SOTorrent-Services-f260794bf9c1.json"
client = bigquery.Client()
off_Num = 0


tag_Name = open("Tags", "r").read().split(', \n')


def query_stackoverflow(tag_Name, off_Num):
    client = bigquery.Client()
    for i in range(len(tag_Name)):
        # for taking the total number or requisition
        print ('searching for the total number of requisitions in',
               tag_Name[i])
        query_job = client.query(f"""
            SELECT count(Id)
            FROM `sotorrent-org.2019_03_17.Posts`
            WHERE Tags LIKE '%<{tag_Name[i][1:-1]}>%'
        """)
        print ('done')
        num_ROW = query_job.result().to_dataframe()
        # display (num_ROW)
        print ('getting the ', tag_Name[i], ' discussions')
        j = 0
        while off_Num <= num_ROW.iloc[0][0]:

            query_job = client.query(f"""
                SELECT Id, Score, ViewCount, Body, Tags,
                    AnswerCount, CommentCount, FavoriteCount, PostTypeId
                FROM `sotorrent-org.2019_03_17.Posts`
                WHERE Tags LIKE '%<{tag_Name[i][1:-1]}>%'
                ORDER BY Id ASC
                LIMIT 5000
                OFFSET {off_Num} """)
            result = query_job.result().to_dataframe()
            result.set_index('Id', inplace=True)
            result.to_csv('./resultados/'+tag_Name[i][1:-1]+'_result.csv')

            off_Num += 5000
            j += 1
        print('done')


if __name__ == '__main__':
    query_stackoverflow(tag_Name, off_Num)
