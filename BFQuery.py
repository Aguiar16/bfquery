import os
import pandas as pd
from google.cloud import bigquery
import glob
from IPython.display import display
# os.chdir("/mydir")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
client = bigquery.Client()
off_Num = 0

tag_Name = open("Tags", "r").read().split(', ')

def query_stackoverflow(tag_Name, off_Num):
	client = bigquery.Client()
	for i in range(len(tag_Name)): #for taking the total number or requisition
		print ('searching for the total number of requisitions in', tag_Name[i])
		query_job = client.query(f"""
			SELECT count(Id) 
			FROM `sotorrent-org.2019_03_17.Posts`
			WHERE tags LIKE {tag_Name[i]}""")
		print ('done')
		num_ROW = query_job.result().to_dataframe()
		display (num_ROW) # tratamento de erro, n estou conseguindo o valor correto de requisições
		print ('getting the ',tag_Name[i],' discussions')
		while off_Num <= num_ROW[0]: #condicional errado, resolvo depois

			query_job = client.query(f"""
				SELECT Id, Score, ViewCount, Body, Tags, AnswerCount, CommentCount, FavoriteCount, PostTypeId
				FROM `sotorrent-org.2019_03_17.Posts`
				WHERE tags LIKE {tag_Name[i]}
				ORDER BY Id DESC
				LIMIT 5000
				OFFSET {off_Num}
				""")
			off_Num += 5000
			result = job.result().to_dataframe() # temporario, ainda n resolvi essa parte
			result.to_csv(tag_Name[i])

if __name__ == '__main__':
	query_stackoverflow(tag_Name, off_Num)
