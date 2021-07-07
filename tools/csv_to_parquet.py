from os import listdir
from os.path import isfile, join
import pandas as pd
import regex as re
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO 


def tickers_to_list(string):
    regex = re.compile(r'\'([A-Z]+)\'')
    if string != '':
        tickers = [x.group(1) for x in regex.finditer(string)]
        return tickers
    else:
        return []


#used to convert our csvs to parquet, from that point, we already save the data in parquet
path = '.\\data\\daily_reddit_data'
path_parquet = path + "\\parquet_data\\"

def s3_to_parquet():
    #connect to cloud bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('reddit-temp')
    prefix_objs = bucket.objects.filter(Prefix="reddit-temp/")
    for obj in prefix_objs:
        try: #one file was corrupted
            key = obj.key
            print(key)
            body = obj.get()['Body'].read()
            df = pd.read_csv(BytesIO (body), encoding='utf8',sep=',')

            df[df['text_type'].isin(['post','comment'])] #only keep rows with correct formatting
            if(len(df.columns)>7):
                print("wait")
            df.drop(df.iloc[:, 7:], inplace = True, axis = 1) #drop columns created due to wrong formatting #TBD with parquet
            df = df.fillna('') #change NaNs to empty string
            df['tickers'] = df['tickers'].apply(tickers_to_list)
            df['post_id'] = df['post_id'].astype(str)
            df['comment_id'] = df['comment_id'].astype(str)
            df['subreddit'] = df['subreddit'].astype(str)
            df['text_type'] = df['text_type'].astype(str)
            filename = key.replace(".csv", '').replace("reddit-temp/", '')
            path_tsv = path_parquet + filename + ".parquet"
            print(df.head())
            table_pa = pa.Table.from_pandas(df)
            pq.write_table(table_pa, path_tsv)
        except Exception as e:
            print(e)


def local_to_parquet():
    files = [f for f in listdir(path) if isfile(join(path, f))]
    for file in files:
        df = pd.read_csv(path+'\\'+ file)
        df[df['text_type'].isin(['post','comment'])] #only keep rows with correct formatting
        if(len(df.columns)>7):
            print("wait")
        df.drop(df.iloc[:, 7:], inplace = True, axis = 1) #drop columns created due to wrong formatting #TBD with parquet
        df = df.fillna('') #change NaNs to empty string
        df['tickers'] = df['tickers'].apply(tickers_to_list)
        df['post_id'] = df['post_id'].astype(str)
        df['comment_id'] = df['comment_id'].astype(str)
        df['subreddit'] = df['subreddit'].astype(str)
        df['text_type'] = df['text_type'].astype(str)
        filename = file.replace(".csv", '')
        path_tsv = path_parquet + filename + ".parquet"
        print(df.head())
        table_pa = pa.Table.from_pandas(df)
        pq.write_table(table_pa, path_tsv)

s3_to_parquet()