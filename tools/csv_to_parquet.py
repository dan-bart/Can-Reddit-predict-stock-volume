from os import listdir
from os.path import isfile, join
import pandas as pd
import regex as re
import pyarrow as pa
import pyarrow.parquet as pq


#used to convert our csvs to parquet, from that point, we already save the data in parquet
path = '.\\data\\daily_reddit_data'
path_parquet = path + "\\parquet_data\\"

files = [f for f in listdir(path) if isfile(join(path, f))]

def tickers_to_list(string):
    regex = re.compile(r'\'([A-Z]+)\'')
    if string != '':
        tickers = [x.group(1) for x in regex.finditer(string)]
        return tickers
    else:
        return []

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