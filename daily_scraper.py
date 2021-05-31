import pandas as pd
import json
from datetime import date
from tools.Extractor import Reddit_Extractor, Ticker_Extractor
from tools.Stock_Dictionary import get_dict
import pyarrow as pa
import pyarrow.parquet as pq


#normally, we would have made the credentials relative, and leave them in .gitignore
#however Reddit requires a Dev enabled account, so we created one just for this purpose
#checks for ticker mentiones in a 100 posts and 500 comment in 4 subreddits
with open(".\\data\\credentials.json") as json_file:
    creds = json.load(json_file)     

extr = Reddit_Extractor(creds)
stock_dictionary = get_dict()
tickerExtr = Ticker_Extractor(stock_dictionary)
subreddits = ["stocks","investing","StockMarket","wallstreetbets"]

#load posts
main_df = extr.joinData(subreddits = subreddits,limit = 100)
main_df['tickers'] = main_df['text'].apply(tickerExtr.tickerCounter)

#observe dataframe
main_df.tickers[main_df.tickers.notna()].tolist()
main_df['subreddit'].value_counts()

#store object in parquet format
today = date.today()
stringEpoch = today.strftime("%d_%m_%y")
path_tsv = '.\\data\\daily_parquet_data\\' + stringEpoch + ".parquet"
table_pa = pa.Table.from_pandas(main_df)
pq.write_table(table_pa, path_tsv)