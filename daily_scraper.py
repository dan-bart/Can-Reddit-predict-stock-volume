import os
import pandas as pd
import json
from datetime import date
from tools.Extractor import Reddit_Extractor, Ticker_Extractor
from tools.Stock_Dictionary import get_dict


#normally, we would have made the credentials relative, and leave them in .gitignore
#however Reddit requires a Dev enabled account, so we created one just for this purpose
#checks for ticker mentiones in a 100 posts and 500 comment in 4 subreddits
file_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(file_dir, 'data',"credentials.json")
with open(file_path) as json_file:
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

#store object
today = date.today()
stringEpoch = today.strftime("%d_%m_%y")
filename = stringEpoch+".csv"
file_path = os.path.join(file_dir, 'data\\daily_reddit_data',filename)
main_df.to_csv(file_path,index=False,encoding='utf-8-sig')