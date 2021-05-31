import json
from datetime import date
import boto3
from Extractor import Reddit_Extractor, Ticker_Extractor
from Stock_Dictionary import get_dict

def dailyScraper(event, context):

    s3 = boto3.client('s3')
    bucket =  'reddit-temp'
    key = 'creds/credentials.json'

    file_content = s3.get_object(Bucket=bucket, Key=key)
    s3_meta = file_content['Body'].read()
    creds = json.loads(s3_meta)


    extr = Reddit_Extractor(creds)
    stock_dictionary = get_dict()
    tickerExtr = Ticker_Extractor(stock_dictionary)
    subreddits = ["stocks","investing","StockMarket","wallstreetbets"]


    #load posts
    main_df = extr.joinData(subreddits = subreddits,limit = 100)
    main_df['tickers'] = main_df['text'].apply(tickerExtr.tickerCounter)

    #observe dataframe
    vals = main_df['subreddit'].value_counts()

    #store object in parquet format
    today = date.today()
    stringEpoch = today.strftime("%d_%m_%y")

    path_parq = "s3://reddit-temp/reddit-temp/" + stringEpoch + ".csv"

    main_df.to_csv(path_parq, index=False)


    #path_parq = "s3://reddit-temp/parq-data/" + stringEpoch + ".parquet.gzip"
    #main_df.to_parquet(path_parq, compression='gzip')

    #wr.s3.to_parquet(df=main_df, path=path_parq)


    response = {
        "statusCode": 200,
        "body": json.dumps({"Crawled messages":vals.to_string()})
    }

    return response