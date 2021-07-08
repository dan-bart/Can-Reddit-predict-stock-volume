from os import listdir
from os.path import isfile, join
import pandas as pd
from datetime import datetime

class Ticker_Matrix:
    def __init__(self,path):
        self.path = path
        self.full_df = self.create_df()
        self.ticker_matrix = self.create_ticker_matrix()

    def create_df(self):
        files = [f for f in listdir(self.path) if isfile(join(self.path, f))]
        li = []
        for file in files:
            df = pd.read_parquet(self.path+file, engine='pyarrow')
            df = df.drop(labels = 'text',axis = 1) # dropping biggest column to prevent memory overloading
            li.append(df)
        full_df = pd.concat(li)
        #print(full_df.dtypes)


        full_df = full_df.fillna('') #change NaNs to empty string

        #drop comments that are duplicates
        full_df_comments = full_df.loc[full_df['text_type'] == 'comment']
        full_df_comments = full_df_comments.drop_duplicates(subset = ['comment_id'])
        #drop posts that are duplicates
        full_df_posts = full_df.loc[full_df['text_type'] == 'post']
        full_df_posts = full_df_posts.drop_duplicates(subset = ['post_id'])
        #concat back together
        full_df = pd.concat([full_df_posts,full_df_comments])

        #create a "Date" column from utc
        full_df['date'] = full_df['epoch_time'].apply(lambda x: datetime.utcfromtimestamp(x))

        #prepare the final dataframe ticker_matrix
        full_df['date'] = pd.to_datetime(full_df['date']).dt.date
        #March 17th seems to be a reasonable lower bound for our dataset
        full_df = full_df.loc[full_df['date'] >= pd.to_datetime('2021-03-17').date()]

        return full_df


    def create_ticker_matrix(self):
        date_index = self.full_df['date'].unique()
        date_index.sort()
        ticker_matrix = pd.DataFrame(index = date_index)
        ticker_matrix.index = pd.to_datetime(ticker_matrix.index)

        #with values being the number of mentions for each day
        for i in range(len(self.full_df)):
            row = self.full_df.iloc[i]
            row_date = row['date']
            if row['tickers'] != []:
                for ticker in row['tickers']: 
                    #rowIndex = ticker_matrix.index[row['date']]
                    if(ticker not in ticker_matrix.columns): #column already crated
                        ticker_matrix[ticker] = pd.Series(dtype='int64')
        ticker_matrix = ticker_matrix.fillna(0)
        #now we create matrix with rows as days and columns as ticker mentions,
        #with values being the number of mentions for each day
        for i in range(len(self.full_df)):
            row = self.full_df.iloc[i]
            row_date = row['date']
            if row['tickers'] != []:
                for ticker in row['tickers']: 
                    ticker_matrix.loc[[row_date],[ticker]] += 1 #add mention

        #we do not want to work with tickers mentioned only once as that is too small of a dataset, so we leave these out
        above_one = ticker_matrix.apply(sum,axis = 0)>1
        trimmed_df = ticker_matrix[above_one.index[above_one]]#['NDAQ']

        return trimmed_df
    
    def get_info(self):
        #summary statistics
        print(self.full_df.shape)
        print()
        print(self.full_df.head())
        print()
        print('Data from each subreddit:')
        print(self.full_df.groupby('subreddit')['post_id'].count()) #how much data from each subreddit
        print()
        print('Comments and posts:')
        print(self.full_df.groupby('text_type')['post_id'].count()) #how much posts and comments
        print()
        print('Entries containing a ticker mention from our dictionary:')
        print(self.full_df.loc[self.full_df['tickers'].map(lambda d: len(d)) > 0]['post_id'].count()) 
        print()
        print("Min and max date:")
        print(self.ticker_matrix.index.min())
        print(self.ticker_matrix.index.max())
        print()
        print("Top ticker mentions in one day:")
        print(self.ticker_matrix.apply(max).sort_values(ascending = False).head())
        print()
        print("Top days:")
        print(self.ticker_matrix.apply(sum,axis = 1).sort_values(ascending = False).head())