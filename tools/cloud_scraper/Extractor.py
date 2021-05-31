import praw
import re
import time
import json
import pandas as pd

class Reddit_Extractor:
    def __init__(self, credentials):
        self.reddit =  praw.Reddit(
            client_id = credentials['client_id'],
            client_secret = credentials['client_secret'],
            user_agent = credentials['user_agent'],
            username = credentials['username'],
            password = credentials['password'])
    def subredditData(self,subreddit,limit = 10):
        postList = self.reddit.subreddit(subreddit).hot(limit=limit)
        return postList     
        
    def showcasePrint(self,postList):
        for post in postList:
            print(post.title)
            print(post.distinguished)
            print(post.link_flair_text)
            '''for top_level_comment in submission.comments:
                print(i)
                print(top_level_comment.link_id)   
                print(top_level_comment.body)      
                print(top_level_comment.created_utc)
                i+=1'''    
            
    def subredditDF(self,postList):
        df = pd.DataFrame({'post_id': pd.Series([], dtype='str'),
                        'comment_id': pd.Series([], dtype='str'),
                        'subreddit': pd.Series([], dtype='str'),
                        'text_type': pd.Series([], dtype='str'),
                        'epoch_time': pd.Series([], dtype='int'),
                        'text': pd.Series([], dtype='str'),
                        'tickers': pd.Series([], dtype='str')})  
        for post in postList:
            #post extraction
            post_row = pd.DataFrame({'post_id': pd.Series([post.id], dtype='str'),
                                    'comment_id': pd.Series([], dtype='str'),
                                    'subreddit': pd.Series([post.subreddit.display_name], dtype='str'),
                                    'text_type': pd.Series(['post'], dtype='str'),
                                    'epoch_time': pd.Series([post.created_utc], dtype='int'),
                                    'text': pd.Series([post.title+post.selftext], dtype='str'),
                                   'tickers': pd.Series([], dtype='str')})   
            df = df.append(post_row) 
            #comments extraction
            submission = self.reddit.submission(id=post.id)
            submission.comment_sort = 'best'
            submission.comment_limit = 5
            for top_level_comment in submission.comments:
                if isinstance(top_level_comment, praw.models.MoreComments):
                    continue                    
                new_row = pd.DataFrame({'post_id': pd.Series([post.id], dtype='str'),
                                        'comment_id': pd.Series([top_level_comment.id], dtype='str'),
                                        'subreddit': pd.Series([top_level_comment.subreddit.display_name], dtype='str'),
                                        'text_type': pd.Series(['comment'], dtype='str'),
                                        'epoch_time': pd.Series([top_level_comment.created_utc], dtype='int'),
                                        'text': pd.Series([top_level_comment.body], dtype='str'),
                                       'tickers': pd.Series([], dtype='str')})  
                df = df.append(new_row)  
                                         
        return df
    def joinData(self,subreddits,limit):

        main_df = pd.DataFrame({'post_id': pd.Series([], dtype='str'),
                'comment_id': pd.Series([], dtype='str'),
                'subreddit': pd.Series([], dtype='str'),
                'text_type': pd.Series([], dtype='str'),
                'epoch_time': pd.Series([], dtype='int'),
                'text': pd.Series([], dtype='str'),
                'tickers': pd.Series([], dtype='str')})
        for subreddit in subreddits:
            posts_test = self.subredditData(subreddit = subreddit,limit = limit)
            start = time.time()
            df = self.subredditDF(posts_test)
            end = time.time()
            print("Elapsed time for subreddit",subreddit)
            print(end-start)
            main_df = main_df.append(df)
        return main_df


#searches text for ticker mentions defined in our stock dictionary 
class Ticker_Extractor:
    def __init__(self,dict):
        self.stockDict = dict
    def openDict(self):
        with open('stockDict.json') as json_file: 
            stockDict = json.load(json_file) #
        return stockDict
    def findNewTicker(self, postText,tickerDict): #method to expand possible dictionaries of stock tickers
        pass
    
    def tickerCounter(self, postText): #append number of mentions to dataframe
        #pattern  = r"(?:\$)?[A-Z][A-Z\d]+"
        mentionedTickers = []
        for key in self.stockDict:
            if re.search(self.stockDict[key],postText,re.IGNORECASE) is not None:
                mentionedTickers.append(key)
        mentionedTickers = list(set(mentionedTickers)) #unique values
        if len(mentionedTickers)>0:
            return mentionedTickers
        else:
            return None