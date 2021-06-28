from bs4 import BeautifulSoup
import requests
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np

#path = "C:\\Users\\hso20\\Python\\Project\\YahooData\\"

class Yahoo_Extractor:
    '''Scrape stock data from Yahoo finance and return this data as a pandas data frame or an excel file
    
    :usage:
        Y = Yahoo_Extractor('2020-05-15','2021-05-15')  
        Y.create_excel(path)
    '''
    def __init__(self, start_date, end_date):
        '''The constructor for the Yahoo_Extractor class.
        
        :args:
        - start_date (datetime.object): The date from which the stock volume traded should be extracted.
        - end_date (datetime.object): The date until which the stock volume traded should be extracted.
        '''
        self.start_date = start_date
        self.end_date = end_date

    def get_dates(self):
        '''Return a list of dates for the range inhereted from the class constructor.
        
        :usage:
            self.get_dates()
            
        :returns:
        - dateList (list): A list of dates for the range inhereted from the class constructor.
        '''
        dateList = []
        start = date.fromisoformat(self.start_date)
        end = date.fromisoformat(self.end_date)
        numdays = (end - start).days
        for x in range (0, numdays):
            date_name = (end - datetime.timedelta(days = x)).strftime("%b %d, %Y")
            dateList.append(date_name)
            
        return dateList
    
    def get_acro_list(self):
        '''Return the list of 500 S&P stocks in a list.
        
        :usage:
            self.get_accro_list()
        
        :returns:
        - acro_list (list): A list of 500 S&P stocks.
        
        :note:
        - The constructor inherently creates an acro list for the stocks that appear in both data sets. It may thus
            seem redundant having a method to get all 500 S&P stocks. However, we feel like this may be used
            to compare various datasets with the S&P list as a benchmark, so we include the method here.
        '''
        tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        acro_list = tickers[0]['Symbol'].tolist()             #creating a list of tickers
        acro_list = [a.replace('.', '-') for a in acro_list]
    
        return acro_list    

    def get_data(self, acro):
        '''Specify a ticker of a stock and return a data frame for said stock containing information
            about volume traded.
            
        :args:
        - acro (str): A ticker representing the stock for which the information should be extracted.
        
        :usage:
            self.get_data('AAPL')
            
        :returns:
        - stocksDF (pd.DataFrame): A data frame containing information about volume traded for the desired stock.
        '''
        link = 'https://finance.yahoo.com/quote/' + acro + '/history?p=' + acro
        r = requests.get(link)
        r.encoding = 'utf-8-sig'
        soup = BeautifulSoup(r.text, 'html.parser')
        dates = self.get_dates()
        stocksDF = pd.DataFrame(index = dates, columns = [acro]) #an empty data frame for the stock
        for tr in soup.findAll('tr', {'class':'BdT Bdc($seperatorColor) Ta(end) Fz(s) Whs(nw)'}):
            Date = pd.Series(tr.contents[0].text)
            if Date.isin(dates)[0] == True: 
                try:
                    Volume = tr.contents[6].text.replace(",","")
                except IndexError:
                    Volume = None
                stocksDF.loc[Date,:] = Volume #replace NaNs with the traded volume
            else:
                continue
            
        return stocksDF
    
    def join_data(self, acro = None):
        '''Specify the list of desired tickers and return a data frame containing information about
            stock volume traded for stocks represented by said tickers. If acro is not specified,
            get the acro list from the S&P 500.
            
        :args:
        - acro (list): List of tickers to be used in the data frame.
        
        :usage:
            self.join_data()
            
        :returns:
        - data (pd.DataFrame): A data frame containing stock volume traded information for the desired stocks.
        '''
        if acro == None:
            acro = self.get_acro_list()
            
        data = pd.DataFrame()
        
        for a in acro:
            start = time.time()
            stocksDF = self.get_data(a)
            data = pd.concat([data, stocksDF], axis = 1, copy = False)
            end = time.time()
            print('Ellapsed time for stock', a)
            print(end-start)
            
        data = data.dropna(axis = 0, how = 'all')

        return data
    
    def create_excel(self, path, data = None):
        '''Specify the data to create a csv file from and a path to save this file under and create said file.
            If data is not specified, use data for all stocks from the S&P index.
        
        :args:
        - data (pd.DataFrame): Data to be used in the creation of the csv file.
        - path (str): A path to save the csv file to.
        
        :usage:
            self.create_excel(path)
            
        :returns:
        - None: A csv file in the specified path containing stock trading data.
        '''
        if data == None:
            data = self.join_data()
            
        today = date.today()
        stringEpoch = today.strftime("%d_%m_%y")
        s = ''
        nameVars = [path,stringEpoch,".csv"]
        outputDF.to_csv(path_or_buf=s.join(nameVars),sep = ',', index=True, encoding='utf-8-sig')
        
        return None

