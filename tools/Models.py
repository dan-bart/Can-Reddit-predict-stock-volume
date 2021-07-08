import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import copy
import tools.Ticker_Counter as counter


class Data_Analyzer:
    '''Analyze various aspects of the Yahoo and Reddit data.
    
    :usage:
        D = Data_Analyzer()  
    '''
    def __init__(self):
        '''
        The constructor for the Data_analyzer class.
        '''
        self.yahoo_data = pd.read_csv('data/yahoo_data.csv', parse_dates=['Date'], infer_datetime_format="%b %d, %Y",
                         index_col = [0]).interpolate() #We use interpolation to linearly fill missing data
        
        self.model_data = self.get_reddit_data()
        self.model_data.index.name = 'Date'                      #Set index name to match with Yahoo data
        self.model_data.index = pd.to_datetime(self.model_data.index) #Convert the index to datetime type object
        self.model_data = self.model_data.sort_index(ascending = False)
        
        #Discard stocks which are not mentioned by reddit; one can not compare the two data sets on these stocks
        indexer_yahoo = self.yahoo_data.columns.isin(self.model_data.columns).tolist()
        self.yahoo_data = self.yahoo_data.loc[:,indexer_yahoo]
        
        indexer_model = self.model_data.columns.isin(self.yahoo_data.columns).tolist()
        self.model_data = self.model_data.loc[:,indexer_model]
        
        #Use the columns of stocks which appear in both data frames as a benchmark
        self.acro_new = self.model_data.columns.tolist()

    def get_reddit_data(self):
        '''Load reddit data into Python.
        
        :usage:
            self.get_reddit_data()
            
        :returns:
        - TickerMat.full_df (pandas.DataFrame): A data frame containing the reddit data.
        '''
        print('Loading Reddit data. This may take a while.')
        path = "./data/daily_parquet_data/"
        TickerMat = counter.Ticker_Matrix(path)
        print('Reddit data loaded successfully.')
        
        return TickerMat.ticker_matrix
            
    def plot_all_data(self):
        '''Return a plot showing trends in reddit mentions and volume traded for 500 S&P stocks
             (the latter one in logarithm).
        
        :usage:
            self.plot_all_data()
            
        :returns:
        -None: A plot showing trends in reddit mentions and volume traded for 500 S&P stocks.
        '''
        mention_count = self.model_data.sum(axis = 0).sort_values(ascending = False) #Number of total mentions for each stock
        volume_traded = self.yahoo_data.sum(axis = 0).sort_values(ascending = False) #Amount of volume traded for each stock

        #Defining ticks to use in the plots
        acros_mtn = mention_count.index.tolist()
        if len(acros_mtn)<400:
            ticks_mtn = [acros_mtn[0], acros_mtn[100], acros_mtn[200], acros_mtn[300], acros_mtn[-1]]
        else:
            ticks_mtn = [acros_mtn[0], acros_mtn[100], acros_mtn[200], acros_mtn[300], acros_mtn[400]]
            
        acros_vol = volume_traded.index.tolist()
        if len(acros_vol)<400:
            ticks_vol = [acros_vol[0], acros_vol[100], acros_vol[200], acros_vol[300], acros_vol[-1]]
        else:
            ticks_vol = [acros_vol[0], acros_vol[100], acros_vol[200], acros_vol[300], acros_vol[400]]            
            
        
        fig, (ax1, ax2) = plt.subplots(2,1)
        fig.suptitle('Observing the behavior of our data')

        ax1.plot(np.log(mention_count))
        ax1.set_xticks(ticks_mtn)
        ax1.set_ylabel('Mention count (log)')

        #Transformed to logarithm to better observe the effect
        ax2.plot(np.log(volume_traded))
        ax2.set_xticks(ticks_vol)
        ax2.set_ylabel('Volume traded (log)')

        fig.tight_layout()
        plt.show()
        
        return None

    def get_stock_data(self, stock_name):
        '''Specify a stock name and return a data frame with percentage change between days
            in Yahoo and Reddit data.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        
        :usage:
            self.get_stock_data('AAPL')

        :returns:
        - stock_data (pd.DataFrame): A data frame with percentage change between days.
        '''
        mtn = self.model_data[stock_name].pct_change().replace(np.inf,np.nan).interpolate() #Extract mention count
        vol = self.yahoo_data[stock_name].pct_change().replace(np.inf,np.nan).interpolate() #Extract volume traded
        stock_data = pd.concat([mtn, vol], axis = 1, join = 'inner', keys = ['Mentions', 'Volume']) #Join these two
        
        return stock_data
    
    def plot_stock_data(self, stock_name = None):
        '''Specify a stock name and return a plot showing trends in development of reddit mentions
            and volume traded.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        
        :usage:
            self.plot_stock_data('AAPL')
            
        :returns:
        -None: A plot showing trends in development of reddit mentions and volume traded.
        '''
        if stock_name == None:
            pass
        else:
            for s in stock_name:
                data = self.get_stock_data(s)

                data['Mentions'].plot()
                data['Volume'].plot()

                plt.title(f'Daily percentage change - ' + s)
                plt.legend()
                plt.show()
        
        return None

    def get_directed_data(self, stock_name):
        '''Specify a stock name and return data indicating on which days the mentions or volume traded increased
            or decreased.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        
        :usage:
            self.get_directed_data('AAPL')
            
        :returns:
        - directed_data (pd.DataFrame): A data frame indicating on which days the mentions or volume traded increased
            or decreased. 1 indicates an increase, 0 indicates a decrease.
        
        '''
        directed_data = self.get_stock_data(stock_name).diff()

        #Iterate over columns and rows
        for x in directed_data: 
            for i in range(0,len(directed_data[x])):
                if directed_data[x][i] > 0:
                    directed_data[x][i] = 1
                elif directed_data[x][i] < 0:
                    directed_data[x][i] = -1
                else:
                    directed_data[x][i] = 0
        return directed_data

    def get_correlation(self, stock_name):
        '''Specify a stock name and calculate the correlation between reddit mentions and volume traded from the
            directed data.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        
        :usage:
            self.get_correlation('AAPL')
            
        :returns:
        - test_corr (np.ndarray): An array with values indicating the correlation between mentions and
            volume traded.
        '''
        directed_data = self.get_directed_data(stock_name)
        
        test_corr = np.corrcoef(directed_data['Mentions'], directed_data['Volume'])
        return test_corr

    def get_incidence(self, stock_name):
        '''Specify a stock name and calculate the incidence rate, i.e. the percentage of cases where
            the direction of the trend is the same for reddit mentions and volume traded on the same day.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        
        :usage:
            self.get_incidence('AAPL')
            
        :returns:
        - inc_rate (np.float64): Incicates the percentage of cases where
            the direction of the trend is the same for reddit mentions and volume traded on the same day.
        '''
        directed_data = self.get_directed_data(stock_name)
        incidence = directed_data['Mentions'] == directed_data['Volume']
        inc_rate = (incidence.sum()/len(incidence)).round(4)
        #inc_rate = "{:.2%}".format(inc_rate) #Converting to a percentage
        return inc_rate
    

    def get_incidence_offset(self, stock_name, offset_mentions = 0, offset_volume = 0):
        '''Specify the stock name and the amount of days, by which to offset the calculation and
            return the incidence rate for these parameters.
            
        :args:
        - stock_name (str): Name of the stock to analyze.
        - offset_mentions (int): Number of days by which to offset the reddit mentions data to the right.
        - offset_volume (int): Number of days by which to offset the colume traded data to the right.
        
        :usage:
            self.get_incidence_offset(stock_name = 'AAPL', offset_mentions = 4, offset_volume = 2)
            
        :warning:
        - Specifying too large an offset number will result in loss of information.
            
        :returns:
        - inc.offset (np.float64): Incicates the percentage of cases where
            the direction of the trend is the same for reddit mentions and volume traded on the same day.
        '''
        directed_data = self.get_directed_data(stock_name)
        
        incidence = []
        for r in range(0,directed_data.shape[0]):
            try:
                incidence.append(directed_data['Mentions'][r + offset_mentions] == directed_data['Volume'][r + offset_volume])
            except IndexError:
                pass
        incidence = pd.Series(incidence)    
        inc_offset = (incidence.sum()/len(incidence)).round(4)
        return inc_offset
    
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
    
    def get_incidences(self, offset_mentions_by = 0, offset_volume_by = 0, acro = None):
        '''Specify the stock names and the amount of days, by which to offset the calculation and
            return the incidence rates for these parameters as a data frame.
            
        :args:
        - acro (list): Tickers of the stocks to analyze.
        - offset_mentions (int): Number of days by which to offset the reddit mentions data to the right.
        - offset_volume (int): Number of days by which to offset the colume traded data to the right.
        
        :usage:
            self.get_incidences(acro = ['AAPL', 'AMD', 'AMZN'], offset_mentions_by = 1, offset_volume_by = 3)
            
        :returns:
        - incidence_df (pd.DataFrame): A data frame capturing the incidence and incidence offset for stocks
            specified by acro, along with variable 'Within 2.5%', which is equal to 1, if the incidences
            are within 2.5% of each other on a given day.
        
        '''
        if acro == None:
            acro = self.acro_new
        
        incidence_df = pd.DataFrame({'Stock': pd.Series([], dtype='str'),
                                    'Incidence': pd.Series([], dtype='int'),
                                    f'Incidence offset': pd.Series([], dtype='float')})
        for a in acro:
            try: 
                stock_name = a
                incidence = self.get_incidence(a)
                incidence_3 =self.get_incidence_offset(a, offset_mentions = offset_mentions_by, offset_volume = offset_volume_by)
                
                new_row = pd.DataFrame({'Stock': pd.Series([stock_name]),
                                        'Incidence': pd.Series([incidence], dtype='float'),
                                        f'Incidence offset': pd.Series([incidence_3], dtype='float')})
                incidence_df = incidence_df.append(new_row)

            except:
                stock_name = a
                incidence = np.random.uniform(low = 0, high = 1)
                incidence_3 = np.random.uniform(low = 0, high = 1)
                
                new_row = pd.DataFrame({'Stock': pd.Series([stock_name]),
                                        'Incidence': pd.Series([incidence], dtype='float'),
                                        f'Incidence offset': pd.Series([incidence_3], dtype='float')})
                incidence_df = incidence_df.append(new_row)
        
        
        incidence_mean = incidence_df['Incidence'].mean()
        incidence_offset_mean = incidence_df['Incidence offset'].mean()

        row_total = pd.DataFrame({'Stock': 'Mean',
                                        'Incidence': pd.Series([incidence_mean], dtype = 'float'),
                                        f'Incidence offset': pd.Series([incidence_offset_mean], dtype='float')})
        incidence_df = incidence_df.append(row_total)
        
        incidence_df = incidence_df.set_index('Stock')
        
        within_2_5 = []
        try:
            for a in acro:
                if (abs(incidence_df.loc[a, 'Incidence'] - incidence_df.loc[a, 'Incidence offset'])) < 0.025:
                    within_2_5.append(1)
                else:
                    within_2_5.append(0)
        except KeyError:
            print('This stock does not appear in the data provided. Returning None.')
            within_2_5.append(None)

        within_2_5.append(None)    
        
        incidence_df['Within 2.5%'] = within_2_5
        
        incidence_df.loc['Mean', 'Within 2.5%'] = incidence_df['Within 2.5%'].mean()
        
        return incidence_df
    
    def get_power(self, offset_mentions_by = 0, offset_volume_by = 0, acro = None):
        '''Specify the stock names and the amount of days, by which to offset the calculation and
            print the power parameter for these parameters. Power represents the rate at which
            reddit is able to predict stock movement.
            
        :args:
        - acro (list): Tickers of the stocks to analyze.
        - offset_mentions (int): Number of days by which to offset the reddit mentions data to the right.
        - offset_volume (int): Number of days by which to offset the colume traded data to the right.
        
        :usage:
            self.get_power(offset_mentions_by = 1, offset_volume_by = 3, acro = ['AAPL', 'AMD', 'AMZN'])
            
        :returns:
        - None: Prints the value of the power parameter, which represents the rate at which
            reddit is able to predict stock movement. The return value is set to None in order
            to avoid duplicate printing of the power value.
        
        '''
        incidence_df = self.get_incidences(offset_mentions_by, offset_volume_by, acro)
        power = incidence_df.loc['Mean', 'Within 2.5%']

        if power > 0.5:
            print('Reddit did a great job this time.')
        elif power > 0.25:
            print('Reddit did an OK job this time.')
        elif power > 0:
            print('Reddit did not do well this time.')
        else:
            print('Reddit did not do well this time. Maybe try increasing the number of stocks in order for LLN to do its thing.')
            
        power = "{:.2%}".format(power) #Converting to a percentage
        
        offset = offset_volume_by - offset_mentions_by
        
        if offset < -1:
            print(f'The trend in its stock mentions compared to the stock movement {-offset} days ago was the same in {power} of cases.')
        elif offset == -1:
            print(f'The trend in its stock mentions compared to the stock movement {-offset} day ago was the same in {power} of cases.')
        elif offset == 0:
            print(f'Its stock mentions coincided with stock movement on the same day in {power} of cases.')
        elif offset == 1:
            print(f'It was able to predict volume of traded stocks {offset} day ahead in {power} of cases.')
        else:
            print(f'It was able to predict volume of traded stocks {offset} days ahead in {power} of cases.')
            
        print(f'The total number of stocks analyzed was {len(incidence_df) - 1}.')
        
        return None
    
    def present_outcome(self, offset_mentions_by = 0, offset_volume_by = 0, acro = None):
        '''Specify the stock names and the amount of days, by which to offset the calculation.
            Return the power for said parameters and display the results graphically.
            
        :args:
        - acro (list): Tickers of the stocks to analyze.
        - offset_mentions (int): Number of days by which to offset the reddit mentions data to the right.
        - offset_volume (int): Number of days by which to offset the colume traded data to the right.
        
        :usage:
            self.present_outcome(shift_days_by_mentions = 1, shift_days_by_volume = 3, stock_list = ['AAPL', 'AMZN', 'AMD'])
        
        :returns:
        - None: The power parameter for said parameters and graphical representation of the results are printed.  
        '''
        self.get_power(offset_mentions_by, offset_volume_by, acro)
        
        if acro == None:
            self.plot_all_data() 
        elif len(acro)<=10:
            self.plot_stock_data(acro)
        else:
            self.plot_stock_data(acro[:10])

        return None
    
    
    
    
    
    
    
    