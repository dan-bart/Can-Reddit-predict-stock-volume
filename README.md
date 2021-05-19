# Can Reddit predict traded stock volume?

Collaborative work with Petr Cala for IES Course Data Processing in Python. We scrape mentions of stock tickers on selected subreddits using Reddit API (https://praw.readthedocs.io/en/latest/). For each post and comment, we look for ticker mentions or the security full names in the text. Than we model the relationship and observe correlation with daily traded volume of featured stocks from Yahoo finance.

The purpose of this project is to prove the authors ability to collaboratively produce a complex project with proper structure and clean code. We show our ability to scrape data and efficiently use object oriented programming. Reaching a definite conclusion for our project topic would require advanced use of NLP (contextual analysis).

## Project structure


```
│   daily_scraper.py                          #runs Extractor daily and saves the output in destined folder
│   README.md
│   
├───data
│   │   credentials.json                      #our Reddit credentials (dev-enabled account created for this project) 
│   │   yahoo_data.csv
│   │   
│   ├───daily_parquet_data                    #folder containing daily scraped Reddit mentioned in parquet format
│               
└───tools
    │   csv_to_parquet.py                     #one time use script when we decided to switch from CSVs to parquet format
    │   Extractor.py                          #Uses PRAW framework to scrape posts and comment form selected stock subreddits
    │   Models.ipynb
    │   Stock_Dictionary.py                   #Returns a json dictionary of S&P ticker variants.
    │   Ticker_Counter.py                     #Standardizes parquet data into a matrix with daily counts for each ticker.
    │   Yahoo_extractor.ipynb
```


## How to run
Clone the repository and run the **presentation.ipynb** to observe the outcome. 
The Reddit data is not retrospectively accessible, but it is possible to crawl current day data by running **daily_scraper.py**.
