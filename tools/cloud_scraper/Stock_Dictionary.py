import pandas as pd
import re
import lxml

#lots of manual work and linear coding here, no need to use OOP 

def get_dict():
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    scrapedSAP = data[0]
    #removing tickers with length one
    data = scrapedSAP[scrapedSAP['Symbol'].str.len()>1][["Symbol","Security"]]
    data = pd.DataFrame(data)

    #populate the dictionary
    stockDict = dict()
    for i,r in data.iterrows():
        ticker = r[0]
        full_name = r[1]
        stockDict[ticker] = [ticker,full_name]

    #expand possibilities for selected indexes
    stockDict['SPGI'][1] = 'S&P'
    stockDict['SPGI'].append("S&P500")
    stockDict['NDAQ'].append("NASDAQ100")
    stockDict['DOW'].append("DJIA")
    stockDict['DOW'].append("DJI")
    stockDict['ARKK'] = ['ARKK','ARK Innovation'] #because ark funds are popular nowadays
    #manually remove stocks which have tickers similar to common english words or could likely be matched by mistake
    stockDict.pop('ARE')
    stockDict.pop('ALL')
    stockDict.pop('CAT')
    stockDict.pop('COST')
    stockDict.pop('DD') #due diligence
    stockDict.pop('FAST')
    stockDict.pop('IT')
    stockDict.pop('PEAK')
    stockDict.pop('WELL')
    stockDict.pop('HAS')
    stockDict.pop('MA') #jack ma from alibaba
    stockDict.pop('MET')
    stockDict.pop('RE') #you're
    stockDict.pop('INFO')
    stockDict.pop('KEY')
    stockDict.pop('KEYS')
    stockDict.pop('LOW')
    stockDict.pop('TAP')
    stockDict.pop('POOL')
    stockDict.pop('NOW')
    stockDict.pop('SO')
    stockDict.pop('SEE')
    stockDict.pop('MAR')
    stockDict.pop('GOOG')
    stockDict['TGT'] = [stockDict['TGT'][0]] #no full word "target", often used in context of a price target


    #expand on some commonly used names
    stockDict['GOOGL'].append('alphabet')
    stockDict['GOOGL'].append('google')
    stockDict['BRK.B'].append('berkshire')


    #remove unwanted suffixes from the tickers

    unwantedSuffix = [' & Co\.$',
                    ' Co\. Inc\.',
                    ' Co\.$',
                    ' Company$',
                    ' Ltd$',
                    ' Ltd\.$',
                    ', Inc.$',
                    ' Inc\.$',
                    ' Inc$',
                    ' Corp\.$',
                    ' Corp$',
                    ' Inc$',
                    ' Co$',
                    ' Corporation$',
                    "\.com$"]

    for key in stockDict:
        if key != "TGT":
            for j in unwantedSuffix:
                name = stockDict[key][1]
                stockDict[key][1] = re.sub(j,'',name)


    #make another alternative with $ at start, reddit uses it for better filtering, so its used
    for key in stockDict:
        stockDict[key].append('$'+stockDict[key][0])  


    #make values into full word matches for re.match()
    for key in stockDict:
        values = stockDict[key]
        newValues=[]
        for value in values:
            newValues.append("\\b"+value+"\\b")
        stockDict[key] = newValues

    #google specifically is used in many .com link, so we only allow an empty space after it for matching
    stockDict['GOOGL'][3] = '\\bgoogle '

    #observe dictionary
    mainDict = {}
    for i in stockDict.keys():
        combined = "(" + ")|(".join(stockDict[i]) + ")"
        mainDict[i] = combined

    return mainDict
