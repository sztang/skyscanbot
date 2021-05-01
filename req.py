import requests, datetime, os
import pandas as pd
from time import sleep

# url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/browsequotes/v1.0/US/USD/en-US/EWR-sky/GR/2021-06-21/2021-07-01"

def apicall(startdate,enddate):
    # defaults
    # origin = 'NYCA-sky' # New York any
    origin = 'GR' # For Greece to Croatia
    # dest = 'GR' # Greece any
    dest = 'HR' # Greece any
    # startdate = '2021-06-21'
    # enddate = '2021-07-01'

    baseurl = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/browsequotes/v1.0/US/USD/en-US/{}/{}/{}/{}"
    url = baseurl.format(origin,dest,startdate,enddate)

    headers = {
        'x-rapidapi-key': "922c4aef19mshb4910461c286d9fp1dd180jsnd4273d2cd17c",
        'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com"
        }

    response = requests.get(url,headers=headers)
    quotes = pd.DataFrame.from_dict(response.json()['Quotes'])
    carriers = pd.DataFrame.from_dict(response.json()['Carriers'])
    places = pd.DataFrame.from_dict(response.json()['Places'])

    for i in quotes.index:
        quotes.loc[i,'OutCarrier'] = carriers.loc[carriers['CarrierId'] == quotes.loc[i,'OutboundLeg']['CarrierIds'][0],'Name'].values[0]
        quotes.loc[i,'OutOrigin'] = places.loc[places['PlaceId'] == quotes.loc[i,'OutboundLeg']['OriginId'],'Name'].values[0]
        quotes.loc[i,'OutDest'] = places.loc[places['PlaceId'] == quotes.loc[i,'OutboundLeg']['DestinationId'],'Name'].values[0]
        quotes.loc[i,'OutDate'] = quotes.loc[i,'OutboundLeg']['DepartureDate'].split('T')[0]
        quotes.loc[i,'InCarrier'] = carriers.loc[carriers['CarrierId'] == quotes.loc[i,'InboundLeg']['CarrierIds'][0],'Name'].values[0]
        quotes.loc[i,'InOrigin'] = places.loc[places['PlaceId'] == quotes.loc[i,'InboundLeg']['OriginId'],'Name'].values[0]
        quotes.loc[i,'InDest'] = places.loc[places['PlaceId'] == quotes.loc[i,'InboundLeg']['DestinationId'],'Name'].values[0]
        quotes.loc[i,'InDate'] = quotes.loc[i,'InboundLeg']['DepartureDate'].split('T')[0]

    return quotes

# call api, get response
# search EWR-sky, JFK-sky, LGA-sky - or just run NYCA-sky
# search in range 06-15 to 07-10
# change dates for 9,10,11,12,13,14-day trips

def datepairs(datelist,duration): #returns start/end date pairs within a range of dates, x-days apart, in %Y-%m-%d string format
    datepairlist = []
    for d in datelist:
        d1 = d + datetime.timedelta(days=duration)
        if d1 not in datelist:
            break
        datepairlist.append((d.strftime('%Y-%m-%d'),d1.strftime('%Y-%m-%d')))
    return datepairlist

def blockquotes(datepair,duration):
    frames = []
    for d in datepair:
        quote = apicall(d[0],d[1])
        frames.append(quote)
    blockquote = pd.concat(frames).reset_index(drop=True)
    blockquote['tripLength'] = duration
    return blockquote

def analyzequotes(quotemaster,triplength):
    quotemin = []
    for x in triplength:
        quoteslice = quotemaster.loc[quotemaster['tripLength']==x]
        quotemin.append(quoteslice['MinPrice'].idxmin())
    summarymin = quotemaster.iloc[quotemaster.index.isin(quotemin)]

    lastmin = pd.read_csv('MinimumQuotes.csv')
    
    for x in triplength:
        lastbestprice = lastmin.loc[lastmin['tripLength']==x,'MinPrice'].values[0]
        newbestprice = summarymin.loc[summarymin['tripLength']==x,'MinPrice'].values[0]
        if newbestprice < lastbestprice:
            print('Price for {}-day trip has DECREASED to ${}.'.format(x,newbestprice))
            print('Dates: {}-{}\nOutbound: {}-{} | {}\nInbound: {}-{} | {}'.format(
                summarymin.loc[summarymin['tripLength']==x,'OutDate'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'InDate'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'OutOrigin'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'OutDest'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'OutCarrier'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'InOrigin'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'InDest'].values[0],
                summarymin.loc[summarymin['tripLength']==x,'InCarrier'].values[0]
            ))
        elif newbestprice > lastbestprice:
            print('Price for {}-day trip has INCREASED to ${}.'.format(x,newbestprice))
        elif newbestprice == lastbestprice:
            print('Price for {}-day trip has NOT CHANGED.'.format(x))
        print('\n-------------------------------\n')

    summarymin.to_csv('MinimumQuotes.csv')
    quotemaster.to_csv('AllQuotes.csv')

if __name__ == "__main__":
    print('============================\nRunning skyscanbot...')

    # create datelist (range of dates for travel)
    base = datetime.datetime.strptime('2021-06-25','%Y-%m-%d')
    datelist = [base + datetime.timedelta(days=x) for x in range(13)]

    # decide range of trip length
    triplength = [x for x in range(3,7)]

    blocks = []
    callcount = 0
    for x in triplength:
        print('Making API calls...')
        # generate datepairs (start and end dates, x days apart)
        datepair = datepairs(datelist,x)

        if callcount + len(datepair) > 49: # keep calls per minute below 50
            print('Made {} calls, sleeping 60s...'.format(callcount))
            sleep(60)
            callcount = 0

        # run api calls for each date pair
        blockquote = blockquotes(datepair,x)
        blocks.append(blockquote)
        callcount += len(datepair)
    
    quotemaster = pd.concat(blocks).reset_index(drop=True)

    # extract cheapest quote for each trip duration, along with all relevant info for the quote
    print('Analyzing and saving quotes...\n-------------------------------\n')
    analyzequotes(quotemaster,triplength)

    print('Done.\n============================\n')

    if input('Open newest quote details? [y/n]\n') in ['y','Y']:
        os.system('open MinimumQuotes.csv')
        os.system('open AllQuotes.csv')
