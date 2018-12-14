from pyspark import SparkContext
from pyspark.sql import SQLContext
import numpy as np
import csv
from statistics import mean
from stockstats import *
import datetime
import schedule
import ftplib
import feedparser
from goose import Goose
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
import nltk
import sched
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
import pysentiment as ps
from datetime import datetime
import time
import schedule
#from requests import get
import sys


def csv_writer(path, fieldnames, data):  #this function is to write the moving avg value vs date values to csv files
    with open(path, "wb") as out_file:
        writer = csv.writer(out_file, delimiter=',')
        writer.writerow(fieldnames)
        for x in data:
            writer.writerow(x)
import pandas as pd
from datetime import datetime
import csv
from statistics import mean


df=pd.DataFrame(pd.read_csv("/home/hduser1/PycharmProjects/spark/data /UBL_01012003_09042018.csv"))

date_list=df.Date.tolist()
close_price_list=df.Close.tolist()
high_list=df.High.tolist()
low_list=df.Low.tolist()
volume_list=df.Volume.tolist()

def stoch1(close_list1):

    i=0
    j=14
    lowest_low = min(close_list1[i:j])
    highest_high = max(close_list1[i:j])
    if (highest_high - lowest_low == 0):
        i+=1
        j+=1
        pass
    else:
        stoch = (close_list1[13] - lowest_low) / (highest_high - lowest_low) * 100
    return stoch
def stoch_rsi(close_list1, date_list1,time_list1,sname):
    i = 0
    j = 14
    stoch_rsi_list = []
    stoch_time_list=[]
    stochrsi_time_list=[]
    stochrsi_effect_list=[]
    stochrsi_date_list = []
    for x in range(13, len(close_list1)):
        lowest_low = min(close_list1[i:j])
        highest_high = max(close_list1[i:j])
        if highest_high - lowest_low == 0:
            i+=1
            j+=1
            continue
        else:
            stoch = (close_list1[x] - lowest_low) / (highest_high - lowest_low)
            if stoch<=45:
                stochrsi_effect_list.append('SELL')
            elif stoch>=55:
                stochrsi_effect_list.append('BUY')

            else:
                stochrsi_effect_list.append('NEUTRAL')
            stoch_rsi_list.append(abs(round(stoch, 3)))
            stochrsi_date_list.append(date_list1[x])
            stochrsi_time_list.append(time_list1[x])


        i += 1
        j += 1
    return ['stochrsi',stoch_rsi_list[-1],stochrsi_effect_list[-1]]
    # data = [stochrsi_date_list, stochrsi_time_list,stoch_rsi_list, stochrsi_effect_list]
    # fieldnames = ['date', 'time', 'stochrsi', 'effect']
    # csv_writer(sname + '/live/' + sname + '_hourly_RSI.csv', fieldnames, data)
def bollinger_bands(close_list1, date_list1,time_list1,sname):
    moving_price_list = []  # list of moving avg
    # cs= chuck size no of days for moving average
    cs = 10
    i = 0
    j = 20
    upper_band = []
    lower_band = []
    time_bb_list=[]
    middle_band=[]
    date_bb_list=[]
    # here moving avg is calculated and sotred in list
    for x in range(19, len(close_list1)):
        mav = sum(close_list1[i:j]) / cs
        middle_band.append(round(mav,2))
        a = np.array(close_list1[i:j])
        time_bb_list.append(time_list1[x])
        date_bb_list.append(date_list1[x])
        std = np.std(a, axis=0)
        upper_band.append(round(mav + (std * 2),2))
        lower_band.append(round(mav - (std * 2),2))



        i += 1
        j += 1

    data=[date_bb_list,time_bb_list,upper_band,middle_band,lower_band]
    fieldnames = ['date', 'time', 'upper_band','middle_band','lower_band']
    csv_writer(sname + '/live/' + sname + "_bollinger.csv", fieldnames, data)
def stoch(close_list1,date_list1,time_list1,sname):
    i = 0
    j = 14
    stoch_date_list=[]
    stoch_time_list=[]
    stoch_list=[]
    stoch_effect_list=[]

    for x in range(13, len(close_list1)):
        lowest_low = min(close_list1[i:j])
        highest_high = max(close_list1[i:j])
        if (highest_high - lowest_low == 0):
            i+=1
            j+=1
            continue
        else:
            stoch = (close_list1[x] - lowest_low) / (highest_high - lowest_low) * 100
            stoch_list.append(abs(round(stoch, 3)))
            if stoch<=45:
                stoch_effect_list.append('SELL')
            elif stoch>=55:
                stoch_effect_list.append('BUY')

            else:
                stoch_effect_list.append('NEUTRAL')


        stoch_date_list.append(date_list1[x])

        i += 1
        j += 1

    return['STOCH',stoch_list[-1],stoch_effect_list[-1]]
def rsi1(close_list1):
    gain = []
    loss = []
    rsi_list = []
    rsi_date_list = []
    rsi_time_list=[]

    for x in range(0, 14):
        change = close_list1[x + 1] - close_list1[x]
        if change > 0:
            gain.append(change)
        elif change < 0:
            loss.append(abs(change))

    avg_gain = sum(gain) / 14
    avg_loss = sum(loss) / 14
    RS = avg_gain / avg_loss
    RSI = 100 - (100 / (1 + RS))
    return RSI
def rsi(close_list1,date_list1,time_list1,sname):
    gain = []
    loss = []
    rsi_list = []
    rsi_date_list = []
    rsi_time_list=[]

    for x in range(0, 14):
        change = close_list1[x + 1] - close_list1[x]
        if change > 0:
            gain.append(change)
        elif change < 0:
            loss.append(abs(change))

    avg_gain = sum(gain) / 14
    avg_loss = sum(loss) / 14
    RS = avg_gain / avg_loss
    RSI = 100 - (100 / (1 + RS))
    list_effect=[]

    for x in range(14, len(close_list1)):
        if RSI>=70:
            list_effect.append('BUY')
        elif RSI<=30:
            list_effect.append('SELL')
        else:
            list_effect.append('NEUTRAL')
        rsi_list.append(round(RSI,2))


        rsi_date_list.append(date_list1[x])
        rsi_time_list.append(time_list1[x])
        change = close_list1[x] - close_list1[x - 1]
        if change > 0:
            avg_gain = (avg_gain * 13 + change) / 14
            avg_loss = (avg_loss * 13 + 0) / 14
        elif change < 0:
            avg_gain = (avg_gain * 13 + 0) / 14
            avg_loss = (avg_loss * 13 + abs(change)) / 14
        RS = avg_gain / avg_loss
        RSI = 100 - (100 / (1 + RS))
    return ['RSI',rsi_list[-1],list_effect[-1]]
    # data = [rsi_date_list,rsi_time_list, rsi_list,list_effect]
    # fieldnames = ['date','time','rsi','effect']
    # csv_writer(sname+'/live/'+sname+'_hourly_RSI.csv', fieldnames, data)
def moving_avg(close_list1,date_list1,time_list1,period):
    moving_close_list=[] #list of moving avg
    moving_date_list=[]
    moving_time_list=[]#list of dates corresponding to moving avg
    # cs= chuck size no of days for moving average

    i=0
    j=period
    #here moving avg is calculated and sotred in list
    for x in range(0,len(close_list1)-period):
        moving_close_list.append(round(sum(close_list1[i:j])/period,2))
        moving_date_list.append(date_list1[j])
        moving_time_list.append(time_list1[j])


        i+=1
        j+=1
    if moving_close_list[-1]<close_list1[-1]:
        effect='BUY'
    else:
        effect='SELL'
    return['MA'+str(period),moving_close_list[-1],effect]

    # data=[moving_date_list,moving_time_list,moving_close_list]
    # fieldnames=['date','time','price']
    # csv_writer(sname+'/live/'+sname+"_live_MA"+str(period)+".csv",fieldnames,data)#function calls and a csv file name output is generated
def macd(close_list1,date_list1,time_list1,sname):
    close_list1_12=close_list1[14:len(close_list1)]
    ema_12,m,n=exp_mov_avg(close_list1_12,date_list1,time_list1,12,sname)

    ema_26,date_list_macd,time_list_macd=exp_mov_avg(close_list1,date_list1,time_list1,26,sname)

    effect_list=[]


    macdline=[abs(a-b) for a,b in zip(ema_12,ema_26)]
    macdline=[round(x,2) for x in macdline]
    for y in range(0,len(macdline)):
        if y<0:
            effect_list.append('SELL')
        elif y>0:
            effect_list.append('BUY')
        else:
            effect_list.append('NEUTRAL')

    return ['MACD',macdline[-1],effect_list[-1]]

    # print [abs(a-b) for a,b in zip(ema_9,macdline)]
def exp_mov_avg(close_list1,date_list1,time_list1,period,sname):
    exp_close_list = []
    exp_date_list = []
    exp_time_list=[]
    # cs= chuck size no of days for moving average

    EMA = sum(close_list1[0:period]) / period

    # taking timeperiod =10
    multiplier = float(2) / (period + 1)

    for x in range(period, len(close_list1)):
        exp_close_list.append(EMA)
        exp_date_list.append(date_list1[x - 1])
        exp_time_list.append(time_list1[x-1])

        EMA = (close_list1[x] - EMA) * multiplier + EMA

    # print close_list
    # print date_list
    #
    # print exp_date_list
    # print exp_close_list
    return exp_close_list,exp_date_list,exp_time_list
def exp_mov_avg1(close_list1,date_list1,time_list1,period):
    exp_close_list = []
    exp_date_list = []
    exp_time_list=[]
    # cs= chuck size no of days for moving average

    EMA = sum(close_list1[0:period]) / period

    # taking timeperiod =10
    multiplier = float(2) / (period + 1)

    for x in range(period, len(close_list1)):
        exp_close_list.append(round(EMA,2))
        exp_date_list.append(date_list1[x - 1])
        exp_time_list.append(time_list1[x-1])

        EMA = (close_list1[x] - EMA) * multiplier + EMA
    if exp_close_list[-1]<close_list1[-1]:
        effect='BUY'
    else:
        effect='SELL'

    # print close_list
    # print date_list
    #
    # print exp_date_list
    # print exp_close_list

    return ['EMA' + str(period), exp_close_list[-1],effect]
    # data = [exp_date_list,exp_time_list, exp_close_list]
    # fieldnames = ['date','time','exp moving avg']
    # csv_writer(sname+'/'+sname+"_EMA"+str(period)+".csv", fieldnames, data)
def williams(close_list,date_list,time_list,sname):
    i = 0
    j = 14
    william_date_list = []
    william_price_list = []
    william_time_list=[]
    william_effect_list=[]
    for x in range(13, len(close_list)):
        hh = max(close_list[i:j])
        ll = min(close_list[i:j])
        if hh-ll==0:
            i+=1
            j+=1
            continue
        else:
            r = ((hh - close_list[x]) / (hh - ll)) * (-100)
            william_price_list.append(round(r,3))
            if -20<=r<=0 :
                william_effect_list.append('BUY')
            else:
                william_effect_list.append('SELL')
        i += 1
        j += 1
        william_date_list.append(date_list[x])
        william_time_list.append(time_list[x])
    return ['WILLIAM %R',william_price_list[-1],william_effect_list[-1]]

    # data = [william_date_list,william_time_list,william_price_list]
    #
    # fieldnames = ['date','time', 'william_price']
    # csv_writer(sname+'/'+sname+'_william%R.csv', fieldnames, data)

# def cci(date_list,close_list,time_list,sname):
#     tp_price_list = []
#
#     for x in range(0, len(close_list)):
#         tp_price_list.append(round(close_list[x] ,2))
#     i = 0
#     j = 20
#     print tp_price_list
#     cci_list = []
#     cci_date_list = []
#     cci_time_list=[]
#     for x in range(19, len(tp_price_list)):
#         m = mean(tp_price_list[i:j])
#
#         md = mean([abs(m - y) for y in tp_price_list[i:j]])
#         cci = (tp_price_list[x] - mean(tp_price_list[i:j])) / (0.15 * md)
#         cci_list.append(round(cci,2))
#         cci_date_list.append(date_list[x])
#         cci_time_list.append(time_list[x])
#         i+=1
#         j+=1
#     data = [cci_date_list,cci_time_list,cci_list]
#     fieldnames = ['date','time' ,'cci_price']
#     csv_writer(sname+'/'+sname+'_CCI_william.csv', fieldnames, data)
def atr(date_list,close_list,high_list,low_list,sname):
    atr = mean([h - l for h, l in zip(high_list[0:14], low_list[0:14])])
    atr_list = []
    atr_date_list = []
    for x in range(13, len(close_list)):
        atr_list.append(round(atr,2))
        atr_date_list.append(date_list[x])
        c_tr = high_list[x] - close_list[x]
        c_atr = ((atr * 13) + c_tr) / 14
        atr = c_atr
    data = [atr_date_list, atr_list]
    fieldnames = ['Date', 'atr_price']
    csv_writer(sname+'/'+sname+'_atr.csv', fieldnames, data)
def settlement_effect(sname):
    df = pd.DataFrame(pd.read_csv("/home/hduser1/PycharmProjects/spark/indicators code/settlement/"+sname+"_set.csv"))

    date_list = df.date.tolist()
    value_list = df.value.tolist()
    effect=''
    index = date_list.index('27-4-18')
    value=value_list[index]
    if (value>=50 and value<70):
        effect='BUY'
    elif value>=70 and value<=100:
        effect='STRONG BUY'
    elif value>=30 and value<50:
        effect='SELL'
    elif value>=0 and value<30:
        effect='STRONG SELL'
    return effect
def roc(date_list,close_list):
    roc_date_list = []
    roc_list = []
    for x in range(12, len(close_list)):
        close_n_ago = close_list[x - 12]
        roc = (close_list[x] - close_n_ago) / close_n_ago * 100
        roc_date_list.append(date_list[x])
        roc_list.append(roc)
    data = [roc_date_list, roc_list]
    fieldnames = ['Date', 'atr_price']
    csv_writer('atr_william.csv', fieldnames, data)

def uo(date_list,close_list,high_list,low_list,sname):
    bp_list = []
    tr_list = []
    avg7_date_list=[]
    avg14_date_list=[]
    avg28_date_list=[]
    for x in range(1, len(close_list)):
        bp_list.append(close_list[x] - min(low_list[x], close_list[x - 1]))
        tr_list.append( max(high_list[x], close_list[x - 1]) - min(low_list[x], close_list[x - 1]))
    i = 0
    j = 7
    avg7_list = []
    add_list=[' ' for x in range(0,8)]
    for x in range(7, len(bp_list)):
        avg7_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))
        avg7_date_list.append(date_list[x])
        i += 1
        j += 1
    avg7_list+=add_list
    i = 0
    j = 14
    avg14_list = []
    add_list = [' ' for x in range(0, 15)]
    for x in range(14, len(bp_list)):
        avg14_date_list.append(date_list[x])
        avg14_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))
        i += 1
        j += 1
    avg14_list+=add_list
    i = 0
    j = 28
    avg28_list = []
    add_list = [' ' for x in range(0, 29)]
    for x in range(28, len(bp_list)):
        avg28_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))
        avg28_date_list.append(date_list[x])
        i += 1
        j += 1

    avg28_list+=add_list
    data = [date_list, avg7_list,avg14_list,avg28_list]
    # print len(date_list),"len date_list"
    # print len(avg7_list),"avg7"
    # print len(avg14_list), "avg14"
    # print len(avg28_list), "avg28"

    fieldnames = ['Date', 'UO_7avg', 'UO_14avg', 'UO_28avg']

    csv_writer(sname + '/' + sname + '_uo.csv', fieldnames, data)
def file_upload(lpath,x,y):
    session = ftplib.FTP('ftp.mystocks.pk', 'mystocks', 'wnyc(%C7o,b_')
    file = open(lpath, 'rb')
    session.cwd("/public_html/data/" + x)
    session.storbinary('STOR ' + x +y+ '.csv', file)  # send the file
    file.close()  # close file and FTP
    session.quit()
    print x+y+"_Done"
def prediction(avgs_effect,indicator_effect,set_effect,prev_close):
    print "inside prediction"
    sell_count = avgs_effect.count('SELL')
    buy_count = avgs_effect.count('BUY')
    if sell_count >= 5:
        effect_avg = 'STRONG SELL'
    elif sell_count == 4:
        effect_avg = 'SELL'
    elif buy_count >= 5:
        effect_avg = 'STRONG BUY'
    elif buy_count == 4:
        effect_avg = 'BUY'
    else:
        effect_avg = 'NEUTRAL'
    if effect_avg == 'STRONG SELL':
        effect_avg_num = -(prev_close * .009)
    elif effect_avg == 'SELL':
        effect_avg_num = -(prev_close * .045)
    elif effect_avg == 'STRONG BUY':
        effect_avg_num = (prev_close * .009)
    elif effect_avg == 'BUY':
        effect_avg_num = (prev_close * .45)
    else:
        effect_avg_num = 0

    sell_count = indicator_effect.count('SELL')
    buy_count = indicator_effect.count('BUY')
    effect_indicator = ''
    if sell_count >= 4:
        effect_indicator = 'STRONG SELL'
    elif sell_count == 3:
        effect_indicator = 'SELL'
    elif buy_count >= 4:
        effect_indicator = 'STRONG BUY'
    elif buy_count == 3:
        effect_indicator = 'BUY'
    else:
        effect_indicator = 'NEUTRAL'
    if effect_indicator == 'STRONG SELL':
        effect_indicator_num = -(prev_close * .021)
    elif effect_avg == 'SELL':
        effect_indicator_num = -(prev_close * .01)
    elif effect_avg == 'STRONG BUY':
        effect_indicator_num = (prev_close * .021)
    elif effect_avg == 'BUY':
        effect_indicator_num = (prev_close * .01)
    else:
        effect_indicator_num = 0

    # settlement effect
    prev_close=prev_close-1.5
    if set_effect == 'STRONG SELL':
        effect_set_num = -(prev_close * .015)
    elif set_effect == 'SELL':
        effect_set_num = -(prev_close * .007)
    elif set_effect == 'STRONG BUY':
        effect_set_num = (prev_close * .015)
    elif set_effect == 'BUY':
        effect_set_num = (prev_close * .007)
    else:
        effect_set_num = 0
    predicted_value = prev_close + (effect_avg_num + effect_set_num + effect_indicator_num)*0.40
    return predicted_value
def previous_closing_price(sname):
    df = pd.DataFrame(pd.read_csv("/home/hduser1/Downloads/27042018.csv"))
    symbol_list = df.Symbol.tolist()
    close_list = df.Close.tolist()

    i = symbol_list.index(sname)

    return close_list[i]
def prediction1():
    s_name = ['UBL', 'HBL', 'PSO', 'ENGRO', 'OGDC']
    print "inside prediction1"
    for x in s_name:
        df = pd.DataFrame(
            pd.read_csv("/home/hduser1/PycharmProjects/spark/indicators code/23-27DATA/hourly/" + x + "_1h.csv"))

        date_list = df.date.tolist()
        price_list = df.price.tolist()
        time_list = df.time.tolist()

        rsi_f = rsi(price_list, date_list, time_list, ' ')
        stoch_f = stoch(price_list, date_list, time_list, ' ')
        stochrsi_f = stoch_rsi(price_list, date_list, time_list, ' ')
        macd_f = macd(price_list, date_list, time_list, ' ')
        mov_5 = moving_avg(price_list, date_list, time_list, 5)

        mov_10 = moving_avg(price_list, date_list, time_list, 10)
        mov_20 = moving_avg(price_list, date_list, time_list, 20)
        emov_5 = exp_mov_avg1(price_list, date_list, time_list, 5)
        emov_10 = exp_mov_avg1(price_list, date_list, time_list, 10)
        emov_20 = exp_mov_avg1(price_list, date_list, time_list, 20)
        william_f = williams(price_list, date_list, time_list, ' ')
        #
        # csv_writer(x + '/hourly/' + x + '_indicators_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'],
        #            [rsi_f, stoch_f, macd_f, stochrsi_f, william_f])
        # csv_writer(x + '/hourly/' + x + '_moving_avg_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'], [mov_5, mov_10, mov_20])
        # csv_writer(x + '/hourly/' + x + '_exp_moving_avg_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'],
        #            [emov_5, emov_10, emov_20])
        #
        # file_upload('/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_exp_moving_avg_hourly.csv', x,
        #             '_exp_moving_avg_hourly')
        # file_upload(
        #     '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_moving_avg_hourly.csv', x, '_moving_avg_hourly')
        # file_upload(
        #     '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_indicators_hourly.csv', x, '_indicators_hourly')

        set_effect1 = settlement_effect(x)

        avgs_effect1 = [mov_5[2], mov_10[2], mov_20[2], emov_5[2], emov_10[2], emov_20[2]]

        prev_close1 = previous_closing_price(x)
        indicators_effect1 = [rsi_f[2], stoch_f[2], macd_f[2], stochrsi_f[2], william_f[2]]
        p = prediction(avgs_effect1, indicators_effect1, set_effect1, prev_close1)

        csv_writer(x + '/prediction/' + x + '_prediction.csv', ['DATE', 'VALUE'], [[date_list[-1], round(p,2)]])
        file_upload('/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/prediction/' + x + '_prediction.csv',
                    x, '_prediction')
def news_effect():
    reload(sys)

    timestmp = str(datetime.now())
    print timestmp

    sys.setdefaultencoding('utf8')
    # nltk.download('words')
    # nltk.download('averaged_perceptron_tagger')
    # nltk.download('wordnet')
    # nltk.download('stopwords')
    # nltk.download('punkt')
    s = sched.scheduler(time.time, time.sleep)
    titles = []
    titles_urls = []
    scores = {}

    def func():
        feed = feedparser.parse(
            "https://news.google.com/news/rss/headlines/section/topic/BUSINESS.en_pk/Business?ned=en_pk&hl=en&gl=PK")
        # feed = feedparser.parse("http://feeds.feedburner.com/com/Yeor")
        wnl = WordNetLemmatizer()
        # feed_title = feed['feed']['title']
        # feed_entries = feed.entries

        for entry in feed.entries:
            article_title = (entry.title).encode("utf-8")
            article_link = (entry.link).encode("utf-8")
            article_published_at = entry.published
            article_title = article_title.lower()
            print article_title, article_link, article_published_at

            #### Title processing
            # title = [wnl.lemmatize(i, j[0].lower()) if j[0].lower() in ['a', 'n', 'v'] else wnl.lemmatize(i) for i, j in
            #  pos_tag(word_tokenize(article_title))]
            #
            # no_unicode = []
            #
            # for items in title:
            #     no_unicode.append(items.encode("utf-8"))
            # for words in title:
            #      no_unicode += " " + words.encode("utf-8")

            # print "Lemmatized"
            # print no_unicode

            if article_title not in titles:
                # titles.append(article_title)
                # urls.append(article_link)
                titles.append(article_title)
                titles_urls.append(tuple([article_title, article_link]))
                extractor = Goose()
                article = extractor.extract(url=article_link)
                text = article.cleaned_text
                text = text.lower()
                stop_words = set(stopwords.words('english'))

                word_tokens = word_tokenize(text)

                filtered_sentence = [w for w in word_tokens if not w in stop_words]

                for words in filtered_sentence:
                    # filtered_sentence = words.encode('ascii', 'ignore')
                    filtered_sentence = words.encode("utf-8")

                filtered_sentence = str(filtered_sentence)

                for w in word_tokens:
                    if w not in stop_words:
                        # filtered_sentence.append(w)
                        filtered_sentence = filtered_sentence.encode("utf-8")
                        filtered_sentence += " " + w

                # words = set(nltk.corpus.words.words())
                filtered_sentence = re.sub(ur"[^\w\d\s]+", '', filtered_sentence)

                print "Non-lemmatized News:"
                print filtered_sentence

                filtered_sentence = [
                    wnl.lemmatize(i, j[0].lower()) if j[0].lower() in ['a', 'n', 'v'] else wnl.lemmatize(i) for i, j in
                    pos_tag(word_tokenize(filtered_sentence))]

                no_unicode = []

                for items in filtered_sentence:
                    no_unicode.append(items.encode("utf-8"))
                # for words in filtered_sentence:
                #      no_unicode += " " + words.encode("utf-8")



                hiv4 = ps.HIV4()
                # words = set(nltk.corpus.words.words())
                # all_clean = filtered_sentence
                all_clean = " ".join(x for x in no_unicode)
                # cleaned = " ".join(w for w in nltk.wordpunct_tokenize(all_clean) \
                #                   if w.lower() in words or not w.isalpha())

                # print all_clean

                general = [" kse ", " psx ", "pakistan stock exchange",
                           "karachi stock exchange", "pakistan stock market"]
                keywords_pso = [" pso ", "pakistan state oil"]
                keywords_engro = [" engro ", "engro fertilizer"]
                keywords_hbl = [" hbl ", "habib bank"]
                keywords_ubl = [" ubl ", "unite bank"]
                keywords_ogdcl = [" ogdc ", " ogdcl ", "oil & gas development company",
                                  "oil and gas development company"]

                flag = 0
                # relevant = []

                for words in general:
                    if words in all_clean:
                        flag = 1
                        # relevant.append(article_title)

                for words in keywords_pso:
                    if words in all_clean:
                        flag = 2
                        # relevant.append(article_title)

                for words in keywords_hbl:
                    if words in all_clean:
                        flag = 3
                        # relevant.append(article_title)

                for words in keywords_ogdcl:
                    if words in all_clean:
                        flag = 4
                        # relevant.append(article_title)

                for words in keywords_ubl:
                    if words in all_clean:
                        flag = 5

                for words in keywords_engro:
                    if words in all_clean:
                        flag = 6
                        # relevant.append(article_title)

                if flag == 1:
                    print "General Stock Market News"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned News Below:"
                    print all_clean
                    print "Score Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 2:
                    print "News related to PSO"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned News Below:"
                    print all_clean
                    print "Score Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 3:
                    print "News related to HBL"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned News Below:"
                    print all_clean
                    print "Score Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 4:
                    print "News related to OGDCL"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned News Below:"
                    print all_clean
                    print "Score Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 5:
                    print "News related to UBL"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned News Below:"
                    print all_clean
                    print "Score Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 6:
                    print "News related to ENGRO"
                    score = hiv4.get_score(all_clean.split())
                    print "Cleaned and Lemmatized News Below:"
                    print all_clean
                    print "Score Dictionary Below:"
                    print score

                    scores[article_title] = score['Polarity']

                if flag == 0:
                    print "News was irrelevant!"

                print titles
                print scores
                print "\n"

            else:
                print "Already Processed!\n"

        print "Exiting...\n"

    # func()

    # print "Re-running....\n"
    schedule.every(1).minutes.do(func)

    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    s_name = ['UBL', 'HBL','PSO', 'ENGRO', 'OGDC']


    # for x in s_name:
    #
    #     df = pd.DataFrame(pd.read_csv("/home/hduser1/PycharmProjects/spark/indicators code/23-27DATA/hourly/"+x+"_1h.csv"))
    #
    #     date_list = df.date.tolist()
    #     price_list = df.price.tolist()
    #     time_list = df.time.tolist()
    #
    #     rsi_f=rsi(price_list,date_list,time_list,' ')
    #     stoch_f=stoch(price_list,date_list,time_list,' ')
    #     stochrsi_f=stoch_rsi(price_list,date_list,time_list,' ')
    #     macd_f=macd(price_list,date_list,time_list,' ')
    #     mov_5=moving_avg(price_list,date_list,time_list,5)
    #
    #
    #     mov_10=moving_avg(price_list, date_list, time_list, 10)
    #     mov_20=moving_avg(price_list, date_list, time_list, 20)
    #     emov_5=exp_mov_avg1(price_list, date_list, time_list, 5)
    #     emov_10=exp_mov_avg1(price_list, date_list, time_list, 10)
    #     emov_20=exp_mov_avg1(price_list, date_list, time_list, 20)
    #     william_f = williams(price_list, date_list, time_list, ' ')
    prediction1()
        # csv_writer(x + '/hourly/' + x + '_indicators_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'],
        #            [rsi_f, stoch_f, macd_f, stochrsi_f, william_f])
        # csv_writer(x + '/hourly/' + x + '_moving_avg_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'], [mov_5, mov_10, mov_20])
        # csv_writer(x + '/hourly/' + x + '_exp_moving_avg_hourly.csv', ['INDICATOR', 'VALUE', 'EFFECT'],
        #            [emov_5, emov_10, emov_20])
        #
        # file_upload('/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_exp_moving_avg_hourly.csv', x,
        #             '_exp_moving_avg_hourly')
        # file_upload(
        #     '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_moving_avg_hourly.csv', x, '_moving_avg_hourly')
        # file_upload(
        #     '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/hourly/' + x + '_indicators_hourly.csv', x, '_indicators_hourly')
        #



#
# schedule.every(60).minutes.do(main)
#
# schedule.every().day.at("11:32").do(prediction1)

# while True:
#     schedule.run_pending()
if __name__==main():
    main()



