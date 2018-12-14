from pyspark import SparkContext
from pyspark.sql import SQLContext
import numpy as np
import csv
from pandas import *
from statistics import mean
sc=SparkContext()
sqlcontext=SQLContext(sc)


def csv_writer(path, fieldnames, data):  #this function is to write the moving avg value vs date values to csv files
    with open(path, "wb") as out_file:
        writer = csv.writer(out_file, delimiter=',')
        writer.writerow(fieldnames)
        for x in range(0,len(data[0])):
            writer.writerow([data[0][x],data[1][x]])





#indicators Function starts here

#function to calculate moving avg
def moving_avg(close_list,date_list,period,sname):
    moving_close_list=[] #list of moving avg
    moving_date_list=[]#list of dates corresponding to moving avg
    # cs= chuck size no of days for moving average

    i=0
    j=period
    #here moving avg is calculated and sotred in list
    for x in range(0,len(close_list)-period):
        moving_close_list.append(sum(close_list[i:j])/period)
        moving_date_list.append(date_list[j])
        i+=1
        j+=1

    data=[moving_date_list,moving_close_list]
    fieldnames=['Date','Price']
    csv_writer(sname+'/'+sname+"_MA"+str(period)+".csv",fieldnames,data)#function calls and a csv file name output is generated
def exp_mov_avg(close_list,date_list,period,sname):
    exp_close_list = []
    exp_date_list = []
    # cs= chuck size no of days for moving average

    EMA = sum(close_list[0:period]) / period
    print "staring SMA", EMA
    # taking timeperiod =10
    multiplier = float(2) / (period + 1)
    print "multiplier", multiplier
    for x in range(period, len(close_list)):
        exp_close_list.append(EMA)
        exp_date_list.append(date_list[x - 1])
        EMA = (close_list[x] - EMA) * multiplier + EMA

    # print close_list
    # print date_list
    #
    # print exp_date_list
    # print exp_close_list

    data = [exp_date_list, exp_close_list]
    fieldnames = ['Date', 'Exp Moving avg']
    csv_writer(sname+'/'+sname+"_EMA"+str(period)+".csv", fieldnames, data)


def stoch(close_list,date_list,sname):
    i = 0
    j = 14
    stoch_date_list=[]
    stoch_list=[]
    for x in range(13, len(close_list)):
        lowest_low = min(close_list[i:j])
        highest_high = max(close_list[i:j])
        if (highest_high - lowest_low == 0):
            stoch_list.append('*')
        else:
            stoch = (close_list[x] - lowest_low) / (highest_high - lowest_low) * 100
            stoch_list.append(abs(round(stoch, 3)))
        stoch_date_list.append(date_list[x])
        i += 1
        j += 1

    data = [stoch_date_list, stoch_list]
    fieldnames = ['Date', 'Stoch']
    csv_writer(sname+'/'+sname+"_STOCH.csv", fieldnames, data)
    
    
def bollinger_bands(close_list,date_list):
    
    moving_price_list = []  # list of moving avg
    # cs= chuck size no of days for moving average
    cs = 10
    i = 0
    j = 20
    upper_band = []
    lower_band = []
    # here moving avg is calculated and sotred in list
    for x in range(0, len(close_list) - cs):
        mav = sum(close_list[i:j]) / cs
        moving_price_list.append(mav)
        a = np.array(close_list[i:j])

        std = np.std(a, axis=0)
        upper_band.append(mav + (std * 2))
        lower_band.append(mav + (std * 2))

        i += 1
        j += 1
    time_list1 = []
    for x in range(20, len(close_list)):
        time_list1.append(x)
    return [upper_band,lower_band]
def rsi(close_list,date_list,sname):
    gain = []
    loss = []
    rsi_list = []
    rsi_date_list = []
    list_effect=[]
    for x in range(0, 13):
        change = close_list[x + 1] - close_list[x]
        if change > 0:
            gain.append(change)
        elif change < 0:
            loss.append(abs(change))

    avg_gain = sum(gain) / 14
    avg_loss = sum(loss) / 14
    RS = avg_gain / avg_loss
    RSI = 100 - (100 / (1 + RS))

    for x in range(14, len(close_list)):
        if RSI>=70:
            list_effect.append('buy')
        elif RSI<=30:
            list_effect.append('sell')
        else:
            list_effect.append('neutral')
        rsi_list.append(round(RSI,3))
        rsi_date_list.append(date_list[x])
        change = close_list[x] - close_list[x - 1]
        if change > 0:
            avg_gain = (avg_gain * 13 + change) / 14
            avg_loss = (avg_loss * 13 + 0) / 14
        elif change < 0:
            avg_gain = (avg_gain * 13 + 0) / 14
            avg_loss = (avg_loss * 13 + abs(change)) / 14
        RS = avg_gain / avg_loss
        RSI = 100 - (100 / (1 + RS))
    print "code is running"
    data = [rsi_date_list, rsi_list,list_effect]
    fieldnames = ['Date', 'RSI','effect']
    csv_writer(sname+'25_RSI.csv', fieldnames, data)

def williams(close_list,date_list,high_list,low_list,sname):
    i = 0
    j = 14
    william_date_list = []
    william_price_list = []
    for x in range(13, len(close_list)):
        hh = max(high_list[i:j])
        ll = min(low_list[i:j])
        if hh-ll==0:
            william_price_list.append('*')
        else:
            r = ((hh - close_list[x]) / (hh - ll)) * (-100)
            william_price_list.append(round(r,3))
        i += 1
        j += 1
        william_date_list.append(date_list[x])

    data = [william_date_list,william_price_list]
    print data
    fieldnames = ['Date', 'William_price']
    csv_writer(sname+'/'+sname+'_william%R.csv', fieldnames, data)

def cci(date_list,close_list,high_list,low_list,sname):
    tp_price_list = []
    for x in range(0, len(close_list)):
        tp_price_list.append(round((close_list[x] + high_list[x] + low_list[x]) / 3,2))
    i = 0
    j = 20
    print tp_price_list
    cci_list = []
    cci_date_list = []
    for x in range(19, len(tp_price_list)):
        m = mean(tp_price_list[i:j])

        md = mean([abs(m - y) for y in tp_price_list[i:j]])
        cci = (tp_price_list[x] - mean(tp_price_list[i:j])) / (0.15 * md)
        cci_list.append(round(cci,2))
        cci_date_list.append(date_list[x])
        i+=1
        j+=1
    data = [cci_date_list, cci_list]
    fieldnames = ['Date', 'cci_price']
    csv_writer(sname+'/'+sname+'_CCI_william.csv', fieldnames, data)
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


def main():
    # stock_list = [ 'EFERT', 'UBL', 'OGDC', 'PSO']
    # avgs=[5,10,20,50,100,200]
    # for y in stock_list:

    y='UBL'
    df = sqlcontext.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load(
        '/home/hduser1/PycharmProjects/spark/data /'+y+'_01012003_09042018.csv')
    df_list = df.select('Close', 'Date', 'High', 'Low').collect()

    date_list1 = []  # list of dates
    close_list1 = []  # list of closing values
    high_list1 = []
    low_list1 = []

    for x in df_list:
        close_list1.append(x.Close)
        date_list1.append(x.Date)
        high_list1.append(x.High)
        low_list1.append(x.Low)
        # for z in avgs:
        #     exp_mov_avg(close_list1,date_list1,z,y)
        #     moving_avg(close_list1,date_list1,z,y)
        # rsi(close_list1,date_list1,y)
        # stoch(close_list1,date_list1,y)
        # williams(close_list1,date_list1,high_list1,low_list1,y)
        # cci(date_list1,close_list1,high_list1,close_list1,y)
    moving_avg(close_list1,date_list1,5,'UBL')

if __name__==main():
    main()






