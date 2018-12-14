import pandas as pd
from datetime import datetime
import csv
from statistics import mean
def csv_writer(path,header,data):  #this function is to write the moving avg value vs date values to csv files
    with open(path, "wb") as out_file:
        writer = csv.writer(out_file, delimiter=',')
        writer.writerow(header)
        for x in data:
            writer.writerow(x)
import ftplib



def moving_avg(close_list1,date_list1,period):
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



        i+=1
        j+=1
    if moving_close_list[-1]<close_list1[-1]:
        effect='BUY'
    else:
        effect='SELL    '
    return['MA'+str(period),moving_close_list[-1],effect]
def exp_mov_avg1(close_list1,date_list1,period):
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


        EMA = (close_list1[x] - EMA) * multiplier + EMA

    # print close_list
    # print date_list
    #
    # print exp_date_list
    # print exp_close_li
    if exp_close_list[-1]<close_list1[-1]:
        effect='BUY'
    else:
        effect='SELL'

    return ['EMA' + str(period), exp_close_list[-1],effect]


def rsi(close_list1,date_list1,sname):
    gain = []
    loss = []
    rsi_list = []
    rsi_date_list = []
    list_effect=[]
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


    for x in range(14, len(close_list1)):
        if  RSI>=70:
            list_effect.append('BUY')
        elif RSI<=30:
            list_effect.append('SELL')
        else:
            list_effect.append('NEUTRAL')
        rsi_list.append(round(RSI,2))
        rsi_date_list.append(date_list1[x])
        change = close_list1[x] - close_list1[x - 1]
        if change > 0:
            avg_gain = (avg_gain * 13 + change) / 14
            avg_loss = (avg_loss * 13 + 0) / 14
        elif change < 0:
            avg_gain = (avg_gain * 13 + 0) / 14
            avg_loss = (avg_loss * 13 + abs(change)) / 14
        RS = avg_gain / avg_loss
        RSI = 100 - (100 / (1 + RS))
    return [[rsi_list,rsi_date_list],['RSI',rsi_list[-1],list_effect[-1]]]
def stoch(close_list1,date_list1):
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
            stoch_list.append(abs(round(stoch, 2)))
            if stoch<=45:
                stoch_effect_list.append('SELL')
            elif stoch>=55:
                stoch_effect_list.append('BUY')

            else:
                stoch_effect_list.append('NEUTRAL')


        stoch_date_list.append(date_list1[x])

        i += 1
        j += 1
    return ['STOCH',stoch_list[-1],stoch_effect_list[-1]]
def macd(close_list1,date_list1,sname):
    close_list1_12=close_list1[14:len(close_list1)]

    ema_12,m=exp_mov_avg(close_list1_12,date_list1,12,sname)

    ema_26,date_list_macd=exp_mov_avg(close_list1,date_list1,26,sname)

    effect_list=[]


    macdline=[a-b for a,b in zip(ema_12,ema_26)]
    macdline=[round(x,2) for x in macdline]


    for y in range(0,len(macdline)):
        if macdline[y]<0:
            effect_list.append('SELL')
        elif macdline[y]>0:
            effect_list.append('BUY')
        else:
            effect_list.append('NEUTRAL')

    return ['MACD',macdline[-1],effect_list[-1]]
def exp_mov_avg(close_list1,date_list1,period,sname):
    exp_close_list = []
    exp_date_list = []


    EMA = sum(close_list1[0:period]) / period
    # taking timeperiod =10
    multiplier = float(2) / (period + 1)

    for x in range(period, len(close_list1)):
        exp_close_list.append(EMA)
        exp_date_list.append(date_list1[x - 1])


        EMA = (close_list1[x] - EMA) * multiplier + EMA

    return exp_close_list,exp_date_list
def stoch_rsi(close_list1, date_list1, sname):
    i = 0
    j = 14
    stoch_date_list = []
    stoch_time_list = []
    stoch_list = []
    stoch_effect_list = []

    for x in range(13, len(close_list1)):
        lowest_low = min(close_list1[i:j])
        highest_high = max(close_list1[i:j])
        if (highest_high - lowest_low == 0):
            i += 1
            j += 1
            continue
        else:
            stoch = (close_list1[x] - lowest_low) / (highest_high - lowest_low) * 100
            stoch_list.append(abs(round(stoch, 2)))
            if stoch <= 45:
                stoch_effect_list.append('SELL')
            elif stoch >= 55:
                stoch_effect_list.append('BUY')

            else:
                stoch_effect_list.append('NEUTRAL')

        stoch_date_list.append(date_list1[x])

        i += 1
        j += 1
    return ['STOCHRSI', stoch_list[-1], stoch_effect_list[-1]]
def williams(close_list,date_list,high_list,low_list,sname):
    i = 0
    j = 14
    william_date_list = []
    william_price_list = []
    william_effect_list=[]
    for x in range(13, len(close_list)):
        hh = max(high_list[i:j])
        ll = min(low_list[i:j])
        if hh-ll==0:
            i+=1
            j+=1
            continue
        else:
            r = ((hh - close_list[x]) / (hh - ll)) * (-100)
            william_price_list.append(round(r,2))
            if -20<=r<=0 :
                william_effect_list.append('BUY')
            else:
                william_effect_list.append('SELL')

        i += 1
        j += 1
        william_date_list.append(date_list[x])

    return ['WILLIAM %R',william_price_list[-1],william_effect_list[-1]]
def cci(close_list1,date_list1,high_list1,low_list1,sname):
    tp_price_list = []
    cci_effect_list=[]
    for x in range(0, len(close_list1)):
        tp_price_list.append(round((close_list1[x] + high_list1[x] + low_list1[x]) / 3,2))
    i = 0
    j = 20

    cci_list = []
    cci_date_list = []
    for x in range(19, len(tp_price_list)):
        m = mean(tp_price_list[i:j])

        md = mean([abs(m - y) for y in tp_price_list[i:j]])
        if 0.15*md==0:
            i+=1
            j+=1
            continue

        cci = (tp_price_list[x] - mean(tp_price_list[i:j])) / (0.15 * md)
        cci_list.append(round(cci,2))
        cci_date_list.append(date_list1[x])
        if cci<0:
            cci_effect_list.append('SELL')
        elif cci>0:
            cci_effect_list.append('BUY')
        i+=1
        j+=1

    return ['CCI',cci_list[-1],cci_effect_list[-1]]

def roc(close_list1,date_list1):
    roc_date_list = []
    roc_list = []
    roc_effect_list=[]
    for x in range(12, len(close_list1)):
        close_n_ago = close_list1[x - 12]
        roc = (close_list1[x] - close_n_ago) / close_n_ago * 100
        roc_date_list.append(date_list1[x])
        roc_list.append(round(roc,2))
        if roc<0:
            roc_effect_list.append('SELL')
        elif roc>0:
            roc_effect_list.append('BUY')

    return ['ROC',roc_list[-1],roc_effect_list[-1]]
def uo(close_list1,date_list1,high_list1,low_list1,sname):
    bp_list = []
    tr_list = []
    avg7_date_list=[]
    avg14_date_list=[]
    avg28_date_list=[]
    c1=0
    c2=0
    c3=0
    for x in range(1, len(close_list1)):
        bp_list.append(close_list1[x] - min(low_list1[x], close_list1[x - 1]))
        tr_list.append( max(high_list1[x], close_list1[x - 1]) - min(low_list1[x], close_list1[x - 1]))
    i = 0
    j = 7

    avg7_list = []




    for x in range(7, len(bp_list)):
        if sum(tr_list[i:j])==0:
            i+=1
            j+=1
            c1+=1
            continue
        else:
            avg7_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))

        avg7_date_list.append(date_list1[x])
        i += 1
        j += 1



    i = 0
    j = 14
    avg14_list = []

    for x in range(14, len(bp_list)):

        if sum(tr_list[i:j])==0:
            i+=1
            j+=1
            c2+=1

            continue
        else:
            avg14_date_list.append(date_list1[x])
            avg14_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))
        i += 1
        j += 1

    i = 0
    j = 28
    avg28_list = []

    for x in range(28, len(bp_list)):
        if sum(tr_list[i:j])==0:
            i+=1
            j+=1
            c3+=1
            continue
        else:
            avg28_list.append(sum(bp_list[i:j]) / sum(tr_list[i:j]))
        avg28_date_list.append(date_list1[x])
        i += 1
        j += 1


    i=0
    j=0
    k=0
    uo_list=[]
    uo_effect_list=[]

    avg7_list=avg7_list[0:3092]
    avg14_list=avg14_list[0:3092]

    for x in range(0,len(avg28_list)):
        uol=100*((4 * avg7_list[i])+(2 * avg14_list[j])+ avg28_list[k])/7
        uo_list.append(uol)
        if uol<50:
            uo_effect_list.append('sell')
        elif uol>50:
            uo_effect_list.append('buy')

        i+=1
        j+=1
        k+=1


    return ['uo',uo_list[-1],uo_effect_list[-1]]
def file_upload(lpath,x,y):
    session = ftplib.FTP('ftp.mystocks.pk', 'mystocks', 'wnyc(%C7o,b_')
    file = open(lpath, 'rb')
    session.cwd("/public_html/data/" + x)
    session.storbinary('STOR ' + x +y+ '.csv', file)  # send the file
    file.close()  # close file and FTP
    session.quit()
    print "Done"





s_name=['UBL','PSO','HBL','ENGRO','OGDC']
for x in s_name:
    df = pd.DataFrame(pd.read_csv("/home/hduser1/PycharmProjects/spark/data /"+x+"_01012003_09042018.csv"))

    date_list = df.Date.tolist()
    close_price_list = df.Close.tolist()

    high_list=df.High.tolist()
    low_list=df.Low.tolist()
    result_rsi = rsi(close_price_list, date_list, '')

    rsi_list1 = result_rsi[0]
    rsi_f = result_rsi[1]
    stoch_f = stoch(close_price_list, date_list)
    macd_f = macd(close_price_list, date_list, ' ')
    stochrsi_f = stoch_rsi(rsi_list1[0], rsi_list1[1], ' ')
    william_f = williams(close_price_list, date_list, high_list, low_list, ' ')
    roc_f = roc(close_price_list, date_list)
    cci_f = cci(close_price_list, date_list, high_list, low_list, ' ')
    # uo_f = uo(close_price_list, date_list, high_list, low_list, ' ')

    mov_5=moving_avg(close_price_list,date_list,5)
    mov_10 = moving_avg(close_price_list, date_list, 10)
    mov_20 = moving_avg(close_price_list, date_list, 20)
    mov_30 = moving_avg(close_price_list, date_list, 30)
    emov_5 = exp_mov_avg1(close_price_list, date_list, 5)
    emov_10 = exp_mov_avg1(close_price_list, date_list, 10)
    emov_20 = exp_mov_avg1(close_price_list, date_list, 20)
    emov_30 = exp_mov_avg1(close_price_list, date_list, 30)
    csv_writer(x + '/live/' + x + '_daily.csv', ['INDICATOR', 'VALUE', 'EFFECT'], [rsi_f, stoch_f,macd_f,stochrsi_f,william_f,roc_f,cci_f])
    csv_writer(x + '/live/' + x + '_moving_avg.csv', ['INDICATOR', 'VALUE', 'EFFECT'], [mov_5, mov_10, mov_20, mov_30])
    csv_writer(x + '/live/' + x + '_exp_moving_avg.csv', ['INDICATOR', 'VALUE', 'EFFECT'],
               [emov_5, emov_10, emov_20, emov_30])

    file_upload('/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/live/' + x + '_exp_moving_avg.csv', x,
                '_exp_moving_avg')
    file_upload(
        '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/live/' + x + '_moving_avg.csv', x, '_moving_avg')
    file_upload(
        '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/live/' + x + '_daily.csv', x, '_daily')
# for x in s_name:
#     file_upload('/home/hduser1/PycharmProjects/spark/indicators code/'+x+'/live/'+x+'_exp_moving_avg.csv',x,'_exp_moving_avg')
#     file_upload(
#         '/home/hduser1/PycharmProjects/spark/indicators code/' + x + '/live/' +x+ '_moving_avg.csv',x,'_moving_avg')
#
#
#     csv_writer(x+'/live/'+x+'_moving_avg.csv',['INDICATOR','VALUE','EFFECT'],[mov_5,mov_10,mov_20,mov_30])
#     csv_writer(x+'/live/'+x+ '_exp_moving_avg.csv', ['INDICATOR', 'VALUE', 'EFFECT'], [emov_5, emov_10, emov_20, emov_30])



# final_effect=[rsi_f[2],stoch_f[2],macd_f[2],stochrsi_f[2],william_f[2],roc_f[2],cci_f[2],uo_f[2]]
# effect=''
# sell_count=final_effect.count('sell')
# buy_count=final_effect.count('buy')
# if sell_count>5:
#     effect='strong sell'
# elif sell_count==5:
#     effect='sell'
#
# elif buy_count>5:
#     effect='strong buy'
# elif buy_count==50:
#     effect='buy'
# else:
#     effect='neutral'
# current_close=close_price_list[-1]
# if effect=='strong sell':
#     predicted_value=current_close-current_close*.03
# elif effect=='sell':
#     predicted_value=current_close-current_close*.01
# elif effect=='strong buy':
#     predicted_value=current_close+current_close*.03
# elif effect=='buy':
#     predicted_value=current_close+current_close*.01
# else:
#     predicted_value=current_close
# print 'yesterday close',current_close
# print 'today predicted value',predicted_value
#
#
#
#
#
#
#
#
#
#
#
