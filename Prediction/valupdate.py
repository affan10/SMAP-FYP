from datetime import datetime, timedelta
import schedule
import csv
import os
import glob
import memcache
import time
import ftplib

#flag = 0

# outcsv = open("out/UBL_act_pred.csv", 'w')
# writer = csv.writer(outcsv)
# writer.writerow(["time", "actual", "predicted"])
# outcsv.close()
# outcsv1 = open("out/HBL_act_pred.csv", 'w')
# writer = csv.writer(outcsv1)
# writer.writerow(["time", "actual", "predicted"])
# outcsv1.close()
# outcsv2 = open("out/ENGRO_act_pred.csv", 'w')
# writer = csv.writer(outcsv2)
# writer.writerow(["ti+me", "actual", "predicted"])
# outcsv2.close()
# outcsv3 = open("out/PSO_act_pred.csv", 'w')
# writer = csv.writer(outcsv3)
# writer.writerow(["time", "actual", "predicted"])
# outcsv3.close()

counter = 0

def outer():

    newtime = str(datetime.now())
    newtime = newtime.split()
    newtime = newtime[1]
    #print newtime
    time2 = newtime[:-10]

    path = '/home/hduser1/Desktop/ml'
    extension = 'csv'
    os.chdir(path)
    result = [i for i in glob.glob('*.{}'.format(extension))]
    print result

    shared = memcache.Client(['127.0.0.1:11211'], debug=0)
    counter = shared.get('Value')

    while(counter != str(len(result))):
        counter = shared.get('Value')

    time.sleep(5)
    # file_name = "counter.txt"
    # path = '/home/hduser1/Desktop/ml/out'
    # extension = '.txt'
    # os.chdir(path)
    # result = [i for i in glob.glob('*.{}'.format(extension))]
    # print result
    #
    # while(len(result) != 1):
    #     path = '/home/hduser1/Desktop/ml/out'
    #     extension = '.txt'
    #     os.chdir(path)
    #     result = [i for i in glob.glob('*.{}'.format(extension))]

    # count_file = open("out/counter.txt","r")
    # line = count_file.readlines()
    # print line
    #
    # while(line[0] != str(len(result))):
    #     count_file = open("out/counter.txt", "r")
    #     line = count_file.readlines()

    session = ftplib.FTP('ftp.mystocks.pk', 'mystocks', 'wnyc(%C7o,b_')
    for items in result:
        names = items.split(".")

        #read csv, and split on "," the line
        csv_file = csv.reader(open(names[0] + ".csv", "rb"), delimiter=",")
        print time2
        val = ""
        #loop through csv list
        for row in csv_file:
            if time2 in row[1]:
                list = row[1].split(":")
                temp = time2.split(":")
                #print temp
                if temp[0] == list[0]:
                #print list
                    val = row
                    print val
                    #print val

        print val
        # pred_csv = open("out/ubl_singval.csv","r")
        # value = pred_csv.readlines()
        # print value

        file = open("out/" + names[0] + "_predvalonly.txt", "r")
        lines = file.readlines()
        #print lines
        print lines[0]

        # with open("out/" + names[0] + "_act_pred.csv", 'a') as outcsv:
        #     writer = csv.writer(outcsv)
        #     writer.writerow(["time", "actual", "predicted"])

        with open("out/" + names[0] + "_act_pred.csv", 'a') as outcsv:
            writer = csv.writer(outcsv)
            #writer.writerow(["time", "actual", "predicted"])
            writer.writerow([time2, val[2], lines[0]])

        time.sleep(5)

        # file = open("out/" + names[0] +"_act_pred.csv", 'rb')
        # session.cwd("/public_html/data/" + names[0])
        # session.storbinary('STOR ' + names[0] + "_act_pred.csv" , file)

        # file2 = open("out/" + names[0] + "_pred.csv", 'rb')
        # session.cwd("/public_html/data/" + names[0])
        # session.storbinary('STOR ' + names[0] + "_pred.csv", file2)

        file.close()
        #file2.close()# send the file
        print "Done"

  # close file and FTP
    session.quit()
    #counter += 1
    print "done"
    #print counter

schedule.every(1).minutes.do(outer)
#outer()
while True:
    schedule.run_pending()

#
# # data = [{'time': time, 'actual': val[2], 'predicted':"NULL"}]
# #
# # with open('out/ubl2_predict.json', 'w') as outfile:
# #     json.dump(data, outfile)
#