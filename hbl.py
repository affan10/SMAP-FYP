from BeautifulSoup import BeautifulSoup
import urllib2
import sched, time
from datetime import datetime
import ftplib

date = str(datetime.now())
date = date.split()[0]
myfile = open("HBL " + date + ".csv", "a")
myfile = open("HBL.csv", "a")
NoneType = type(None)
s = sched.scheduler(time.time, time.sleep)

#print "Oil Price In $/barrel:"
def bring_index():
    timestmp = str(datetime.now())
    hours = timestmp.split(" ")
    d1 = datetime.strptime(timestmp, "%Y-%m-%d %H:%M:%S.%f")
    d2 = datetime.strptime(hours[0] + " 00:15:00.000", "%Y-%m-%d %H:%M:%S.%f")
    print timestmp[0]

    corrected_time = d1 - d2
    corrected_time = str(corrected_time)
    print corrected_time
    url = "https://www.investing.com/equities/habib-bank-ltd"
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    data = opener.open(url).read()

    soup = BeautifulSoup(data)

    t = soup.find('span', {'class': 'arial_26 inlineblock pid-104183-last'})

    if type(t) != NoneType:
        print t.text
        myfile.write(corrected_time + "\n")
        myfile.write(t.text + "\n")
        session = ftplib.FTP('ftp.mystocks.pk', 'mystocks', 'wnyc(%C7o,b_')
        file = open('HBL 2018-04-09.csv', 'ab+')
        session.cwd("/public_html")
        session.storbinary('STOR HBL 2018-04-09.csv', file)  # send the file
        file.close()  # close file and FTP
        session.quit()
        print "Done"
        global value
        if t.text != value:
            value = t.text
            myfile.write(corrected_time + "\n")
            myfile.write(t.text + "\n")
            print "added"
        else:
    print "Not added"

if __name__ == "__main__":
    while (1):
        bring_index()

#s.enter(0, 1, bring_index, (s,))
# s.run()
