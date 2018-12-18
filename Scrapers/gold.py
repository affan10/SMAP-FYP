from BeautifulSoup import BeautifulSoup
import urllib2
#import sched, time
from datetime import datetime

NoneType = type(None)
#myfile = open("Gold.csv", "a")
date = str(datetime.now())
date = date.split()[0]

#myfile = open("Gold " + date + ".csv", "a")
myfile = open("Gold 2018-04-06.csv", "a")
#s = sched.scheduler(time.time, time.sleep)

#print "Oil Price In $/barrel:"

def bring_index():
    timestmp = str(datetime.now())
    hours = timestmp.split(" ")
    #hours1 = hours[1][:2]
    # print hours
    # print hours1
    d1 = datetime.strptime(timestmp, "%Y-%m-%d %H:%M:%S.%f")
    d2 = datetime.strptime(hours[0] + " 00:15:00.000", "%Y-%m-%d %H:%M:%S.%f")

    corrected_time = d1 - d2
    corrected_time = str(corrected_time)
    print corrected_time
    url = "https://www.investing.com/commodities/gold"
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    data = opener.open(url).read()

    soup = BeautifulSoup(data)

    #t=soup.find('span',{'class':'arial_26 inlineblock pid-8849-last'})
    #t1 = soup.find('span', {'class': 'arial_20 greenFont   pid-8849-pc'})

    t = soup.find('span', {'class': 'arial_26 inlineblock pid-8830-last'})
    #t1 = soup.find('span', {'class': 'arial_20 pid-8849-pc'})

    if type(t) != NoneType:
        print t.text
        myfile.write(corrected_time + "\n")
        myfile.write(t.text + "\n")
        # global value
        # if t.text != value:
        #     value = t.text
        #     myfile.write(corrected_time + "\n")
        #     myfile.write(t.text + "\n")
        #     print "added"
        # else:
        #     print "Not added"
    else:
        bring_index()


while (1):
    bring_index()

#s.enter(0, 1, bring_index, (s,))
#s.run()