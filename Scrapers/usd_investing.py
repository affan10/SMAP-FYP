from BeautifulSoup import BeautifulSoup
import urllib2
import sched, time
from datetime import datetime

myfile = open("usd index", "a")

#with open("test.txt", "a") as myfile:
#    myfile.write("appended text")

timestmp = str(datetime.now())
#timestmp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
print timestmp
myfile.write(timestmp + "\n")

s = sched.scheduler(time.time, time.sleep)

print "$ Index:"

def bring_index(sc):
    #pkg = "com.mavdev.focusoutfacebook"
    url = "https://www.investing.com/quotes/us-dollar-index"
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    data = opener.open(url).read()

    soup=BeautifulSoup(data)

    t=soup.find('span',{'class':'arial_26 inlineblock pid-8827-last'})
    #t1 = soup.find('span', {'class': 'arial_20 redFont   pid-8827-pc'})

    print t.text
    myfile.write(t.text + "\n")
    s.enter(1, 1, bring_index, (sc,))

s.enter(1, 1, bring_index, (s,))
s.run()