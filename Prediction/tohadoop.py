import pydoop.hdfs as hdfs
import pydoop
from datetime import datetime

print datetime.now().time()

pydoop.hdfs.hdfs(host='default', port=0, user=None, groups=None)

hdfs.mkdir('NEWS ARTICLES')
# hdfs.put('/home/hduser1/PycharmProjects/Crawler/NEWS.csv', 'NEWS ARTICLES/NEWS.csv')

var = hdfs.mkdir('ALL UBL DATA')
print var
hdfs.mkdir('ALL HBL DATA')
hdfs.mkdir('ALL OGDCL DATA')
hdfs.mkdir('ALL ENGRO DATA')
hdfs.mkdir('ALL PSO DATA')
hdfs.mkdir('MISC')

hdfs.put('/home/hduser1/PycharmProjects/Crawler/Unwanted Stuff', 'MISC/Unwanted')
hdfs.put('/home/hduser1/PycharmProjects/Crawler/kse 100', 'HISTORICAL/')

hdfs.hdfs.delete("/user/hduser1/HISTORICAL", recursive=True)
