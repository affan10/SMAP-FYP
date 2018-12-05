import urllib2
import re
import mysql.connector
import PyPDF2
import mysql
from goose import Goose
import validators
from bs4 import BeautifulSoup
import requests
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
import schedule
import time
import sys


opt = 'a'
reload(sys)
sys.setdefaultencoding('utf8')
#nltk.download('stopwords')
#nltk.download('punkt')

def spider():
    #url = raw_input("Enter a website to crawl articles from: ")
    print "Crawling from DAWN.com"
    #nltk.download('stopwords')

    r = requests.get("https://dawn.com")

    data = r.text

    soup = BeautifulSoup(data, "lxml")
    dict = {}
    counter = 0

    for link in soup.find_all('a'):
        # print(link.get('href'))
        dict[counter] = link.get('href')
        counter += 1

    print dict

    print "URLs DICTIONARY"
    # print urls_dict
    print "\n\nGoose Beginning from here \n"

    dict_of_validated_urls = {}

    for key, value in dict.iteritems():
        # print dict[key]
        check = validators.url(dict[key])
        # print check
        if check:
            dict_of_validated_urls[key] = value

    ####### Pass URL of article here ##########

    print dict_of_validated_urls
    print len(dict_of_validated_urls)

    keywords = { 0: "twitter", 1: "facebook", 2: "fashion", 3: "entertainment",
                4: "epaper", 5: "sport", 6: "politics", 7: "images", 8: "obituary", 9: "watch-live", 10: "herald",
                11: "supplements",
                12: "classifieds", 13: "aurora", 14: "cityfm", 15: "#comments", 16: "expo", 17: "nnews",
                18: "latest-news", 19: "category", 20: "videos", 21: "tv-shows", 22: "urdu", 23: "live", 24: "php",
                25: "trending", 26: "privacy", 27: "about", 28: "aspx", 29: "faq", 30: "talent", 31: "ratecardon",
                32:"advertise" }

    print "Validation\n"

    for key in dict_of_validated_urls.keys():
        for values in keywords.values():
            if values in dict_of_validated_urls[key]:
                print dict_of_validated_urls[key]
                del dict_of_validated_urls[key]
                break

    print len(dict_of_validated_urls)

    print dict_of_validated_urls

    dict_of_articles = {}
    counter = 0

    for key, value in dict_of_validated_urls.iteritems():
        dict_of_articles[counter] = value
        counter += 1

    counter = 0
    print dict_of_articles

    dict_of_cleaned_urls = {}

    for key, value in dict_of_articles.items():
        if value not in dict_of_cleaned_urls.values():
            dict_of_cleaned_urls[key] = value

    print "Clean URLs:"
    print dict_of_cleaned_urls
    text = ""
    filtered_sentence = []

    dict_of_cleaned_articles_and_titles = {}

    cnx = mysql.connector.connect(user='root', password='root', host='localhost', database='articles')
    cursor = cnx.cursor()

    for key in dict_of_cleaned_urls.keys():
        url = dict_of_cleaned_urls[key]
        g = Goose()
        article = g.extract(url=url)
        print article.title
        # dict_of_cleaned_articles_and_titles[article.title]
        print "Title printed"
        print "\n"
        # print article.meta_description
        text = article.cleaned_text
        stop_words = set(stopwords.words('english'))

        word_tokens = word_tokenize(text)

        filtered_sentence = [w for w in word_tokens if not w in stop_words]

        for words in filtered_sentence:
            #filtered_sentence = words.encode('ascii', 'ignore')
            filtered_sentence = words.encode("utf-8")

        print filtered_sentence
        #print type(filtered_sentence)
        filtered_sentence = str(filtered_sentence)

        for w in word_tokens:
            if w not in stop_words:
                # filtered_sentence.append(w)
                filtered_sentence += " " + w

        print "Filtered:"
        print filtered_sentence
        print "Text printed"
        # print article.top_image.src

        # dict_of_cleaned_articles_and_titles[article.title] = filtered_sentence



        data = (article.title, filtered_sentence)

        # data = (title, file_text)

        cursor.execute("SELECT Title, COUNT(*) FROM articles_table WHERE Title = %s GROUP BY Title",
                       (article.title,))
        # query =
        msg = cursor.fetchone()
        # check if it is empty and print error
        if not msg:
            cursor.execute("insert into articles_table (Title, Text) values(%s,%s)", (data))
            # cursor.execute(add_to_db_query, data)
            cnx.commit()
            print "Added to Database"

    id = "[]"
    delstatmt = "DELETE FROM articles_table WHERE Text = %s"
    cursor.execute(delstatmt, (id,))
    cnx.commit()

    cursor.close()
    cnx.close()
    print "Done Crawling and Cleaned Database!\n"
    #option = raw_input("\nPress q to quit or any other to restart program: ")
    #print "\n"
    #if option == 'q':
    #    exit()



#spider()

#def job(t):
#    print "I'm working...", t
#    return

schedule.every().day.at("22:38").do(spider)
#schedule.every().day.at("01:00").do(spider(),'It is 6:05')

while True:
    schedule.run_pending()
    time.sleep(10) # wait 10 secs
