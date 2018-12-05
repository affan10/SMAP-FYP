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

opt = 'a'
#nltk.download('stopwords')
#nltk.download('punkt')

while(opt != 'q'):

    user_in = raw_input("Enter 1 for reading from file:\nEnter 2 for entering URLs manually:\nEnter 3 for keywords search: "
                        "\nEnter 4 for crawling articles from a website: ")
    counter = 0

    if user_in == "1":
        file_name = raw_input("Enter file name: ")
        file_name = file_name + ".txt"
        dict_of_urls = {}
        file = open(file_name)
        dict_of_file_names = {}
        counter2 = 0

        for line in file:
            line = re.sub('[\n]', '', line)
            dict_of_urls[counter] = line
            download_url = dict_of_urls[counter]
            response = urllib2.urlopen(download_url)
            dict_of_file_names[counter2] = download_url.rsplit('/', 1)[-1]
            file = open(dict_of_file_names[counter2], 'wb')
            file.write(response.read())
            file.close()
            print("Completed")
            #print file_text

            ######## MySQL Connection #########
            cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='articles')
            cursor = cnx.cursor()

            # add_to_db_query = ("INSERT INTO articles table "
            #                 "(Identifier, Title, Text) "
            #                 "VALUES (%s(Identifier), %(Title)s, %(Text)s)")


            pdfFileObj = open(dict_of_file_names[counter2], 'rb')
            pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
            pages = pdfReader.numPages
            ticker = 0

            file_text = ""

            while (ticker < pages):
                pageObj = pdfReader.getPage(ticker)
                file_text += pageObj.extractText()
                ticker += 1

            #print file_text

            data = (dict_of_file_names[counter2], file_text)

            cursor.execute("SELECT Title, COUNT(*) FROM articles_table WHERE Title = %s GROUP BY Title",(dict_of_file_names[counter2],))
            #query =
            msg = cursor.fetchone()
            # check if it is empty and print error
            if not msg:
                print 'It does not exist'
                cursor.execute("insert into articles_table (Title, Text) values(%s,%s)", (data))
                # cursor.execute(add_to_db_query, data)
                cnx.commit()
                cursor.close()
                cnx.close()

                print "Added in database"

                counter += 1
                counter2 += 1

                file.close()
            else:
                print "Already present in database!"

        counter = 0
        option = raw_input("\nPress q to quit or any other to restart program: ")
        print "\n"
        if option == 'q':
            exit()

    if user_in == "2":
        download_url = raw_input("Enter complete URL: ")
        response = urllib2.urlopen(download_url)
        title = download_url.rsplit('/', 1)[-1]
        file = open(title, 'wb')
        file.write(response.read())
        file.close()
        print("Completed")

        cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='articles')
        cursor = cnx.cursor()

        pdfFileObj = open(title, 'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        pages = pdfReader.numPages
        ticker = 0

        file_text = ""

        while (ticker < pages):
            pageObj = pdfReader.getPage(ticker)
            file_text += pageObj.extractText()
            ticker += 1

        #print file_text

        data = (title, file_text)

        cursor.execute("SELECT Title, COUNT(*) FROM articles_table WHERE Title = %s GROUP BY Title",
                       (title,))
        # query =
        msg = cursor.fetchone()
        # check if it is empty and print error
        if not msg:
            cursor.execute("insert into articles_table (Title, Text) values(%s,%s)", (data))
            # cursor.execute(add_to_db_query, data)
            cnx.commit()
            cursor.close()
            cnx.close()
            print "Added in database"
        else:
            print "Already present in database!"

        option = raw_input("\nPress q to quit or any other to restart program: ")
        print "\n"
        if option == 'q':
            exit()

    if user_in == "3":

        # For stock market news

        # list_of_keywords = ["shares", "share", "stocks", "stock", "prices", "price",
        #                     "volume", "trade", "traded", "KSE", "KSE100", "points",
        #                     "market", "pts"]

        list_of_keywords = ["share", "stocks",
                            "volume", "trade","kse","points",
                            "market", "pts"]


                            #keywords = raw_input("Enter Keyword(s): ")

        temp = ""

        #list_of_keywords[0] = "+" + list_of_keywords[0]

        another_list = []
        for words in list_of_keywords:
            another_list.append("+" + words)


        print another_list

        keywords = ""

        for words in another_list:
            keywords = keywords + words
            keywords = keywords + " "

        print keywords

        # keywords = "+" + keywords
        # keywords = keywords.replace(" ", " +")

        print "\n"

        cnx = mysql.connector.connect(user='root', password='root', host='localhost', database='articles')
        cursor = cnx.cursor()

        cursor.execute("alter table articles_table ENGINE = MYISAM")

        cursor.execute("alter table articles_table add FULLTEXT(Title, Text)")

        #cursor.execute("SELECT text FROM articles_table")
        #cursor.execute("insert into articles_table (Title, Text) values(%s,%s)", (data))
        #back = cursor.execute("SELECT Text FROM articles_table WHERE MATCH(Text) AGAINST ('%s' IN BOOLEAN MODE)", (keywords))
        #cursor.execute(query, (keywords))
        # cursor.execute(add_to_db_query, data)
        #print back
        #cursor.execute(query, (keywords))

        cursor.execute(
            "select * from articles_table where match(Title,Text) against ('{}' IN BOOLEAN MODE)".format(keywords))
        records = cursor.fetchall()

        for row in records:
            print row[1], row[2]

        cnx.commit()
        cursor.close()
        cnx.close()

        option = raw_input("\nPress q to quit or any other to restart program: ")
        print "\n"
        if option == 'q':
            exit()

    if user_in == '4':
        url = raw_input("Enter a website to crawl articles from: ")
        nltk.download('stopwords')

        r = requests.get("http://" + url)

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

        keywords = {0: "twitter", 1: "facebook", 2: "fashion", 3: "entertainment",
                    4: "epaper", 5: "sport", 6: "politics", 7: "images", 8: "obituary", 9: "watch-live", 10: "herald",
                    11: "supplements",
                    12: "classifieds", 13: "aurora", 14: "cityfm", 15: "#comments", 16: "expo", 17: "nnews",
                    18:"latest-news", 19:"category", 20:"videos", 21:"tv-shows", 22:"urdu", 23:"live", 24:"php",
                    25:"trending", 26:"privacy", 27:"about", 28:"aspx", 29:"faq", 30:"talent", 31:"ratecardon"}

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

        for key in dict_of_cleaned_urls.keys():
            url = dict_of_cleaned_urls[key]
            g = Goose()
            article = g.extract(url=url)
            print article.title
            #dict_of_cleaned_articles_and_titles[article.title]
            print "Title printed"
            print "\n"
            # print article.meta_description
            text = article.cleaned_text
            stop_words = set(stopwords.words('english'))

            word_tokens = word_tokenize(text)

            filtered_sentence = [w for w in word_tokens if not w in stop_words]

            for words in filtered_sentence:
                filtered_sentence = words.encode('ascii', 'ignore')

            print filtered_sentence
            print type(filtered_sentence)
            filtered_sentence = str(filtered_sentence)

            for w in word_tokens:
                if w not in stop_words:
                    # filtered_sentence.append(w)
                    filtered_sentence += " " + w

            print "Filtered:"
            print filtered_sentence
            print "Text printed"
            # print article.top_image.src

            #dict_of_cleaned_articles_and_titles[article.title] = filtered_sentence

            cnx = mysql.connector.connect(user='root', password='root', host='localhost', database='articles')
            cursor = cnx.cursor()

            data = (article.title, filtered_sentence)

            #data = (title, file_text)

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

        cursor.close()
        cnx.close()
        option = raw_input("\nPress q to quit or any other to restart program: ")
        print "\n"
        if option == 'q':
            exit()