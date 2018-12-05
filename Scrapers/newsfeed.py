# Coming from the news international
import feedparser
from goose import Goose
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
import pysentiment as ps
from datetime import datetime
import time
import schedule
import sched
import csv
#from requests import get
import sys
reload(sys)

timestmp = str(datetime.now())
print timestmp

sys.setdefaultencoding('utf8')
# nltk.download('words')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('wordnet')

s = sched.scheduler(time.time, time.sleep)
titles = []
titles_urls = []
scores = {}

csv_file = open("out/news_effect.csv", "w")
writer = csv.writer(outcsv)
writer.writerow(["name", "title", "value", "time"])

def func():
    feed = feedparser.parse("https://news.google.com/news/rss/headlines/section/topic/BUSINESS.en_pk/Business?ned=en_pk&hl=en&gl=PK")
    #feed = feedparser.parse("http://feeds.feedburner.com/com/Yeor")
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

        #print "Lemmatized"
        #print no_unicode

        if article_title not in titles:
            # titles.append(article_title)
            # urls.append(article_link)
            titles.append(article_title)
            titles_urls.append(tuple([article_title,article_link]))
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

            #words = set(nltk.corpus.words.words())
            filtered_sentence = re.sub(ur"[^\w\d\s]+", '', filtered_sentence)

            print "Non-lemmatized News:"
            print filtered_sentence

            # filtered_sentence = [wnl.lemmatize(i, j[0].lower()) if j[0].lower() in ['a', 'n', 'v'] else wnl.lemmatize(i) for i, j in
            # pos_tag(word_tokenize(filtered_sentence))]
            #
            # no_unicode = []
            #
            # for items in filtered_sentence:
            #     no_unicode.append(items.encode("utf-8"))
            # for words in filtered_sentence:
            #      no_unicode += " " + words.encode("utf-8")



            lm = ps.LM()
            # hiv4 = ps.HIV4()
            #words = set(nltk.corpus.words.words())
            # text = " ".join(w for w in nltk.wordpunct_tokenize(filtered_sentence) \
            #                 if w.lower() in words or not w.isalpha())

            print filtered_sentence
            #print text
            # score1 = hiv4.get_score(body)
            #tokens = lm.tokenize(text)
            # print tokens
            # print tokens
            #score = lm.get_score(tokens)
            #print score['Polarity']

            #hiv4 = ps.HIV4()
            #words = set(nltk.corpus.words.words())
            #all_clean = filtered_sentence
            #all_clean = " ".join(x for x in no_unicode)
            all_clean = filtered_sentence
            # cleaned = " ".join(w for w in nltk.wordpunct_tokenize(all_clean) \
            #                   if w.lower() in words or not w.isalpha())

            #print all_clean

            general = [" kse "," psx ", "pakistan stock exchange",
                       "karachi stock exchange","pakistan stock market"]
            keywords_pso = [" pso ","pakistan state oil"]
            keywords_engro = [" engro ", "engro fertilizer"]
            keywords_hbl = [" hbl ", "habib bank"]
            keywords_ubl = [" ubl ", "united bank"]
            keywords_ogdcl = [" ogdc "," ogdcl ", "oil & gas development company", "oil and gas development company"]

            flag = 0
            #relevant = []

            for words in general:
                if words in all_clean:
                    flag = 1
                    #relevant.append(article_title)

            for words in keywords_pso:
                if words in all_clean:
                    flag = 2
                    #relevant.append(article_title)

            for words in keywords_hbl:
                if words in all_clean:
                    flag = 3
                    #relevant.append(article_title)

            for words in keywords_ogdcl:
                if words in all_clean:
                    flag = 4
                    #relevant.append(article_title)

            for words in keywords_ubl:
                if words in all_clean:
                    flag = 5

            for words in keywords_engro:
                if words in all_clean:
                    flag = 6
                    #relevant.append(article_title)

            #if flag == 1:
                #print "General Stock Market News"
                # tokens = lm.tokenize(all_clean)
                # score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                # print "Cleaned News Below:"
                # print all_clean
                #print "Score Below:"
                #print score

                # scores[article_title] = score['Polarity']

            csv_file = csv.reader(open("news_effect.csv", "a"), delimiter=",")
            writer = csv.writer(outcsv)
            #writer.writerow(["name", "title", "value", "time"])

            if flag == 2:
                print "News related to PSO"
                tokens = lm.tokenize(all_clean)
                score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                # print "Cleaned News Below:"
                # print all_clean
                print "Score Below:"
                print score

                #scores[article_title] = score['Polarity']
                #datetime.now().time()
                writer.writerow(["PSO", article_title, score, datetime.now().time()])

            if flag == 3:
                print "News related to HBL"
                tokens = lm.tokenize(all_clean)
                score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                # print "Cleaned News Below:"
                # print all_clean
                print "Score Below:"
                print score

                #scores[article_title] = score['Polarity']
                writer.writerow(["HBL", article_title, score, datetime.now().time()])

            if flag == 4:
                print "News related to OGDCL"
                tokens = lm.tokenize(all_clean)
                score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                # print "Cleaned News Below:"
                # print all_clean
                print "Score Below:"
                print score

                #scores[article_title] = score['Polarity']
                writer.writerow(["ODGCL", article_title, score, datetime.now().time()])

            if flag == 5:
                print "News related to UBL"
                tokens = lm.tokenize(all_clean)
                score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                # print "Cleaned News Below:"
                # print all_clean
                print "Score Below:"
                print score

                #scores[article_title] = score['Polarity']
                writer.writerow(["UBL", article_title, score, datetime.now().time()])

            if flag == 6:
                print "News related to ENGRO"
                tokens = lm.tokenize(all_clean)
                score = lm.get_score(tokens)
                #score = hiv4.get_score(all_clean.split())
                #print "Cleaned and Lemmatized News Below:"
                #print all_clean
                print "Score Dictionary Below:"
                print score

                #scores[article_title] = score['Polarity']
                writer.writerow(["ENGRO", article_title, score, datetime.now().time()])

            if flag == 0:
                print "News was irrelevant!"

            print titles
            #print scores
            print "\n"

        else:
            print "Already Processed!\n"

    print "Exiting...\n"

func()

#print "Re-running....\n"
# schedule.every(1).minutes.do(func)
#
# while True:
#     schedule.run_pending()
#    time.sleep(1)
