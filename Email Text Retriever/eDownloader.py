# -*- coding: utf-8 -*-
import imaplib
import os
import getpass
import re
import sched, time
import email
import mimetypes
import uuid
from emaildata.attachment import Attachment
import glob
import PyPDF2
import mysql
import mysql.connector

UID_RE = re.compile(r"\d+\s+\(UID (\d+)\)$")
FILE_RE = re.compile(r"(\d+).eml$")
GMAIL_FOLDER_NAME = "[Gmail]/All Mail"
s = sched.scheduler(time.time, time.sleep)

def getUIDForMessage(svr, n):
    resp, lst = svr.fetch(n, 'UID')
    m = UID_RE.match(lst[0])
    if not m:
        raise Exception(
            "Internal error parsing UID response: %s %s.  Please try again" % (resp, lst))
    return m.group(1)


def downloadMessage(svr, n, fname):
    resp, lst = svr.fetch(n, '(RFC822)')
    if resp != 'OK':
        raise Exception("Bad response: %s %s" % (resp, lst))
    f = open(fname, 'w')
    #print fname
    f.write(lst[0][1])
    f.close()

    message = email.message_from_file(open(fname))
    for content, filename, mimetype, message in Attachment.extract(message, False):
        if not filename:
            filename = str(uuid.uuid1()) + (mimetypes.guess_extension(mimetype) or '.txt')
        print "here"
        #print filename
        with open(filename, 'w') as stream:
            stream.write(content)
        # If message is not None then it is an instance of email.message.Message
        if message:
            print "The file {0} is a message with attachments.".format(filename)

    # Traversing directory for .pdf files
    list_of_files = []
    os.chdir("/home/hduser1/PycharmProjects/Email handler")
    for file in glob.glob("*.pdf"):
        #print file
        if file not in list_of_files:
            list_of_files.append(file)

    print list_of_files

    list_of_files = []
    os.chdir("/home/hduser1/PycharmProjects/Email handler")
    for file in glob.glob("*.pdf"):
        #print file
        if file not in list_of_files:
            list_of_files.append(file)

    print list_of_files

    cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='articles')
    cursor = cnx.cursor()

    for files in list_of_files:
        pdfFileObj = open(files, 'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        pages = pdfReader.numPages
        ticker = 0

        file_text = ""

        while (ticker < pages):
            pageObj = pdfReader.getPage(ticker)
            file_text = pageObj.extractText()
            file_text = file_text.encode("utf-8")
            ticker += 1

        data = (files, file_text)

        cursor.execute("SELECT file_name, COUNT(*) FROM pdf_table WHERE file_name = %s GROUP BY file_name",
                       (files,))
        # query =
        msg = cursor.fetchone()
        # check if it is empty and print error
        if not msg:
            print 'It does not exist'
            cursor.execute("insert into pdf_table (file_name, text) values(%s,%s)", (data))
            # cursor.execute(add_to_db_query, data)
            cnx.commit()

            print "Added in database"


            # counter += 1
            # counter2 += 1

        # else:
        #     print "Already present in database!"

    cursor.close()
    cnx.close()

def UIDFromFilename(fname):
    m = FILE_RE.match(fname)
    if m:
        return int(m.group(1))


def get_credentials():
    user = "mystockspk@gmail.com"
    pwd = "mySTOCKSpk10"
    # user = raw_input("Gmail address: ")
    # pwd = getpass.getpass("Gmail password: ")
    return user, pwd


def do_backup(sc):
    svr = imaplib.IMAP4_SSL('imap.gmail.com')
    user, pwd = get_credentials()
    svr.login(user, pwd)

    resp, [countstr] = svr.select(GMAIL_FOLDER_NAME, True)
    count = int(countstr)

    existing_files = os.listdir(".")
    lastdownloaded = max(UIDFromFilename(f)
                         for f in existing_files) if existing_files else 0

    # A simple binary search to see where we left off
    gotten, ungotten = 0, count + 1
    while (ungotten - gotten) > 1:
        attempt = (gotten + ungotten) / 2
        uid = getUIDForMessage(svr, attempt)
        if int(uid) <= lastdownloaded:
            print "Finding starting point: %d/%d (UID: %s) too low" % (attempt, count, uid)
            gotten = attempt
        else:
            print "Finding starting point: %d/%d (UID: %s) too high" % (attempt, count, uid)
            ungotten = attempt

    # The download loop
    for i in range(ungotten, count + 1):
        uid = getUIDForMessage(svr, i)
        print "Downloading %d/%d (UID: %s)" % (i, count, uid)
        another_uid = uid
        downloadMessage(svr, i, uid + '.eml')


    svr.close()
    svr.logout()

    print "\n"
    s.enter(10, 1, do_backup, (sc,))

#if __name__ == "__main__":
s.enter(10, 1, do_backup, (s,))
s.run()
#do_backup()