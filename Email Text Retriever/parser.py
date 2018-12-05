import email
import mimetypes
import uuid
from emaildata.attachment import Attachment

message = email.message_from_file(open('9.eml'))
for content, filename, mimetype, message in Attachment.extract(message, False):
    if not filename:
        filename = str(uuid.uuid1()) + (mimetypes.guess_extension(mimetype) or '.txt')
    print "here"
    print filename
    with open(filename, 'w') as stream:
        stream.write(content)
    # If message is not None then it is an instance of email.message.Message
    if message:
        print "The file {0} is a message with attachments.".format(filename)
