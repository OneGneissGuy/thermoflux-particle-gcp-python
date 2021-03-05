import base64
import os
import json

from sendgrid.helpers.mail import (
    Mail,
    Attachment,
    FileContent,
    FileName,
    FileType,
    Disposition,
    ContentId,
)
from sendgrid import SendGridAPIClient

message = Mail(
    from_email="contact@anaposensing.com",
    to_emails="jfsaraceno@gmail.com",
    subject="Sending with Twilio SendGrid is Fun",
    html_content="<strong>and easy to do anywhere, even with Python</strong>",
)
file_path = "Voltaic Systems P103 R3B.pdf"

with open(file_path, "rb") as f:
    data = f.read()
    f.close()
encoded = base64.b64encode(data).decode()
attachment = Attachment()
attachment.file_content = FileContent(encoded)
attachment.file_type = FileType("application/pdf")
attachment.file_name = FileName("test_filename.pdf")
attachment.disposition = Disposition("attachment")
attachment.content_id = ContentId("Example Content ID")
message.attachment = attachment
try:
    with open("sendmail-apikey.json") as f:
        data = json.load(f)
    #    sendgrid_client = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
    sendgrid_client = SendGridAPIClient(data["SENDGRID_API_KEY"])
    response = sendgrid_client.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e.message)