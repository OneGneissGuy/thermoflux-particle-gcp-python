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
    To,
    From,
)
from sendgrid import SendGridAPIClient

# to_email = To("LandIQ-data-reports@googlegroups.com", "jfsaraceno@gmail.com")
message = Mail(
    from_email="contact@anaposensing.com",
    to_emails="LandIQ-data-reports@googlegroups.com",
    subject="Test Email Report Sending with Twilio SendGrid",
    html_content="<strong>Test Report</strong>",
)
file_path = "LT_MicroIQ_Alfalfa - Daily .png"

with open(file_path, "rb") as f:
    data = f.read()
    f.close()
encoded = base64.b64encode(data).decode()
attachment = Attachment()
attachment.file_content = FileContent(encoded)
attachment.file_type = FileType("image/png")
attachment.file_name = FileName("test_report.png")
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