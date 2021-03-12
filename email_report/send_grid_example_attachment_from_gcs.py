import base64
import os

# import json
from urllib.parse import urlparse
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
from google.cloud import storage

# TODO: add link to data in html_content
def decode_gcs_url(url):
    p = urlparse(url)
    path = p.path[1:].split("/", 1)
    bucket, file_path = path[0], path[1]
    return bucket, file_path


def create_attachment(report):
    encoded = base64.b64encode(report).decode()
    attachment = Attachment()
    attachment.file_content = FileContent(encoded)
    attachment.file_type = FileType("image/png")
    attachment.file_name = FileName("test_report.png")
    attachment.disposition = Disposition("attachment")
    attachment.content_id = ContentId("Example Content ID")
    return attachment


def fetch_report(url, storage_client):
    bucket, file_path = decode_gcs_url(url)
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(file_path)
    report = blob.download_as_string()
    return report


def email_report(event, context):

    storage_client = storage.Client()
    url = "https://storage.cloud.google.com/thermoflux-reports/report.png"
    report = fetch_report(url, storage_client)
    to_email = [("report-testing@googlegroups.com"), ("jfsaraceno@gmail.com")]

    message = Mail(
        from_email=("contact@anaposensing.com", "Anapos Sensing and Design"),
        to_emails=to_email,
        subject="Daily Report",
        html_content="""
        <p>
        <strong>Daily Report<strong>
        <br>
        </p>
        <p>
        <a
        href="https://storage.cloud.google.com/thermoflux-output/alfalfa_demo_table_output.csv">Download data included in this report
        </a>
        </p>""",
    )
    attachment = create_attachment(report)
    message.attachment = attachment

    try:
        # with open("sendmail-apikey.json") as f:
        #     data = json.load(f)
        # sendgrid_client = SendGridAPIClient(data["SENDGRID_API_KEY"])
        sendgrid_client = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        response = sendgrid_client.send(message)
        code, body, headers = response.status_code, response.body, response.headers
        print(f"Response Code: {code} ")
        print(f"Response Body: {body} ")
        print(f"Response Headers: {headers} ")
        print("Message Sent!")
    except Exception as e:
        print("Error: {0}".format(e))

    return str(response.status_code)