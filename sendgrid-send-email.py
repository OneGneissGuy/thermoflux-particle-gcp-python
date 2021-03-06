# https://us-central1-thermoflux-particle.cloudfunctions.net/sendgrid-send-email
def email(request):
    import os
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email
    from python_http_client.exceptions import HTTPError

    sg = SendGridAPIClient(os.environ["SENDGRID-API-KEY"])

    html_content = "<p>Hello World!</p>"

    message = Mail(
        to_emails="jfsaraceno@gmail.com",
        from_email=Email("contact@anaposensing.com", "Anapos"),
        subject="Hello world",
        html_content=html_content,
    )
    # message.add_bcc("[YOUR]@gmail.com")

    try:
        response = sg.send(message)
        return f"email.status_code={response.status_code}"
        # expected 202 Accepted

    except HTTPError as e:
        return e.message


def build_hello_email():
    ## Send a Single Email to a Single Recipient
    import os
    import json
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Mail,
        From,
        To,
        Subject,
        PlainTextContent,
        HtmlContent,
        SendGridException,
    )

    message = Mail(
        from_email=From("from@example.com.com", "Example From Name"),
        to_emails=To("to@example.com", "Example To Name"),
        subject=Subject("Sending with SendGrid is Fun"),
        plain_text_content=PlainTextContent(
            "and easy to do anywhere, even with Python"
        ),
        html_content=HtmlContent(
            "<strong>and easy to do anywhere, even with Python</strong>"
        ),
    )

    try:
        print(json.dumps(message.get(), sort_keys=True, indent=4))
        return message.get()

    except SendGridException as e:
        print(e.message)

    for cc_addr in personalization["cc_list"]:
        mock_personalization.add_to(cc_addr)

    for bcc_addr in personalization["bcc_list"]:
        mock_personalization.add_bcc(bcc_addr)

    for header in personalization["headers"]:
        mock_personalization.add_header(header)

    for substitution in personalization["substitutions"]:
        mock_personalization.add_substitution(substitution)

    for arg in personalization["custom_args"]:
        mock_personalization.add_custom_arg(arg)

    mock_personalization.subject = personalization["subject"]
    mock_personalization.send_at = personalization["send_at"]
    return mock_personalization


def send_hello_email():
    # Assumes you set your environment variable:
    # https://github.com/sendgrid/sendgrid-python/blob/HEAD/TROUBLESHOOTING.md#environment-variables-and-your-sendgrid-api-key
    message = build_hello_email()
    sendgrid_client = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
    response = sendgrid_client.send(message=message)
    print(response.status_code)
    print(response.body)
    print(response.headers)


def build_attachment2():
    """Build attachment mock."""
    attachment = Attachment()
    attachment.file_content = "BwdW"
    attachment.file_type = "image/png"
    attachment.file_name = "banner.png"
    attachment.disposition = "inline"
    attachment.content_id = "Banner"
    return attachment