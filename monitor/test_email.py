# monitor/test_email.py
from notify import email_notify

# Fill in with the same values you put in settings.toml
smtp_host = "smtp-mail.outlook.com"
smtp_port = 465
username  = "setiawa@purdue.edu"
password  = "Tacotofu2020"
to_addrs = ["mrakmalsetiawan@gmail.com"]

subject = "Awards Monitor Test"
body    = "This is a test email to confirm SMTP settings are correct."

try:
    email_notify(
        subject=subject,
        body=body,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=username,
        password=password,
        to_addrs=to_addrs,
    )
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
