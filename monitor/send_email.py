import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path

from dotenv import load_dotenv # pip install python-dotenv

PORT = 586
EMAIL_SERVER = "smtp-mail.outlook.com"

# Load the environment variables
current_dir = Path(__file__).resolve().parent if "_file_" in locals() else Path.cwd() #if statement for jupyter notebook
envars = current_dir / ".env"
load_dotenv(envars)

# Read environment variables
sender_email = os.getenv("EMAIL")
password_email = os.getenv("PASSWORD")

def send_email(subject, receiver_email, name, body):
    """Send an email notification."""
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(("Awards Monitor", sender_email))
    msg["To"] = receiver_email
    msg["BCC"] = sender_email   # This will send a copy of the email to the sender
    msg.set_content(f"Hello {name},\n\n{body}\n\nBest regards,\nAwards Monitor")

    try:
        with smtplib.SMTP(EMAIL_SERVER, PORT) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, password_email)
            server.send_message(msg)
        print(f"Email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send email to {receiver_email}: {e}")

if __name__ == "__main__":
    # Example usage
    send_email(
        subject = "Test Email from Awards Monitor",
        receiver_email = "afarmus@purdue.edu",
        name = "Ana Farmus",
        body = ":3",
    )
