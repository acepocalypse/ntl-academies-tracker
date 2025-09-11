"""
monitor/notify.py

Central email notification utility for NTL-Academies-Tracker.
Loads email credentials from a local .env file and sends
plain-text status reports via Outlook SMTP.
"""
import os
import smtplib
import urllib.request
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from dotenv import load_dotenv

# === SMTP Configuration ===
PORT = 587  # correct STARTTLS port for Outlook
EMAIL_SERVER = "smtp-mail.outlook.com"

# === Load credentials from .env ===
# .env file should contain:
#   EMAIL=ExternalAnalytics@purdue.edu
#   PASSWORD=<app_password>
current_dir = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
load_dotenv(current_dir / ".env")

SENDER_EMAIL = os.getenv("EMAIL")
PASSWORD_EMAIL = os.getenv("PASSWORD")

if not SENDER_EMAIL or not PASSWORD_EMAIL:
    raise RuntimeError("❌ EMAIL or PASSWORD not found in .env file")

def email_notify(
    subject: str,
    body: str,
    to_addrs: list[str],
    name: str = "",
) -> None:
    
    """
    Send a plain-text email notification.

    Parameters
    ----------
    subject : str
        Email subject line.
    body : str
        Main text of the message.
    to_addrs : list[str]
        List of recipient email addresses.
    name : str, optional
        Recipient name for greeting (default empty).
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(("Awards Monitor", SENDER_EMAIL))
    msg["To"] = ", ".join(to_addrs)
    msg["BCC"] = SENDER_EMAIL  # send a copy to self
    greeting = f"Hello {name},\n\n" if name else ""
    msg.set_content(f"{greeting}{body}\n\nBest regards,\nAwards Monitor")

    try:
        with smtplib.SMTP(EMAIL_SERVER, PORT) as server:
            server.starttls()  # secure connection
            server.login(SENDER_EMAIL, PASSWORD_EMAIL)
            server.send_message(msg)
        print(f"✅ Email sent to {', '.join(to_addrs)}")
    except Exception as e:
        print(f"❌ Failed to send email to {', '.join(to_addrs)}: {e}")

# Optional quick test
if __name__ == "__main__":
    email_notify(
        subject="Test Email from Awards Monitor",
        body=":3",
        to_addrs=["your_test_address@purdue.edu"],
        name="Test Recipient",
    )