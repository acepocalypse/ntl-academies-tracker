"""
monitor/notify.py

Central email notification utility for NTL-Academies-Tracker.
Loads Gmail API credentials and sends plain-text status reports via Gmail API.
"""
import os
import os.path
import base64
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === Gmail API Configuration ===
SCOPES = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/gmail.readonly"]
# Requires credentials.json (from Google Cloud Console) and token.json (generated on first run)

def get_gmail_service():
    """Get authenticated Gmail API service and user email."""
    creds = None
    current_dir = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
    token_path = current_dir / "monitor\\token.json"
    creds_path = current_dir / "monitor\\credentials.json"
    
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as token:
            token.write(creds.to_json())
    
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    user_email = profile["emailAddress"]
    return service, user_email

def email_notify(
    subject: str,
    body: str,
    to_addrs: list[str],
    name: str = "",
    attachments: list[str] = None,
) -> None:
    """
    Send a plain-text email notification via Gmail API, with optional attachments.

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
    attachments : list[str], optional
        List of file paths to attach to the email.
    """
    attachments = attachments or []
    greeting = f"Hello {name}," if name else "Hello,"
    closing = "Best regards,\nEDA Automated Bot"
    attach_note = ""
    if attachments:
        attach_list = "\n".join(f"  - {os.path.basename(f)}" for f in attachments)
        attach_note = f"\n\nAttached files:\n{attach_list}"
    full_body = f"{greeting}\n\n{body}{attach_note}\n\n{closing}"

    try:
        service, user_email = get_gmail_service()
        if attachments:
            msg = MIMEMultipart()
            msg.attach(MIMEText(full_body))
        else:
            msg = MIMEText(full_body)
        msg['Subject'] = subject
        msg['From'] = formataddr(("Awards Monitor", user_email))
        msg['To'] = ", ".join(to_addrs)
        msg['BCC'] = user_email  # send a copy to self

        # Attach files if any
        for filepath in attachments:
            if not os.path.isfile(filepath):
                print(f"[WARNING] Attachment not found: {filepath}")
                continue
            with open(filepath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{os.path.basename(filepath)}"',
                )
                msg.attach(part)

        raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        message = {'raw': raw_msg}
        sent_msg = service.users().messages().send(userId="me", body=message).execute()
        print(f"[SUCCESS] Email sent to {', '.join(to_addrs)} (Message ID: {sent_msg['id']})")
    except HttpError as e:
        print(f"[ERROR] Failed to send email to {', '.join(to_addrs)}: {e}")

# Optional quick test
if __name__ == "__main__":
    email_notify(
        subject="Test Email from Awards Monitor",
        body=":3",
        to_addrs=["mrakmalsetiawan@gmail.com", "afarmus@purdue.edu", "anafarmus@gmail.com"],
        name="Test Recipient",
        attachments=[],  # Add file paths here to test attachments
    )

