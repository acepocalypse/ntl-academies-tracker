# monitor/notify.py
import json
import smtplib
import ssl
import urllib.request
from email.message import EmailMessage


def email_notify(
    subject: str,
    body: str,
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    to_addrs: list[str],
) -> None:
    """Send a plaintext email notification."""
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = username
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(username, password)
        server.send_message(msg)


def discord_notify(webhook_url: str, content: str) -> None:
    """Send a simple text message to a Discord channel via webhook."""
    data = {"content": content}
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        # Optional: check status code
        if resp.status != 204:  # Discord returns 204 No Content on success
            raise RuntimeError(f"Discord webhook failed with HTTP {resp.status}")
