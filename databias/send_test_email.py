import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv  # <-- important

# Load variables from .env
load_dotenv()

SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

msg = MIMEText("✅ This is a test email from your CiteConnect bias detection system.")
msg["Subject"] = "✅ Bias Detection Test Email"
msg["From"] = SMTP_USER
msg["To"] = SMTP_USER

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    print(f"✅ Test email sent successfully to {SMTP_USER}")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
