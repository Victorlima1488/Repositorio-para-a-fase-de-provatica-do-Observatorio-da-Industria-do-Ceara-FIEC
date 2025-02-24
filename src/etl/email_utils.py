import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

def send_email(subject, body):
    # SMTP server settings
    sender_email = os.getenv('SENDER_EMAIL')  # Your email
    receiver_email = os.getenv('RECEIVER_EMAIL')  # Recipient email
    password = os.getenv('SENDER_PASSWORD')  # Email password (you can use an app password if using Gmail)

    # SMTP server configuration (Example: Gmail)
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    # Creating the email object
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Email body
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connecting to the SMTP server and sending the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Encrypt the communication
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print(f'Email sent to {receiver_email}')
    except Exception as e:
        print(f'Error sending the email: {e}')
