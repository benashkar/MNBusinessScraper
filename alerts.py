#!/usr/bin/env python3
"""
===============================================================================
MINNESOTA BUSINESS SCRAPER - ALERT SYSTEM
===============================================================================

This module provides alerting capabilities via Slack and Email for the
MN Business Scraper project.

PURPOSE:
--------
Send notifications when:
1. A scraping year is completed
2. Data is pushed to GitHub
3. Errors occur that need attention
4. Progress milestones are reached

SETUP INSTRUCTIONS:
-------------------

SLACK ALERTS:
1. Create a Slack App at https://api.slack.com/apps
2. Enable "Incoming Webhooks" for your app
3. Create a webhook URL for your channel
4. Set the SLACK_WEBHOOK_URL environment variable:
   export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

EMAIL ALERTS:
1. Set up environment variables for SMTP:
   export SMTP_HOST="smtp.gmail.com"
   export SMTP_PORT="587"
   export SMTP_USER="your-email@gmail.com"
   export SMTP_PASSWORD="your-app-password"  # Use App Password for Gmail
   export ALERT_EMAIL_TO="recipient@example.com"

USAGE:
------
    from alerts import send_alert, send_slack_alert, send_email_alert

    # Send to all configured channels
    send_alert("Scraping Complete", "Year 2023 has finished scraping.")

    # Send only to Slack
    send_slack_alert("Quick Update", "Found 1000 new records!")

    # Send only via Email
    send_email_alert("Error Alert", "Worker 3 encountered an error.")

FOR JUNIOR DEVELOPERS:
----------------------
This module uses two common notification methods:

1. SLACK WEBHOOKS: A simple way to send messages to Slack channels.
   You POST a JSON payload to a special URL and Slack displays the message.

2. SMTP EMAIL: The standard protocol for sending emails.
   Python's smtplib handles the connection and sending.

ENVIRONMENT VARIABLES: These are settings stored outside your code.
   They're used for sensitive data (passwords, API keys) that shouldn't
   be in your source code. Access them with os.environ.get('VAR_NAME').

===============================================================================
"""

import json
import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

# For HTTP requests to Slack webhook
# Using urllib so we don't need the 'requests' library as a dependency
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION FROM ENVIRONMENT VARIABLES
# =============================================================================

# Slack Configuration
# Get the webhook URL from environment variable
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', '')

# Email Configuration
# These should be set as environment variables for security
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
ALERT_EMAIL_TO = os.environ.get('ALERT_EMAIL_TO', '')
ALERT_EMAIL_FROM = os.environ.get('ALERT_EMAIL_FROM', SMTP_USER)


# =============================================================================
# SLACK ALERT FUNCTIONS
# =============================================================================

def send_slack_alert(
    title: str,
    message: str,
    color: str = '#003366',
    webhook_url: Optional[str] = None
) -> bool:
    """
    Send an alert message to Slack via webhook.

    PARAMETERS:
    -----------
    title : str
        The title/header of the alert message.

    message : str
        The main content of the alert.

    color : str
        Hex color code for the message sidebar (default: dark blue).
        Common colors:
        - '#28a745' = green (success)
        - '#dc3545' = red (error)
        - '#ffc107' = yellow (warning)
        - '#003366' = blue (info)

    webhook_url : str, optional
        Override the default webhook URL.

    RETURNS:
    --------
    bool
        True if message was sent successfully, False otherwise.

    EXAMPLE:
    --------
    >>> send_slack_alert(
    ...     "Scrape Complete",
    ...     "Year 2023 finished with 7,073 records.",
    ...     color='#28a745'
    ... )
    True
    """
    # Use provided URL or fall back to environment variable
    url = webhook_url or SLACK_WEBHOOK_URL

    # Check if Slack is configured
    if not url:
        logger.warning("Slack webhook URL not configured. Set SLACK_WEBHOOK_URL environment variable.")
        return False

    # Build the Slack message payload
    # Using "attachments" format for rich formatting
    payload = {
        "attachments": [
            {
                "color": color,
                "title": title,
                "text": message,
                "footer": "MN Business Scraper",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }

    # Convert payload to JSON bytes
    data = json.dumps(payload).encode('utf-8')

    # Create the HTTP request
    request = Request(
        url,
        data=data,
        headers={'Content-Type': 'application/json'}
    )

    try:
        # Send the request
        with urlopen(request, timeout=10) as response:
            if response.status == 200:
                logger.info(f"Slack alert sent: {title}")
                return True
            else:
                logger.error(f"Slack returned status {response.status}")
                return False

    except HTTPError as e:
        logger.error(f"Slack HTTP error: {e.code} - {e.reason}")
        return False
    except URLError as e:
        logger.error(f"Slack URL error: {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Slack alert failed: {e}")
        return False


def send_slack_success(title: str, message: str) -> bool:
    """Send a green success alert to Slack."""
    return send_slack_alert(title, message, color='#28a745')


def send_slack_error(title: str, message: str) -> bool:
    """Send a red error alert to Slack."""
    return send_slack_alert(title, message, color='#dc3545')


def send_slack_warning(title: str, message: str) -> bool:
    """Send a yellow warning alert to Slack."""
    return send_slack_alert(title, message, color='#ffc107')


# =============================================================================
# EMAIL ALERT FUNCTIONS
# =============================================================================

def send_email_alert(
    subject: str,
    body: str,
    to_email: Optional[str] = None,
    html: bool = False
) -> bool:
    """
    Send an alert email via SMTP.

    PARAMETERS:
    -----------
    subject : str
        Email subject line.

    body : str
        Email body content.

    to_email : str, optional
        Recipient email address. Uses ALERT_EMAIL_TO if not provided.

    html : bool
        If True, send body as HTML content. Default is plain text.

    RETURNS:
    --------
    bool
        True if email was sent successfully, False otherwise.

    EXAMPLE:
    --------
    >>> send_email_alert(
    ...     "Scraping Error",
    ...     "Worker 3 failed with timeout error.",
    ...     to_email="admin@example.com"
    ... )
    True

    GMAIL SETUP:
    ------------
    1. Enable 2-Factor Authentication on your Google account
    2. Generate an "App Password" at https://myaccount.google.com/apppasswords
    3. Use the App Password (not your regular password) for SMTP_PASSWORD
    """
    # Determine recipient
    recipient = to_email or ALERT_EMAIL_TO

    # Check if email is configured
    if not all([SMTP_USER, SMTP_PASSWORD, recipient]):
        logger.warning("Email not configured. Set SMTP_USER, SMTP_PASSWORD, and ALERT_EMAIL_TO.")
        return False

    # Build the email message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"[MN Scraper] {subject}"
    msg['From'] = ALERT_EMAIL_FROM
    msg['To'] = recipient

    # Add timestamp to body
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    full_body = f"{body}\n\n---\nSent at: {timestamp}\nMN Business Scraper Alert System"

    # Attach content (HTML or plain text)
    if html:
        msg.attach(MIMEText(full_body, 'html'))
    else:
        msg.attach(MIMEText(full_body, 'plain'))

    try:
        # Connect to SMTP server
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            # Start TLS encryption
            server.starttls()

            # Login with credentials
            server.login(SMTP_USER, SMTP_PASSWORD)

            # Send the email
            server.sendmail(ALERT_EMAIL_FROM, recipient, msg.as_string())

        logger.info(f"Email alert sent: {subject}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Email authentication failed. Check SMTP_USER and SMTP_PASSWORD.")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error: {e}")
        return False
    except Exception as e:
        logger.error(f"Email alert failed: {e}")
        return False


# =============================================================================
# UNIFIED ALERT FUNCTION
# =============================================================================

def send_alert(
    title: str,
    message: str,
    level: str = 'info',
    channels: Optional[list] = None
) -> dict:
    """
    Send an alert to all configured channels.

    This is the main function to use for sending alerts. It will try to
    send to all available channels (Slack and Email).

    PARAMETERS:
    -----------
    title : str
        Alert title/subject.

    message : str
        Alert message body.

    level : str
        Alert level: 'info', 'success', 'warning', 'error'
        This affects the color/formatting of the alert.

    channels : list, optional
        List of channels to use: ['slack', 'email']
        If None, tries all configured channels.

    RETURNS:
    --------
    dict
        Results for each channel: {'slack': True/False, 'email': True/False}

    EXAMPLE:
    --------
    >>> results = send_alert(
    ...     "Year Complete",
    ...     "Finished scraping 2023 with 7,073 records.",
    ...     level='success'
    ... )
    >>> print(results)
    {'slack': True, 'email': True}
    """
    # Determine which channels to use
    if channels is None:
        channels = ['slack', 'email']

    # Color mapping for different levels
    colors = {
        'info': '#003366',
        'success': '#28a745',
        'warning': '#ffc107',
        'error': '#dc3545'
    }
    color = colors.get(level, '#003366')

    results = {}

    # Send to Slack
    if 'slack' in channels:
        results['slack'] = send_slack_alert(title, message, color=color)

    # Send via Email
    if 'email' in channels:
        results['email'] = send_email_alert(title, message)

    return results


# =============================================================================
# PREDEFINED ALERT TEMPLATES
# =============================================================================

def alert_year_complete(year: int, record_count: int):
    """
    Send alert when a year's scraping is complete.

    PARAMETERS:
    -----------
    year : int
        The year that was completed.
    record_count : int
        Number of records found for that year.
    """
    return send_alert(
        f"Year {year} Scraping Complete",
        f"Successfully scraped {record_count:,} business records for year {year}.\n"
        f"Data has been saved to the output files.",
        level='success'
    )


def alert_github_push(commit_message: str, records_pushed: int):
    """
    Send alert when data is pushed to GitHub.

    PARAMETERS:
    -----------
    commit_message : str
        The git commit message.
    records_pushed : int
        Total number of records in the push.
    """
    return send_alert(
        "Data Pushed to GitHub",
        f"Successfully pushed {records_pushed:,} records to GitHub.\n"
        f"Commit: {commit_message}",
        level='success'
    )


def alert_error(error_type: str, error_message: str, worker_id: Optional[int] = None):
    """
    Send alert when an error occurs.

    PARAMETERS:
    -----------
    error_type : str
        Type of error (e.g., "Timeout", "Connection", "Parse")
    error_message : str
        Detailed error message.
    worker_id : int, optional
        ID of the worker that encountered the error.
    """
    worker_info = f" (Worker {worker_id})" if worker_id is not None else ""
    return send_alert(
        f"Scraper Error{worker_info}",
        f"Error Type: {error_type}\n"
        f"Message: {error_message}\n"
        f"Action Required: Please check the scraper logs.",
        level='error'
    )


def alert_progress_milestone(milestone: str, current_count: int, total_patterns: int = 676):
    """
    Send alert for progress milestones.

    PARAMETERS:
    -----------
    milestone : str
        Description of the milestone (e.g., "50% Complete")
    current_count : int
        Current number of patterns completed.
    total_patterns : int
        Total number of patterns (default: 676)
    """
    pct = round(current_count / total_patterns * 100, 1)
    return send_alert(
        f"Progress: {milestone}",
        f"Completed {current_count}/{total_patterns} patterns ({pct}%)",
        level='info'
    )


# =============================================================================
# TESTING FUNCTIONS
# =============================================================================

def test_slack():
    """Test Slack alert functionality."""
    print("Testing Slack alerts...")
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set. Skipping Slack test.")
        return False

    result = send_slack_alert(
        "Test Alert",
        "This is a test message from the MN Business Scraper.",
        color='#003366'
    )
    print(f"Slack test result: {'SUCCESS' if result else 'FAILED'}")
    return result


def test_email():
    """Test Email alert functionality."""
    print("Testing Email alerts...")
    if not all([SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO]):
        print("Email not configured. Skipping email test.")
        print("Set SMTP_USER, SMTP_PASSWORD, and ALERT_EMAIL_TO environment variables.")
        return False

    result = send_email_alert(
        "Test Alert",
        "This is a test message from the MN Business Scraper."
    )
    print(f"Email test result: {'SUCCESS' if result else 'FAILED'}")
    return result


# =============================================================================
# MAIN - Testing
# =============================================================================

if __name__ == '__main__':
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║       MN Business Scraper - Alert System Test                ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    print("Current Configuration:")
    print(f"  Slack Webhook: {'Configured' if SLACK_WEBHOOK_URL else 'NOT SET'}")
    print(f"  SMTP Host: {SMTP_HOST}")
    print(f"  SMTP User: {'Configured' if SMTP_USER else 'NOT SET'}")
    print(f"  Email To: {ALERT_EMAIL_TO or 'NOT SET'}")
    print()

    # Run tests
    slack_ok = test_slack()
    print()
    email_ok = test_email()

    print()
    print("=" * 60)
    print("To configure alerts, set these environment variables:")
    print()
    print("For Slack:")
    print("  export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'")
    print()
    print("For Email (Gmail example):")
    print("  export SMTP_HOST='smtp.gmail.com'")
    print("  export SMTP_PORT='587'")
    print("  export SMTP_USER='your-email@gmail.com'")
    print("  export SMTP_PASSWORD='your-app-password'")
    print("  export ALERT_EMAIL_TO='recipient@example.com'")
    print("=" * 60)
