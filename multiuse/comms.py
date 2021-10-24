"""Communications via text, email, signal."""
# %% codecell
import os
from dotenv import load_dotenv
from twilio.rest import Client
# %% codecell


def send_twilio_message(msg=None, to=False):
    """Send text message using twilio."""
    load_dotenv()
    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    twilio_number = os.environ['TWILIO_NUMBER']
    if not to:
        to = os.environ['MY_NUMBER']

    client = Client(account_sid, auth_token)

    if not msg:
        msg = "Testing testing 123"

    client.api.account.messages.create(
        to=to,
        from_=twilio_number,
        body=msg)

# %% codecell
