"""Authentication for TD Ameritrade API."""
# %% codecell
import os
from datetime import datetime

from authlib.integrations.requests_client import OAuth2Session
import dotenv
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.multiuse.aws_keys import get_secret
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from multiuse.aws_keys import get_secret

# %% codecell


class TD_Auth():
    """Authorize TD account to get access token."""

    """
    param: self.client_full : full client id
    param: self.refresh_token : refresh
    param: self.access_token : access expires in 30 minutes
    param: self.access_expiry : when the access token expires
    """

    def __init__(self):
        try:
            self._load_params(self)
        except KeyError:
            self._refresh_access_token(self)
            self._load_params(self)

        if datetime.now() > self.refresh_expiry:
            help_print_arg(f"TDMA refresh expiry: {str(self.refresh_expiry)}")
            self._refresh_access_token(self)
            self._load_params(self)

    @classmethod
    def _load_params(cls, self):
        """Load parameters for api calls."""
        load_dotenv()
        self.token_endpoint = 'https://api.tdameritrade.com/v1/oauth2/token'
        self.client_full = os.environ.get('tdma_client_id', False)
        self.refresh_token = os.environ.get('tdma_refresh_token', False)
        # If either of the above are false, request from AWS
        if not self.client_full or not self.refresh_token:
            secrets = get_secret()
            self.client_full = secrets['tdma_client_id']
            self.refresh_token = secrets['tdma_refresh_token']

        self.access_token = os.environ['tdma_access_token']
        self.refresh_expiry = (datetime.strptime(
                               os.environ['tdma_access_expires_at'],
                               '%Y-%m-%d %H:%M:%S'))

    @classmethod
    def _refresh_access_token(cls, self):
        """Get a new access token."""
        client = OAuth2Session(redirect_uri='https://127.0.0.1')
        tokens = (client.refresh_token(self.token_endpoint,
                  refresh_token=self.refresh_token,
                  client_id=self.client_full))
        # Print to console that we're getting a new auth token
        help_print_arg("TD_AUTH: Getting new access token")

        os.environ['tdma_access_token'] = tokens['access_token']
        os.environ['tdma_access_expires_at'] = (str(datetime.fromtimestamp(
                                                tokens['expires_at'])))
        # Write changes to local .env file
        dotenv_file = dotenv.find_dotenv()
        (dotenv.set_key(dotenv_file, "tdma_access_token",
                        os.environ["tdma_access_token"]))
        (dotenv.set_key(dotenv_file, "tdma_access_expires_at",
                        os.environ["tdma_access_expires_at"]))

# %% codecell
