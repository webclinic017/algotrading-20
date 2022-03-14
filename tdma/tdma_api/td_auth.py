"""Authentication for TD Ameritrade API."""
# %% codecell
import os
from datetime import datetime, timedelta

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
        self.dt_now = datetime.now()
        self._load_params(self)

        if self.dt_now > self.refresh_expiry:
            self._refresh_access_token(self)
            # self._load_params(self)

    @classmethod
    def _load_params(cls, self):
        """Load parameters for api calls."""
        load_dotenv()
        self.client_full = os.environ.get('tdma_client_id', False)
        self.refresh_token = os.environ.get('tdma_refresh_token', False)
        self.access_token = os.environ.get('tdma_access_token', False)
        # Set backup (default) that's already expired, trigger new refresh
        refresh_expiry_backup = (str((datetime.now() - timedelta(minutes=30))
                                 .replace(microsecond=0)))
        self.refresh_expiry = (datetime.strptime(
                               os.environ.get('tdma_access_expires_at',
                                              refresh_expiry_backup),
                               '%Y-%m-%d %H:%M:%S'))

    @classmethod
    def _refresh_access_token(cls, self):
        """Get a new access token."""
        secrets = get_secret()
        self.token_endpoint = 'https://api.tdameritrade.com/v1/oauth2/token'
        self.client_full = secrets['tdma_client_id']
        self.refresh_token = secrets['tdma_refresh_token']

        # Print to console that we're getting a new auth token
        help_print_arg("TD_AUTH: Getting new access token")

        client = OAuth2Session(redirect_uri='https://127.0.0.1')
        tokens = (client.refresh_token(self.token_endpoint,
                  refresh_token=self.refresh_token,
                  client_id=self.client_full))

        self.access_token = tokens['access_token']
        self.refresh_expiry = (str(datetime.fromtimestamp(
                                   tokens['expires_at'])))
        # Print to console the expiry date
        help_print_arg(f"TDMA refresh expiry: {self.refresh_expiry}")

        # Write changes to local .env file
        dotenv_file = dotenv.find_dotenv()
        (dotenv.set_key(dotenv_file, "tdma_access_token",
                        self.access_token))
        (dotenv.set_key(dotenv_file, "tdma_access_expires_at",
                        self.refresh_expiry))

# %% codecell
