"""Telegram Helpers."""
# %% codecell
import os
import requests

from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg

# %% codecell


class TelegramHelpers():
    """Helper functions for Telegram methods."""

    @staticmethod
    def telegram_set_webhook(hook_url=None):
        """Set telegram webhook."""
        load_dotenv()
        bot_api = os.environ.get('telegram_trade_bot')
        burl = 'https://api.telegram.org/bot'

        if not hook_url:
            hook_url = 'https://algotrading.ventures/api/v1/telegram/respond'

        url = f"{burl}{bot_api}/setWebhook?url={hook_url}"
        get = requests.get(url)

        if get.status_code == 200:
            gjson = get.json()['description']
            help_print_arg(f"telegram_set_webhook success: {str(gjson)}")
        else:
            gjson = get.json()
            help_print_arg(f"telegram_set_webhook error: {str(gjson)}")

    @staticmethod
    def telegram_check_status():
        """Check status of telgram webhook bot."""
        load_dotenv()
        burl = 'https://api.telegram.org/bot'
        bot_api = os.environ.get('telegram_trade_bot')
        url = f"{burl}{bot_api}/getWebhookInfo"

        get = requests.get(url)

        return get

# %% codecell
