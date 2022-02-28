# %% codecell
import os
import requests
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.telegram.methods.helpers import TelegramHelpers
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from telegram.methods.helpers import TelegramHelpers
# %% codecell


def telegram_push_message(text, sec_forms=False, testing=False):
    """Push message to telegram chat."""
    load_dotenv()
    bot_api, chat_id = None, None
    if sec_forms:
        bot_api = os.environ.get('telegram_sec_bot')
        chat_id = os.environ.get('telegram_sec_chat_id')
    else:
        bot_api = os.environ.get('telegram_trade_bot')
        chat_id = os.environ.get('telegram_chat_of_id')

    if not text:
        print('Text needs to be supplied to the function')
        raise Exception

    u1 = 'https://api.telegram.org/bot'
    u2 = f"{u1}{bot_api}/sendMessage?chat_id={chat_id}&text={text}"

    rp = requests.post(u2)

    if sec_forms:
        TelegramHelpers.write_sec_msg_log(rp)
    if rp.status_code != 200:
        (help_print_arg(f"telegram_push_message: {str(rp.status_code)}"
                        f"{str(rp.content)} chat_id {str(chat_id)}"))

        return False
    if rp.status_code == 200:
        return rp


# %% codecell
