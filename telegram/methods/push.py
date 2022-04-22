# %% codecell
import os
import requests
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.telegram.methods.helpers import TelegramHelpers
    from scripts.dev.telegram.telegram_keys import return_telegram_keys
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from telegram.methods.helpers import TelegramHelpers
    from telegram.telegram_keys import return_telegram_keys
# %% codecell


def telegram_push_message(text, sec_forms=False, **kwargs):
    """Push message to telegram chat."""
    method = kwargs.get('method', 'testing')
    testing = kwargs.get('testing', None)
    return_on_failure = kwargs.get('return_on_failure')
    if sec_forms:
        method = 'sec_forms'

    bot_api, chat_id = return_telegram_keys(method, **kwargs)

    if not text:
        help_print_arg('Text needs to be supplied to the function')
        raise Exception
    if not bot_api or not chat_id:
        help_print_arg("No bot_api or chat_id passed to telegram_push_message")
        raise Exception
    if testing:
        help_print_arg(f"telegram_push_message: bot_api: {str(bot_api)} chat_id: {str(chat_id)}")

    u1 = 'https://api.telegram.org/bot'
    u2 = f"{u1}{bot_api}/sendMessage?chat_id={chat_id}&text={text}"

    rp = requests.post(u2)

    if sec_forms:
        TelegramHelpers.write_sec_msg_log(rp)
    if return_on_failure and rp.status_code != 200:
        return rp
    if rp.status_code != 200:
        (help_print_arg(f"telegram_push_message: {str(rp.status_code)}"
                        f"{str(rp.content)} chat_id {str(chat_id)}"))

        return False
    if rp.status_code == 200:
        return rp


# %% codecell
