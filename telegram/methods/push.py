# %% codecell
import os
import requests

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
# %% codecell


def telegram_push_message(text, testing=False):
    """Push message to telegram chat."""
    bot_api = os.environ.get('telegram_trade_bot')
    chat_id = os.environ.get('telegram_chat_of_id')

    if not text:
        print('Text needs to be supplied to the function')
        raise Exception

    u1 = 'https://api.telegram.org/bot'
    u2 = f"{u1}{bot_api}/sendMessage?chat_id={chat_id}&text={text}"

    rp = requests.post(u2)
    if rp.status_code != 200:
        (help_print_arg(f"telegram_push_message: {str(rp.status_code)}"
                        f"{str(rp.content)}"))
        return False
    if rp.status_code == 200:
        return rp


# %% codecell
