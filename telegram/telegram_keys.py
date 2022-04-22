"""Telegram keys for returning bot/chat ids."""
# %% codecell
import os

from dotenv import load_dotenv

# %% codecell


def return_telegram_keys(method, **kwargs):
    """Return keys, returning testing keys if true in kwargs."""
    load_dotenv()
    testing = kwargs.get('testing', False)
    bot_id, chat_id = False, False

    bot_dict = ({
            'testing': [
                os.environ.get('telegram_test_bot'),
                os.environ.get('telegram_test_chat_id')
            ],
            'trades': [
                os.environ.get('telegram_trade_bot'),
                os.environ.get('telegram_chat_of_id')
            ],
            'sec_forms': [
                os.environ.get('telegram_sec_bot'),
                os.environ.get('telegram_sec_chat_id')
            ]
    })

    ids = bot_dict[method]
    bot_id = ids[0]
    chat_id = ids[1]

    return bot_id, chat_id

# %% codecell
