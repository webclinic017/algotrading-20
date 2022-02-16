"""Send Poll to Telegram."""
# %% codecell
import os
import json

import requests

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg

# %% codecell


def telegram_push_poll(tid, text, **kwargs):
    """Push message to telegram chat."""
    bot_api = os.environ.get('telegram_trade_bot')
    chat_id = os.environ.get('telegram_chat_of_id')

    # question = 'How accurate is this trade interpretation?'
    options = ['Yeah this works', 'Close miss', 'Totally off']

    query_dict = ({
        # 'id': tid,  # id of the tweet
        'chat_id': chat_id,
        'question': text,
        'options': json.dumps(options),
        # 'explanation': text,
        'total_voter_count': 0,
        'is_closed': False,
        'is_anonymous': True,
        'type': 'regular',
        'allows_multiple_answers': False,

    })

    u1 = f"https://api.telegram.org/bot{bot_api}/sendPoll"
    rp = requests.post(u1, params=query_dict)

    if rp.status_code != 200:
        (help_print_arg(f"telegram_push_message: {str(rp.status_code)}"
                        f"{str(rp.content)}"))
        if kwargs['testing']:
            print(rp.url)
            return rp
        else:
            return False
    if rp.status_code == 200:
        return rp


# %% codecell
