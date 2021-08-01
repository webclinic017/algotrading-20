"""Helper functions for APIs."""
# %% codecell
#################################
import time
from datetime import datetime, timedelta

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg

# %% codecell
#################################


def rate_limit(func_to_execute, master_limit=0, duration=60, counter_limit=199, testing=False, **kwargs):
    """Rate limit an API."""
    counter = 0
    master_count = 0
    finished, sym, sym_list = False, False, False
    if 'symbol' in kwargs.keys():
        sym = kwargs['symbol']
    elif 'sym' in kwargs.keys():
        sym = kwargs['sym']
    elif 'sym_list' in kwargs.keys():
        sym_list = kwargs['sym_list']
        master_limit = len(sym_list)

    def sleep_if_hit(counter, next_min):
        """Program should sleep if rate limit is hit."""
        if (next_min <= datetime.now()) or (counter >= counter_limit):
            secs_to_sleep = (next_min - datetime.now()).seconds
            # print(f"Sleeping for {secs_to_sleep} seconds")
            time.sleep(secs_to_sleep)
            counter = 0
            next_min = datetime.now() + timedelta(seconds=duration)

        return counter, next_min

    while not finished:
        next_min = datetime.now() + timedelta(seconds=duration)
        if sym_list:
            for sym in sym_list:
                if testing:
                    help_print_arg(sym)
                counter += 1
                master_count += 1
                func_to_execute(sym)
                counter, next_min = sleep_if_hit(counter, next_min)

                if master_count < master_limit:
                    continue
                else:
                    finished = True
                    break
        else:
            if finished:
                break

            else:
                if sym:
                    func_to_execute(sym)
                else:
                    func_to_execute()
                counter += 1
                master_count += 1
        # print(counter)
