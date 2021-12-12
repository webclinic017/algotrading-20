"""Streaming from twitter for keyword extraction."""
# %% codecell
import requests
import pandas as pd

import numpy as np
from pathlib import Path

# %% codecell
t_key = 'wXFhPba867t8CBuTpcQjF1NxQ'
t_key_secret = 'OmW2qdZ7WuvA8nQoTVKDP17DfCwguQ6yQGJga0Qw8KTJZhSluO'

# %% codecell
from searchtweets import load_credentials, ResultStream, gen_rule_payload, collect_results

# %% codecell
creds = load_credentials(filename=".twitter_keys.yaml",
                 yaml_key="search_tweets_api",
                 env_overwrite=False)

# %% codecell
rule = gen_rule_payload("covaxin", results_per_call=100)
rule

# %% codecell

tweets = collect_results(rule, max_results=100, result_stream_args=creds)

# %% codecell


# %% codecell
