"""SEC special dividend, local file parsing."""
# %% codecell
from pathlib import Path
import os
from tqdm import tqdm

import pandas as pd

# %% codecell

path = Path('../data/sec/filings_v2/')
txt_files = list(path.glob('**/*.txt'))

special_list = []
# %% codecell

# 'received cash proceeds of'

for f in tqdm(txt_files):

    with open(f, "rb") as bf:
        # Read bytes from file
        bf_file = bf.read()
        bf.close()

    get_splits = bf_file.decode('ISO-8859-1').split('<p')
    special = [sp.split('>')[1].split('>')[0] for sp in get_splits if 'special dividend' in sp]

    break_loop = False

    for val in special:
        if 'special dividend' in val:
            special_list.append(val)
            # break_loop = True


    if break_loop:
        break

# %% codecell
special_list

# %% codecell
split_sale_dict = {}

for f in tqdm(txt_files):

    with open(f, "rb") as bf:
        # Read bytes from file
        bf_file = bf.read()
        bf.close()

    # get_splits = bf_file.decode('ISO-8859-1').split('<p')
    splits = bf_file.decode('ISO-8859-1').split('announced today the closing')
    if len(splits) > 1:
        split_sale_dict[f] = splits


# %% codecell
sd_keys = list(split_sale_dict.keys())
sale_list = []
for p in sd_keys:
    specs_to_add = []
    for n in range(len(split_sale_dict[p])):
        if n % 2 == 0:
            specs_to_add.append(split_sale_dict[p][n][-200:])
        else:
            specs_to_add.append(split_sale_dict[p][n][:200])

    sale_list.append(specs_to_add)


# %% codecell
sale_list

# %% codecell
spec_list = []
split_dict = {}

for f in tqdm(txt_files):

    with open(f, "rb") as bf:
        # Read bytes from file
        bf_file = bf.read()
        bf.close()

    # get_splits = bf_file.decode('ISO-8859-1').split('<p')
    splits = bf_file.decode('ISO-8859-1').split('special dividend of')
    if len(splits) > 1:
        split_dict[f] = splits
    # special = [sp.split('>')[1].split('>')[0] for sp in get_splits if 'special dividend' in sp]
        special_div = [p.partition('special dividend of') for p in splits if 'special dividend' in p]

        if special_div:
            for n, val in enumerate(special_div):
                try:
                    specials = special_div[n].partition('special dividend')
                    specs = f"{specials[0][-75:]}{specials[1:][:75]}"
                    spec_list.append(specs)
                except AttributeError:
                    pass
                # print(len(special_div[n]))

    break_loop = False

    if break_loop:
        break


# %% codecell
sd_keys = list(split_dict.keys())
# %% codecell

sd_keys = list(split_dict.keys())
spec_list = []
for p in sd_keys:
    specs_to_add = []
    for n in range(len(split_dict[p])):
        if n % 2 == 0:
            specs_to_add.append(split_dict[p][n][-200:])
        else:
            specs_to_add.append(split_dict[p][n][:200])

    spec_list.append(specs_to_add)

# %% codecell

spec_list
specs = f"{specials[0][-75:]}{specials[1:][:75]}"
split_dict[sd_keys[1]]
spec_list

len(bf_file.decode('ISO-8859-1').split('special dividend'))
# %% codecell


special_div = [p.partition('special dividend of') for p in get_splits if 'special dividend' in p]
special_div

li = special_list[0].find('special dividend of')

specials = special_list[1].partition('special dividend')
spec_0 = specials[0][-75:]
spec_1 = specials[1:][:75]

specs = f"{specials[0][-75:]}{specials[1:][:75]}"
specs


spec_list = []
spec_list.append(specs)


# %% codecell
