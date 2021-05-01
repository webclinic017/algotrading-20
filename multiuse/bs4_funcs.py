"""Useful functions for using bs4 for extracting data."""
# %% codecell
#####################################################
# from bs4 import BeautifulSoup
import bs4.element

# %% codecell
#####################################################


def bs4_child_values(bs4_elm, elm=False, class_=False, id_=False, cols=False):
    """Get navigable strings in list from bs4 element."""
    # bs4 element, class should be string value. Returns list
    data = []
    for row in bs4_elm.find_all(elm, class_=class_):
        row_list = []
        for val in list(row.descendants):
            if isinstance(val, bs4.element.NavigableString):
                if val != '\n':
                    row_list.append(val)
        data.append(row_list)

    if cols:  # If data is used for column headers
        [data] = data
    return data
