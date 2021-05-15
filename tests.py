"""Implement test classes."""
# %% codecell
###################################
import pandas as pd

try:
    from scripts.dev.testing.routine_tests import FpathsTest
except ModuleNotFoundError:
    from testing.routine_tests import FpathsTest

# %% codecell
###################################

tests = FpathsTest()
tests.sys_dict.items()

pd.DataFrame(tests.sys_dict.items(), columns=['system', 'status'])
# %% codecell
###################################
