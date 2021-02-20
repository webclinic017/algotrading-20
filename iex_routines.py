"""
Daily IEX data requests to run.
"""
# %% codecell
##############################################
import os
import os.path

import json
from io import StringIO, BytesIO

import importlib
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import date, timedelta, time

try:
    from dev.help_class import baseDir
except ModuleNotFoundError:
    from help_class import baseDir

# %% codecell
##############################################
