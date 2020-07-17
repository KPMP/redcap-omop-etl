import requests
import re
import sys
import datetime
import pandas as pd
import numpy as np


def time_transform(some_datetime, init_datetime):
    return (datetime.datetime(some_datetime) - datetime.datetime(init_datetime)).total_seconds()
