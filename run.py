"""
A simple script that scrapes moneycontrol website to gather required data
"""

###############
# imports
###############

from bs4 import BeautifulSoup
from io import StringIO

import pandas as pd

import requests
import re
import hashlib
import schedule
import time


###############
# variables
###############

MONEY_CONTROL_URL = 'https://www.moneycontrol.com/stocks/marketstats/blockdeals/index.php'


def get_html(url) -> str:
    """
    given url of webpage, returns its html

    url: 
    return-value: html in str
    """
    return requests.get(url).text


def parse_data(html) -> dict:
    """
    given a html string, returns a parsed dict of required data from the page
    
    html: str
    return: dict of data
    """
    content = BeautifulSoup(html, 'html.parser')
    final_data = {}
    
    table = content.find_all('table')
    content_df = pd.read_html(StringIO(str(table)))[0]
    content_df.columns = ["Company Name", "Exchange", "Sector", "Quantity", "Price", "Value", "Time"]

    content_df['Company Name'] = content_df['Company Name'].map(lambda x: x.split(' Add to Watchlist')[0])
    contents_list = content_df.to_dict('records')

    for content in contents_list:
        final_data[hashlib.md5('|'.join([str(i) for i in content.values()]).encode("utf-8")).hexdigest()] = content
    
    print(final_data)


def schedule_job(interval, func, *args, **kwargs) -> None:
    """
    Given a func, calls it in interval time

    interval: duration in mins
    func: the function to call
    args: the args to pass to the func
    kwargs: the keyword args to pass to the func
    """
    schedule.every(interval).minutes.do(func, *args, **kwargs)


if __name__ == "__main__":

    print(f"Parsing data from URL: {MONEY_CONTROL_URL}")

    # call the schedule method
    html_string = get_html(MONEY_CONTROL_URL)
    schedule_job(10, parse_data, html_string)

    while True:
        schedule.run_pending()
        time.sleep(1)
