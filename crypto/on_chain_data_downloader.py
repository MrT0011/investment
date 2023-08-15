# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 16:10:41 2022

@author: Jeffrey
"""

import json
import requests
import pandas as pd


# Overwriting loads() function in pd.read_json()
pd.io.json._json.loads = lambda s, *a, **kw: json.loads(s)


def galssnode_data_downloader(
    url: str, symbol: str = "BTC", frequency: str = "24h"
) -> pd.DataFrame:
    """
    download data from glassnode
    url: https://api.glassnode.com/v1/metrics/addresses/active_count
    asset symbol: BTC, ETH
    frequency interval: 1h, 24h, 10m
    """
    API_KEY = ""
    res = requests.get(
        url,
        params={"a": symbol, "api_key": API_KEY, "i": frequency, "f": "JSON"},
    )
    return pd.read_json(res.text, convert_dates=["t"]).set_index("t")


if __name__ == "__main__":
    df = galssnode_data_downloader(
        r"https://api.glassnode.com/v1/metrics/addresses/active_count"
    )

    df.plot()

    # Save to csv
    df.to_csv(r"D:\Glassnode Data\btc_active_addresses.csv")
