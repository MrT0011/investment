# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 13:44:54 2022

@author: Jeffrey
"""

# pylint: disable=line-too-long

# Binance price data
import os
import time
import concurrent.futures
import datetime
import zipfile
import glob
import wget

import pandas as pd

# Downlaod data
def binance_data_download(
    date: str, dtype: str = "klines", ticker: str = "BTCUSDT", interval: str = "1m"
):
    """
    download binance price data
    Remeber to make directory if it does not already exist

    ticker: str
    dtype: str {'aggtrades','klines','trades'}
    interval: str
    date = string date yyyy-mm-dd

    reference:
    https://github.com/binance/binance-public-data/#trades-1
    https://www.binance.com/en-NG/landing/data
    """
    if not os.path.exists(
        rf"D:\Binance Data\{dtype}\{ticker} - Unzipped\{ticker}-{interval}-{date}.csv"
    ):
        print(f"downloading {date} data...")
        url = f"https://data.binance.vision/data/spot/daily/{dtype}/{ticker}/{interval}/{ticker}-{interval}-{date}.zip"
        wget.download(url, rf"D:\Binance Data\{dtype}\{ticker}")
        time.sleep(1)
    else:
        print(f"{date} data already exist")


# Unzip files
def unzip(zip_directory: str):
    """
    Unzip file after downloaded price data
    """
    with zipfile.ZipFile(zip_directory, "r") as zip_ref:
        zip_ref.extractall(r"D:\Binance Data\klines\BTCUSDT - Unzipped")  # Output


if __name__ == "__main__":
    # Set start downloading dates
    now = datetime.date.today()
    start_date = datetime.date(2021, 3, 1)
    dates = pd.date_range(start=start_date, end=now, freq="D", normalize=True)
    dates = dates.strftime("%Y-%m-%d")

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        executor.map(binance_data_download, dates)

    # Unzip
    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        executor.map(unzip, glob.glob("D:/Binance Data/klines/BTCUSDT/*"))

    # # Read files
    # import os
    # import pandas as pd

    # PATH = r"D:\Binance Data\klines\BTCUSDT - Unzipped"
    # all_files = glob.glob(os.PATH.join(PATH, "*.csv"))

    # df = pd.concat(
    #     (
    #         pd.read_csv(
    #             f,
    #             names=[
    #                 "Open time",
    #                 "Open",
    #                 "High",
    #                 "Low",
    #                 "Close",
    #                 "Volume",
    #                 "Close time",
    #                 "Quote asset volume",
    #                 "Number of trades",
    #                 "Taker buy base asset volume",
    #                 "Taker buy quote asset volume",
    #                 "Ignore",
    #             ],
    #         )
    #         for f in all_files
    #     ),
    #     axis=0,
    #     ignore_index=True,
    #     verify_integrity=True,
    #     sort=False,
    # )

    # df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    # df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")
    # df.set_index("Open time", inplace=True)
