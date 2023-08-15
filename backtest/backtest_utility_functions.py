# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 12:30:13 2023

@author: Jeffrey
"""

import glob, os
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np


#################### Loading Price Data ####################
# Read price data
def read(ticker: str) -> pd.DataFrame:
    path = rf"D:\Binance Data\klines\{ticker} - Unzipped"
    all_files = glob.glob(os.path.join(path, "*.csv"))

    # Take / Maker, maker is liquidity provider limit order
    # https://dev.binance.vision/t/taker-buy-base-asset-volume/6026/2
    df = pd.concat(
        (
            pd.read_csv(
                f,
                names=[
                    "Open time",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Close time",
                    "Quote asset volume",
                    "Number of trades",
                    "Taker buy base asset volume",
                    "Taker buy quote asset volume",
                    "Ignore",
                ],
            )
            for f in all_files
        ),
        axis=0,
        ignore_index=True,
        verify_integrity=True,
        sort=False,
    )

    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")
    df.set_index("Open time", inplace=True)
    return df


btc = read("BTCUSDT")

# Plot data
btc[["Close", "Volume"]].plot(subplots=True)


#################### Change Resolution ####################
def resample(DF: pd.DataFrame, freq: str) -> pd.DataFrame:
    df = DF.copy()
    resample = df.resample(freq)
    o = resample.first()["Open"]
    h = resample.max()["High"]
    l = resample.min()["Low"]
    c = resample.last()["Close"]
    v = resample.sum()[["Volume"]]

    return pd.concat([o, h, l, c, v], axis=1)


consolidatedData = resample(btc, "1d")
consolidatedData["return"] = consolidatedData["Close"].pct_change()


#################### Load On Chain Data ####################
def load_on_chain_data(data_name: str) -> pd.DataFrame:
    path = rf"D:\Glassnode Data\{data_name}.csv"
    data = pd.read_csv(path)
    data["t"] = pd.to_datetime(data["t"])
    data.set_index("t", inplace=True)

    return data


#################### highlight signal on a graph ####################
def highlight_signal(price: pd.Series, signal: pd.Series, interactive: bool = True):
    """
    Take in pd.Series of asset price and signal and plot an interactive graph
    signal must be either 1 for long, -1 for short, np.nan for no signal
    """
    plot = price.copy()
    for i in [1, -1]:
        temp = signal[signal == i].index
        temp = price[price.index.isin(temp)]
        if i == 1:
            temp.rename("Long", inplace=True)
        elif i == -1:
            temp.rename("Short", inplace=True)
        plot = pd.concat([plot, temp], axis=1)
    plot.dropna(axis=1, how="all")

    # interactive mode
    # if interactive:
    #     %matplotlib
    fig, ax = plt.subplots(figsize=(30, 15))

    ax.scatter(
        plot.index, plot["Long"].tolist(), marker="o", c="g", alpha=1, label="Long"
    )
    ax.scatter(
        plot.index, plot["Short"].tolist(), marker="o", c="r", alpha=1, label="Short"
    )
    ax.plot(plot.index, plot["Close"].tolist())

    ax.legend(prop={"size": 30})

    plt.xlabel("Time", size=16)
    plt.ylabel("Close Price", size=16)
    plt.title("Price with highlighted Signals", size=16)
    plt.show()
