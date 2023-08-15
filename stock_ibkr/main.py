# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 21:05:47 2023

@author: Jeffrey
"""
import threading
import time
from datetime import date

import strategies
from ibkr_bot import IBKRBot, websocket_connection


# Strategy
def signals_spy() -> int:
    """
    sum all signals together for SPY
    """
    cumu_signals = 0

    return cumu_signals


if __name__ == "__main__":
    bot = IBKRBot(tickers=[""])
    connection_thread = threading.Thread(
        target=websocket_connection, args=[bot, "127.0.0.1", 4001, 2], daemon=True
    )
    connection_thread.start()
    time.sleep(1)

    bot.stream_data()
    bot.kline_download("5 Y", "1 day")

    signal_today = signals_spy()
    print(str(date.today()) + "signal: " + str(signal_today))
    bot.open_position(ticker="SPY", signal_now=signal_today, unit_size=100, span=1)

    bot.close()
