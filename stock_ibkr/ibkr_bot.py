# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 12:23:06 2023

@author: Jeffrey
"""

import logging
import threading
import time
from decimal import Decimal

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.order import Order
from ibapi.common import BarData, TickerId, TickAttrib
from ibapi.ticktype import TickType

import pandas as pd
import numpy as np


class IBKRBot(EClient, EWrapper):
    """
    Use Task Scheduler to start everyday

    ReqID
    Underlying Stock: 0-999
    Call Option: 1000-1999
    Put Option: >=2000
    Option Data: x5xx

    methods implemented in camelCase are overloaded from the api
    methods implemented in snake_case are utility functions

    potentially useful functions
    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
    def contractDetailsEnd(self, reqId):
    def openOrder(self, orderId, contract, order, orderState):
    def openOrderEnd(self):
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
    """

    def __init__(self, tickers: list):
        super().__init__(self)
        self._tickers = tickers
        self._tickers = list(map(lambda x: x.upper(), self._tickers))
        self._position = pd.DataFrame(
            columns=[
                "Account",
                "Ticker",
                "Contract ID",
                "SecType",
                "Strike",
                "Call/Put",
                "Expiry",
                "Currency",
                "Position",
                "Avg cost",
            ]
        )
        self.nextValidOrderId = -1
        self.simplePlaceOid = -1
        self.kline_data = {}  # OHLCV data
        self.last_price = {}  # stream tick data
        self.contract_details = {}  # {ticker:str : [ReqID, Contract object]}

        self.event = threading.Event()

        # initialisation
        for ticker in self._tickers:
            # create Contract object for each ticker
            self.contract_details[ticker] = [
                self._tickers.index(ticker),
                self._us_stock(ticker),
            ]

    def stream_data(self):
        """
        Utility function to stream data after trading bot is initialised
        """
        for ticker in self._tickers:
            # stream last price
            self.reqMktData(
                reqId=self._tickers.index(ticker),
                contract=self._us_stock(ticker),
                genericTickList="",
                snapshot=False,
                regulatorySnapshot=False,
                mktDataOptions=[],
            )
            time.sleep(1)

    def historicalData(self, reqId: int, bar: BarData):
        """
        wrapper function for reqHistoricalData. this function gives the candle historical data

        source: https://interactivebrokers.github.io/tws-api/historical_bars.html
                https://interactivebrokers.github.io/tws-api/historical_limitations.html
        """
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        if self._tickers[reqId] not in self.kline_data:
            self.kline_data[self._tickers[reqId]] = pd.DataFrame(
                {
                    "time": bar.date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                },
                index=[0],
            )
        else:
            self.kline_data[self._tickers[reqId]] = pd.concat(
                [
                    self.kline_data[self._tickers[reqId]],
                    pd.DataFrame(
                        {
                            "time": bar.date,
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": bar.volume,
                        },
                        index=[len(self.kline_data[self._tickers[reqId]])],
                    ),
                ]
            )

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """
        wrapper function call whenever historicalData() is called.
        To handle data downloaded into pd.DataFrame, index is utc time
        """
        super().historicalDataEnd(reqId, start, end)
        if reqId < 1000:  # Underlying Stocks
            if len(self.kline_data[self._tickers[reqId]]["time"][0].split()) > 1:
                # handle intraday data
                time_zone = self.kline_data[self._tickers[reqId]]["time"][0].split()[-1]
                self.kline_data[self._tickers[reqId]]["time"] = self.kline_data[
                    self._tickers[reqId]
                ]["time"].map(lambda x: x.rstrip(time_zone))
                self.kline_data[self._tickers[reqId]]["time"] = pd.DatetimeIndex(
                    self.kline_data[self._tickers[reqId]]["time"], tz=time_zone
                )
                self.kline_data[self._tickers[reqId]].set_index("time", inplace=True)
                self.kline_data[self._tickers[reqId]] = self.kline_data[
                    self._tickers[reqId]
                ].tz_convert("utc")

            else:
                # handle interday data
                self.kline_data[self._tickers[reqId]]["time"] = pd.to_datetime(
                    self.kline_data[self._tickers[reqId]]["time"], utc=True
                )
                self.kline_data[self._tickers[reqId]].set_index("time", inplace=True)

            logging.debug(
                "HistoricalDataEnd. ticker: %s from %s to %s",
                self._tickers[reqId],
                start,
                end,
            )
        self.event.set()

    def kline_download(self, durationStr: str, barSizeSetting: str):
        """
        Utility function to download kline for all tickers

        limitation: https://interactivebrokers.github.io/tws-api/historical_limitations.html
        interday data can download up to 20 years
        """
        for ticker in self._tickers:
            self.event.clear()
            self.reqHistoricalData(
                reqId=self.contract_details[ticker][0],
                contract=self.contract_details[ticker][1],
                endDateTime="",
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow="ADJUSTED_LAST",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[],
            )
            self.event.wait()

    def tickPrice(
        self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib
    ):
        """
        wrapper function for reqMktData. this function handles streaming market data  (last / current price)

        source: https://interactivebrokers.github.io/tws-api/md_receive.html
        tickType: https://interactivebrokers.github.io/tws-api/tick_types.html
        """
        super().tickPrice(reqId, tickType, price, attrib)
        if reqId < 1000:  # Underlying Stocks
            if tickType == 4:
                self.last_price[self._tickers[reqId]] = price

    @staticmethod
    def _us_stock(ticker) -> Contract:
        """
        Private utility function to create a Contract object
        """
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "ARCA"  # DO NOT SET TO SMART.
        contract.primaryExchange = "ARCA"
        return contract

    def position(
        self, account: str, contract: Contract, position: Decimal, avgCost: float
    ):
        """
        wrapper function for reqPositions. this function gives the current positions

        source: https://interactivebrokers.github.io/tws-api/positions.html
        """
        super().position(account, contract, position, avgCost)
        position = {
            "Account": str(account),
            "Ticker": str(contract.symbol),
            "Contract ID": str(contract.conId),
            "SecType": str(contract.secType),
            "Strike": float(contract.strike),
            "Call/Put": str(contract.right),
            "Expiry": str(contract.lastTradeDateOrContractMonth),
            "Currency": str(contract.currency),
            "Position": int(position),
            "Avg cost": float(avgCost),
        }

        idx = self._position[
            (self._position["Account"] == str(account))
            & (self._position["Contract ID"] == str(contract.conId))
        ].index

        if idx.empty:
            idx = len(self._position)
            self._position = pd.concat(
                [self._position, pd.DataFrame(position, index=[idx])]
            )

        else:
            idx = idx[0]
            self._position.update(pd.DataFrame(position, index=[idx]))

    def positionEnd(self):
        """
        wrapper function call whenever position() is called.

        source: https://interactivebrokers.github.io/tws-api/positions.html
        """
        super().positionEnd()
        logging.debug("PositionEnd")
        self.event.set()

    def balance(self) -> pd.DataFrame:
        """
        Utility function to obtain account balance of all tickers
        """
        self.event.clear()
        self.reqPositions()
        self.event.wait()
        return self._position

    def balance_single(self, ticker) -> int:
        """
        Utility function to return the current position of a single ticker
        """
        if ticker not in self._tickers:
            raise Exception("Ticker is not traded by this bot")

        balance = self.balance()
        if balance["Ticker"].str.contains(ticker).any():
            current_position = balance[balance["Ticker"] == ticker]["Position"].values[
                0
            ]
        else:
            current_position = 0
        return current_position

    def cancel_all_open_orders(self):
        """
        Utility function to cancel all open order

        source: https://interactivebrokers.github.io/tws-api/cancel_order.html
        """
        self.reqGlobalCancel()
        time.sleep(1)

    def error(
        self,
        reqId: TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ):
        """
        wrapper function for error handling. this function gives the format of showing error.

        source: https://interactivebrokers.github.io/tws-api/error_handling.html
        """
        # super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        logging.debug("Error. Id: %s Code: %s Mgs: %s", reqId, errorCode, errorString)
        if errorCode in [10197, 2104, 2106, 10147, 202]:
            pass
        elif errorCode == 200:
            logging.debug("No security definition has been found for the request")

    @staticmethod
    def _limit_order(side, quantity, limit_price) -> Order:
        """
        Private utility function to create limit order object

        source: https://interactivebrokers.github.io/tws-api/basic_orders.html#limitorder
        """
        order = Order()
        order.action = side
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        order.transmit = True
        return order

    @staticmethod
    def _market_order(side, quantity) -> Order:
        """
        Private utility function to create limit order object

        source: https://interactivebrokers.github.io/tws-api/basic_orders.html#limitorder
        """
        order = Order()
        order.action = side
        order.orderType = "MKT"
        order.totalQuantity = quantity
        order.transmit = True
        return order

    def nextValidId(self, orderId: int):
        """
        wrapper function for reqIds. this function manages the Order ID.

        source: https://interactivebrokers.github.io/tws-api/order_submission.html
        """
        super().nextValidId(orderId)
        # logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        logging.debug("NextValidId: %d", orderId)
        self.event.set()

    def place_order(
        self, ticker: str, side: str, type: str, quantity: float, price: float = None
    ):
        """
        Utility function to place order
        input:
            ticker
            side: {"BUY", "SELL"}
            type: {"MARKET", "LIMIT"}
            quantity
            price

        source: https://interactivebrokers.github.io/tws-api/order_submission.html
        """
        self.event.clear()
        self.reqIds(-1)
        self.event.wait()

        self.simplePlaceOid = self.nextValidOrderId

        if type == "MARKET":
            order = self._market_order(side, quantity)

        elif type == "LIMIT":
            order = self._limit_order(side, quantity, price)
        else:
            raise Exception("Wrong Order Type")

        self.placeOrder(self.simplePlaceOid, self.contract_details[ticker][1], order)
        time.sleep(1)

    def open_position(self, ticker: str, signal_now: int, unit_size: int, span: int):
        """
        This function is to open position

        input
            signal_now: signal t, position to adjust to (ie all integer are permitted)
            unit_size: position size in ticker per 1 signal (ie unit_size=100: 1signal => 100pos, 2signal => 200pos)
            ticker
            span: time span to make the change
        """
        holding_position = self.balance_single(ticker)
        holding_signal = holding_position / unit_size

        if signal_now == holding_signal:
            logging.debug("same signal")

        elif signal_now > holding_signal:
            # increase position
            signal_differece = signal_now - holding_signal
            self._execution_algo(
                ticker,
                target_size=signal_differece * unit_size,
                side="BUY",
                span=span,
            )

        elif signal_now < holding_signal:
            # decrease position
            signal_differece = signal_now - holding_signal
            self._execution_algo(
                ticker,
                target_size=signal_differece * unit_size,
                side="SELL",
                span=span,
            )

    def close_position(self, ticker: str):
        """
        This function is to close all position of a ticker immediately using limit order
        """
        holding_position = self.balance_single(ticker)

        if holding_position == 0:
            print("no opening position ")

        elif holding_position > 0:
            self._execution_algo(ticker, holding_position, "SELL", 1)

        elif holding_position < 0:
            self._execution_algo(ticker, holding_position, "BUY", 1)

    def _execution_algo(self, ticker: str, target_size: int, side: str, span: int):
        """
        Private utility function to control the mechanism to change position using TWAP
        span = 1: using limit order to make position change immediately

        input
            ticker
            target_size: unit of asset to change
            side: {'BUY', 'SELL'}
            span: time span in minute to make the change
        TWAP: https://empirica.io/blog/twap-strategy/
        """
        holding_position = self.balance_single(ticker)
        initial_position = holding_position
        target_per_minute = target_size / span

        # Set target_position
        if side == "BUY":
            target_position = holding_position + target_size
        elif side == "SELL":
            target_position = holding_position - target_size

        # TWAP
        for minute in range(1, span + 1):
            if side == "BUY":
                minute_target = initial_position + target_per_minute * minute
            elif side == "SELL":
                minute_target = initial_position - target_per_minute * minute

            start_time = time.time()

            while holding_position != minute_target:
                self.place_order(
                    ticker,
                    side,
                    "LIMIT",
                    abs(minute_target - holding_position),
                    self.last_price[ticker],
                )
                time.sleep(10)
                self.cancel_all_open_orders()
                holding_position = self.balance_single(ticker)
                time.sleep(1)

            if (
                holding_position == target_position
            ):  # exit loop if target_position reached
                break

            if start_time + 60 > time.time():  # wait for next minute to execute again
                time.sleep(start_time + 60 - time.time())

    @staticmethod
    def config_logging(logging, logging_level, log_file: str = None):
        """
        Utility function to configure logging to provide a more detailed log format, which includes date time in UTC
        input
            logging: python logging
            logging_level (int/str): For logging to include all messages with log levels >= logging_level. Ex: 10 or "DEBUG"
                                      logging level should be based on https://docs.python.org/3/library/logging.html#logging-levels
            log_file (str, optional): The filename to pass the logging to a file, instead of using console. Default filemode: "a"
        """
        logging.Formatter.converter = time.gmtime  # date time in GMT/UTC
        logging.basicConfig(
            level=logging_level,
            filename=log_file,
            format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def close(self):
        """
        Utility function to disconnect
        """
        self.disconnect()

    def set_connection(
        self, host: str = "127.0.0.1", port: int = 7497, clientId: int = 1
    ):
        """
        Utility function to setup connection
        '127.0.0.1' is the IP address that does not need to change
        Port: TWS Live = 7496; TWS Paper = 7497; IBGW Live = 4001; IBGW Paper = 4002
        clientId is the number of clients, max=32
        """
        self.connect(host=host, port=port, clientId=clientId)


# Utility Fucntions
def websocket_connection(trading_bot: IBKRBot, host="127.0.0.1", port=7497, clientId=2):
    """
    Establishing websocket connection
    """
    # configurate logging
    try:
        trading_bot.config_logging(
            logging, logging.DEBUG, log_file=f"{os.path.basename(__file__)[:-3]}.log"
        )
    except NameError:
        trading_bot.config_logging(logging, logging.DEBUG, log_file="debug.log")

    # setup_connection
    trading_bot.set_connection(host=host, port=port, clientId=clientId)

    # start
    trading_bot.run()
