#coding:utf-8

from typing import List
from elabs.app.core.base import TradeBase
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal,TradePosReport,TradeEquityReport
from elabs.app.core import logger
from elabs.app.core.controller import Controller


class MyTradeImpl(TradeBase):
    """简单演示的发单模块"""
    def __init__(self):
        TradeBase.__init__(self)

    def init(self,**kvs):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def onTick(self, tick: Tick):
        logger.debug("TradeImpl Got Tick: ", tick.marshall())

    def onKline(self, kline: KLine):
        logger.debug("TradeImpl Got KLine: ", kline.marshall())

    def onOrderBook(self, orderbook: OrderBook):
        logger.debug("TradeImpl Got OrderBook: ", orderbook.marshall())

    def onPositionSignal(self, pos: PositionSignal):
        logger.debug("TradeImpl Got PositionSignal: ", pos.marshall())

    def init_pos(self,positions:List[PositionSignal]):
        pass

    def onTimer(self,name):
        """发送交易仓位信息，发送权益信息"""
        m = TradePosReport()
        m = TradeEquityReport()
        Controller().send_message(m)
        print("onTimer..",name,m.marshall())