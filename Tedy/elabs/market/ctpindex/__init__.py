#coding:utf-8


"""
ctpindex
实现期货ctp指数行情接入

此服务运行在idc 平台侧，连接 31007 主机，转发到 公网mx服务器
"""
import threading,time,datetime,traceback,random,queue
import zmq
import pymysql
from dateutil.parser import  parse
from elabs.app.core.base import MarketBase
from elabs.app.core.message import KLine,Tick,OrderBook,ExchangeType,TradeType
from elabs.app.core.command import ServiceAlarmData,ExchangeSymbolUp
from elabs.app.core import logger
from elabs.app.core.controller import Controller
from elabs.utils.zmqex import init_keepalive
from elabs.utils.useful import Timer

class CtpIndexMarket(MarketBase):
    def __init__(self):
        MarketBase.__init__(self)
        self.ctx = zmq.Context()
        self.sock = None
        self.thr = None
        self.timer = None
        self.pull_q = queue.Queue()
        self.thr_pull = None
        self.snaps = {}   # { symbol:kline }
        self.mtx = threading.Lock()
        self.thr_shot = None

    def init(self,**kvs):
        MarketBase.init(self,**kvs)
        self.sock = self.ctx.socket(zmq.SUB)
        topic = self.cfgs.get('ctp_index_sub_topic','').encode()
        self.sock.setsockopt(zmq.SUBSCRIBE, topic)  # 订阅所有品种
        init_keepalive(self.sock)
        addr = self.cfgs.get('ctp_index_mx_addr','')
        self.sock.connect(addr)
        self.timer = Timer(self.onKeepAlive,self.cfgs.get('keepalive_interval',5))

    def onKeepAlive(self):
        Controller().keep_alive(**dict( info='kline recv okay'))

    def on_data_quotes(self,data):
        # print data
        fs = data.split(',')
        if len(fs) != 12:
            return
        #  symbol,time,xx,opeigh,low,close,vol,open_int,amount,xxx
        symbol, time, _,_, open,high, low, close, vol, open_int, amount, _ = fs

        symbol = symbol.upper()
        if symbol=='Y':
            symbol = 'YY'
        # if symbol not in self.ctx.get('SYMBOL_LIST',[]):
        #     return

        kline = KLine()
        kline.exchange = ExchangeType.CTP[1]
        kline.tt = TradeType.INDEX[1]
        kline.symbol = symbol
        kline.datetime = int(parse(time).timestamp() * 1000)
        kline.open = float(open)
        kline.high = float(high)
        kline.low = float(low)
        kline.close = float(close)
        kline.vol = float(vol)
        kline.amt = 0
        kline.opi = float(open_int)
        kline.transactions = 0
        kline.is_maker = 0
        kline.buy_vol = 0
        kline.buy_amt = 0

        with self.mtx:
            self.snaps[kline.symbol] = kline

        # Controller().onKline(kline)


    def snapshot_thread(self):
        while self.running:
            interval = self.cfgs.get('snapshot_interval',0.5)
            time.sleep(interval)
            with self.mtx:
                now = datetime.datetime.now().timestamp()
                for kline in self.snaps.values():
                    if (now - kline.datetime/1000) > 60:
                        continue
                    Controller().onKline(kline)


    def mx_recv_thread(self):
        self.running = True

        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        while self.running:
            events = dict(poller.poll(1000))
            try:
                if self.sock in events:
                    message = self.sock.recv_string()
                    chan, msg = message.split(" || ")
                    self.on_data_quotes(msg)
            except:
                traceback.print_exc()
            self.timer.kick()

    def open(self):
        self.running = True
        self.thr = threading.Thread(target=self.mx_recv_thread)
        self.thr.daemon = True
        self.thr.start()

        self.thr_pull = threading.Thread(target=self.klinepull_thread)
        self.thr_pull.daemon = True
        self.thr_pull.start()

        self.thr_shot = threading.Thread(target=self.snapshot_thread)
        self.thr_shot.daemon = True
        self.thr_shot.start()

    def klinepull_thread(self):
        while self.running:
            try:
                pair = self.pull_q.get(True,1)
                exchange, tt, symbol, start, end = pair
                self.do_pull(exchange, tt, symbol, start, end)
            except Exception as e:
                if isinstance(e,queue.Empty):
                    pass
                else:
                    traceback.print_exc( str(e))

    def do_pull(self,exchange,tt,symbol,start,end):
        start = start / 1000
        end = end / 1000
        seconds_per_day = 3600 * 24
        while start < end:
            s = datetime.datetime.fromtimestamp(start)
            e = datetime.datetime.fromtimestamp(start+seconds_per_day)
            start = start + seconds_per_day

            sleep_sec = self.cfgs.get('pull_sleep',0.1)
            if sleep_sec:
                time.sleep(sleep_sec)

            for kline in self.pull_from_db(symbol,s,e):
                Controller().onKlinePull(kline)

    def pull_from_db(self,symbol,start, end):
        result = []
        table_name = 'ctpindex'
        cnxn = pymysql.connect(**self.cfgs['INDEX_DB'])

        cursor = cnxn.cursor(pymysql.cursors.DictCursor)
        sql = "Select StockDate,O,H,L,C,V,OPI,D,T from " + table_name + " where Symbol = '" + symbol + "' and StockDate >= '{}' and StockDate < '{}'".format(
                str(start), str(end)) + " order by StockDate"
        try:
            print(sql)
            cursor.execute(sql)
        except Exception as e:
            print("Exception:", e)
            cnxn.close()
            return []

        data = cursor.fetchall()

        for d in data:
            kline = KLine()
            kline.exchange = ExchangeType.CTP[1]
            kline.tt = TradeType.INDEX[1]
            kline.symbol = symbol
            dt = d['StockDate']
            kline.datetime = int(dt.timestamp() * 1000)
            kline.open =  d['O']
            kline.high = d['H']
            kline.low = d['L']
            kline.close = d['C']
            kline.vol = d['V']
            kline.amt = 0
            kline.opi = d['OPI']
            kline.transactions = 0
            kline.is_maker = 0
            kline.buy_vol = 0
            kline.buy_amt = 0
            result.append(kline)
        cnxn.close()
        print("iterator total:",len(data))
        return result

    def close(self):
        self.running = False
        self.thr.join()

    def kline_pull(self,exchange,tt,symbol,start,end):
        """查询获取指定时间段的kline记录并返回
            exchange : 交易所 ftx
            tt： 交易类型  swap/spot
            symbol: 交易对
            start , end :  utc-timestamp  (ms)
        """
        print('issued:',exchange,tt,symbol,start,end)
        pair = [exchange,tt,symbol,start,end]
        self.pull_q.put_nowait(pair)




