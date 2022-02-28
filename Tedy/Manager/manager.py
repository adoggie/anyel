#coding:utf-8

"""
Manager　
接收各个服务发送的日志消息、心跳消息，写入数据库

"""
import os
import json
import threading
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd
import pymongo
import pytz
import zmq
from dateutil.parser import parse
from elabs.fundamental.utils.useful import singleton,input_params
# from elabs.fundamental.utils.timeutils import localtime2utc,utctime2local
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.command import ServiceKeepAlive,ServiceLogText,\
  ServiceAlarmData,ExchangeSymbolUp,\
  TradeEquityReport,TradePosReport,KlineUpdateReport,HostRunningStatus

from elabs.app.core.command import ServiceKeepAlive
from elabs.app.core.message import MxAlive

from elabs.utils.useful import Timer
from elabs.app.core.message import  *
from elabs.utils.zmqex import init_keepalive
from message_receiver import MessageReceiver


PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class Manager(Behavior,cmd.Cmd):

  prompt = 'Manager > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    self.running = False
    self.conn = None
    self.timer = None
    self.ctx = zmq.Context()
    self.sock = self.ctx.socket(zmq.SUB)
    init_keepalive(self.sock)

    self.running = False
    self.timer_mx_ka = None
    self.thread = None

  def init(self,**kvs):
    Behavior.init(self,**kvs)

    # if self.cfgs.get('registry_client.enable', 1):
    RegistryClient().init(**kvs)
    MessageReceiver().init(**kvs).addUser(self)

    interval = self.cfgs.get('keep_alive_interval',5)
    self.timer = Timer(self.keep_alive,interval)
    interval = self.cfgs.get('keep_alive_mx_broadcast_interval',5)
    self.timer_mx_ka = Timer(self.mx_keepalive_broadcast,interval)
    self.thread = threading.Thread( target=self.workThread)
    self.thread.start()

    return self

  def mx_keepalive_broadcast(self):
    """发送所有服务都接收的保活消息，以免服务长期接收不到订阅消息而离线"""
      # m = MxAlive()
    pass

  def workThread(self):
    self.running = True
    while self.running:
      time.sleep(0.1)
      self.timer.kick()
      # self.timer_mx_ka.kick()


  def keep_alive(self,**kvs):
    """定时上报状态信息"""
    Controller().keep_alive()

  def open(self):
    # if self.cfgs.get('registry_client.enable', 1):
    RegistryClient().open()
    MessageReceiver().open()
    return self

  def close(self):
    self.running = False
    self.thread.join()

  def onServiceKeepAlive(self, m: ServiceKeepAlive):
    """ serialize into file and nosql db """
    logger.debug("ServiceKeepAlive :", m.marshall())
    data = dict(
      service_type = m.service_type,
      service_id = m.service_id,
      pid = m.pid,
      start =  datetime.datetime.fromtimestamp( int(m.start)/1000) ,
      now =  datetime.datetime.fromtimestamp( int(m.now)/1000) ,
      params = m.params,
      ip = m.ip,
      tag = m.tag,
      uptime = datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceKeepAlive']
    coll = db[f"{m.service_type}_{m.service_id}"]
    coll.insert_one( data )
    coll.create_index([('uptime',1)])
    db = conn['RapidMarket']
    coll = db['service_keep_alive']
    if '_id' in data:
      del data['_id']
    coll.update_one(dict(service_type=data['service_type'],service_id = data['service_id']), {'$set': data}, upsert=True)

  def onServiceLogText(self,m: ServiceLogText):
    #logger.debug("ServiceLogText :", m.marshall())
    data = dict(
      service_type=m.from_service,
      service_id=m.from_id,
      timestamp =  datetime.datetime.fromtimestamp(m.timestamp/1000) ,
      level= m.level,
      text = m.text,
      uptime=datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceLogText']
    coll = db[f"{m.from_service}_{m.from_id}"]
    coll.insert_one(data)
    coll.create_index( [('uptime',1)])

  def onServiceAlarmData(self,m: ServiceAlarmData):
    logger.debug("onServiceAlarmData :", m.marshall())
    data = dict(
      service_type=m.from_service,
      service_id=m.from_id,
      timestamp =  datetime.datetime.fromtimestamp(m.timestamp/1000) ,
      type= m.type,
      level = m.level,
      tag = m.tag,
      detail = m.detail,
      data = m.data,
      uptime=datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceAlarmData']
    coll = db[f"{m.from_service}_{m.from_id}"]
    coll.insert_one(data)
    coll.create_index([('uptime',1)])


  def onHostRunningStatus(self,m:HostRunningStatus):
    logger.debug("onHostRunningStatus :", m.marshall())
    conn = self.db_conn()
    db = conn['RapidMarket']
    coll = db['host-status']
    data = m.data
    # print('upload node:',data['ip'],len(data['nodes']))
    if not data.get('ip'):
      print('Error: ip not defined!')
      return
    print(m.data)
    data['datetime'] = data['now']
    xdata = dict(ip=data['ip'],
                 mem=data['mem'],
                 swap=data['swap'],
                 # store_data=data['host']['store_data'],
                 # store_backup=data['host']['store_backup'],
                 load_avg=data['load_avg'],
                 datetime= parse(data['now']),
                 # node_num = len(data['nodes']),
                 # nodes = data['nodes'],
                 cpu = data['cpu'],
                 root = data['root'],
                 )
    now = xdata['datetime']
    coll.update_one(dict(ip=data['ip']), {'$set': xdata}, upsert=True)

    ip = str(data['ip']).replace('.', '_')
    coll = conn['HOST'][ip]
    coll.insert_one(xdata)
    coll.create_index( [("datetime", 1)])

  def db_conn(self):
    if not self.conn:
      self.conn = pymongo.MongoClient(**self.cfgs['mongodb'])
    return self.conn

  def do_exit(self,*args):
    Controller().close()
    print('bye bye!')
    return True

  def do_show(self,line):
    args = input_params(line,['pos'])
    if args:
     pass

  def signal_handler(signal, frame):
    sys.exit(0)

def signal_handler(signal,frame):
  Controller().close()
  print('bye bye!')
  sys.exit(0)

#------------------------------------------
FN = os.path.join(PWD,  'settings.json')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",Manager()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    Manager().cmdloop()


if __name__ == '__main__':
  run()
  # fire.Fire()