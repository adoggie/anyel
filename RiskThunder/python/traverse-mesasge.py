#coding:utf-8

"""
ver,dest_service:dest_id,from_service:from_id,msg_type
1.0,market:001,manager:m01,status_query,plain,text
1.0,market:001,manager:m01,status_query,base64:json,
"""

import datetime,json,base64
import os
import time
import traceback

class Constant(object):
    DIRECTION_BUY = "buy"
    DIRECTION_SELL = "sell"
    OC_OPEN = "open"
    OC_CLOSE = "close"
    OC_CLOSET = "closeT"
    OC_CLOSEY = "closeY"

    MESSAGE_SEND_ORDER_REQ = "SEND_ORDER_REQ"
    MESSAGE_SEND_ORDER_RESP = "SEND_ORDER_RESP"
    MESSAGE_ON_TRADE = "ON_TRADE"
    MESSAGE_THUNDER_STATUS = "THUNDER_STATUS"
    MESSAGE_INFO_HY = "INFO_HY"
    MESSAGE_INFO_ORDER ="INFO_ORDER"

class TraverseMessage(object):
    Type = ''
    def __init__(self):
        self.ver = '1.0'
        self.dest_service = 'rulerisk_server' # 中心管理服务
        self.dest_id = ''
        self.from_service = ''
        self.from_id = ''
        self.timestamp = int(datetime.datetime.now().timestamp())
        self.msg_type = self.Type
        self.encode = 'base64'

    def set(self,**kvs):
        for k,v in kvs.items():
            if hasattr(self,k):
                setattr(self,k, v)
        return self

    def body(self):
        return ''

    def marshall(self):
        signature = 'nosig'
        text = f"{self.ver},{self.dest_service}:{self.dest_id},{self.msg_type}," \
               f"{self.from_service}:{self.from_id},{self.timestamp}," \
               f"{signature},{self.encode},{self.body()}"
        return text


class MessageSendOrderRequest(TraverseMessage):
    """仓位信号"""
    Type = Constant.MESSAGE_SEND_ORDER_REQ
    def __init__(self):
        TraverseMessage.__init__(self)
        self.account = ''
        self.refno = ''
        self.hyname = ''
        self.num = 0
        self.direction = ''
        self.openclose = ''
        self.price = 0

    def body(self):
        data = dict(account = self.account,
                    refno = self.refno,
                    hyname = self.hyname,
                    num = self.num,
                    direction = self.direction,
                    openclose = self.openclose,
                    price = self.price)

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.account = data.get('account','')
        m.refno = data.get('refno','')
        m.hyname = data.get('hyname','')
        m.num = data.get('num',0)
        m.direction = data.get('direction','')
        m.openclose = data.get('openclose','')
        m.price = data.get('price',0)
        return m

class MessageSendOrderResponse(TraverseMessage):
    """"""
    Type = Constant.MESSAGE_SEND_ORDER_RESP
    def __init__(self):
        TraverseMessage.__init__(self)
        self.rst =0
        # self.msg = []
        self.msg = ''
        self.account = ''
        self.refno = ''

    def body(self):
        data = dict(account = self.account,
                    rst = self.rst,
                    msg = self.msg,
                    refno = self.refno)

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.rst = data.get('rst', 0)
        m.msg = data.get('msg', '')
        m.account = data.get('account', '')
        m.refno = data.get('refno','')
        return m


class MessageOnTrade(TraverseMessage):
    """"""
    Type = Constant.MESSAGE_ON_TRADE
    def __init__(self):
        TraverseMessage.__init__(self)
        self.account = ''
        self.refno = ''
        self.hyname = ''
        self.tradenum = 0
        self.direction = ''
        self.openclose = '' #"open" "closet" "closey" "c

    def body(self):
        data = dict(account = self.account,
                    refno = self.refno,
                    hyname = self.hyname,
                    tradenum = self.tradenum,
                    direction = self.direction,
                    openclose = self.openclose
                    )

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.account = data.get('account','')
        m.refno = data.get('refno','')
        m.hyname = data.get('hyname','')
        m.tradenum = data.get('tradenum',0)
        m.direction = data.get('direction','')
        m.openclose = data.get('openclose','')
        return m


class MessageThunderStatus(TraverseMessage):
    """"""
    Type = Constant.MESSAGE_THUNDER_STATUS
    def __init__(self):
        TraverseMessage.__init__(self)
        self.account = ''
        self.status = ''  #  // b'-开启thunder   ‘h’--心跳
        self.date = 0
        self.time = 0

    def body(self):
        data = dict(account = self.account,
                    status = self.status,
                    date = self.date,
                    time = self.time
                    )

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.account = data.get('account','')
        m.status = data.get('status','')
        m.date = data.get('date',0)
        m.time = data.get('time',0)
        return m

class MessageInfoHY(TraverseMessage):
    """"""
    Type = Constant.MESSAGE_INFO_HY
    def __init__(self):
        TraverseMessage.__init__(self)
        self.account = ''
        self.hyname =''
        self.cancelnum = 0
        self.tlong = 0
        self.ylong = 0
        self.tshort = 0
        self.yshort = 0
        self.opennum = 0

    def body(self):
        data = dict(account = self.account,
                    hyname = self.hyname,
                    cancelnum = self.cancelnum,
                    tlong = self.tlong,
                    ylong = self.ylong,
                    tshort = self.tshort,
                    yshort = self.yshort,
                    opennum = self.opennum,
                    )

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.account = data.get('account','')
        m.hyname = data.get('hyname','')
        m.cancelnum = data.get('cancelnum',0)
        m.tlong = data.get('tlong',0)
        m.ylong = data.get('ylong',0)
        m.tshort = data.get('tshort',0)
        m.yshort = data.get('yshort',0)
        m.opennum = data.get('opennum',0)
        return m


class MessageInfoOrder(TraverseMessage):
    """"""
    Type = Constant.MESSAGE_INFO_ORDER
    def __init__(self):
        TraverseMessage.__init__(self)
        self.account = ''
        self.refno = ''
        self.hyname = ''
        self.num = 0
        self.direction =''
        self.openclose = ''
        self.price = 0
        self.status = ''

    def body(self):
        data = dict(account = self.account,
                    refno = self.refno,
                    hyname = self.hyname,
                    num = self.num,
                    direction = self.direction,
                    openclose = self.openclose,
                    price = self.price,
                    status = self.status,
                    )

        text = base64.b64encode(json.dumps(data).encode()).decode()
        return text

    @classmethod
    def parse(cls,data):
        m = cls()
        m.account = data.get('account','')
        m.refno = data.get('refno','')
        m.hyname = data.get('hyname','')
        m.num = data.get('num',0)
        m.ylong = data.get('ylong',0)
        m.direction = data.get('direction','')
        m.openclose = data.get('openclose','')
        m.price = data.get('price',0)
        m.status = data.get('status','')

        return m


MessageDefinedList = [
    MessageSendOrderRequest,
    MessageSendOrderResponse,
    MessageOnTrade,
    MessageThunderStatus,
    MessageInfoHY,
    MessageInfoOrder
]

def parseMessage(text):
    """解析消息报文"""
    if isinstance(text,bytes):
        text = text.decode()
    fs = text.split(',')
    if len(fs) < 8:
        return None
    ver ,dest,msg_type,from_,timestamp,signature,encode,body,*others = fs
    if ver !='1.0':
        return None
    m = None
    for md in MessageDefinedList:
        m = None
        try:
            if md.Type == msg_type:
                body = base64.b64decode(body)
                body = json.loads(body)
                # encs= encode.split(':')
                # for enc in encs:
                #     if enc == 'base64':
                #         body = base64.b64decode(body)
                #     elif enc == 'json':
                #         body = json.loads(body)

                m = md.parse(body)
                m.ver = ver
                m.dest_service, m.dest_id = dest.split(':')
                m.msg_type = msg_type
                m.from_service, m.from_id = from_.split(':')
                m.timestamp = int(float(timestamp))
                m.signature = signature
        except:
            traceback.print_exc()
            m = None
        if m:
            break
    return m

def test_serde():
    # txt = MessageSendOrderResponse().set(dest_id="rr01",from_id="thunder01",
    #                                     rst=1,
    #                                     refno='001',
    #                                     hyname='RB01',
    #                                     num =100,
    #                                     direction='buy',
    #                                     openclose='closey',
    #                                     price=0.1101).marshall()
    # print(txt)
    txt = '1.0,rulerisk_server:rrs001,SEND_ORDER_REQ,thunder:thunder-01,1645442001,nosig,base64,ewogICAiYWNjb3VudCIgOiAiYWJjMDAxIiwKICAgImRpcmVjdGlvbiIgOiAiYnV5IiwKICAgImh5bmFtZSIgOiAiUkIwMSIsCiAgICJuYW1lXyIgOiAiU0VORF9PUkRFUl9SRVEiLAogICAibnVtIiA6IDExLAogICAib3BlbmNsb3NlIiA6ICJvcGVuIiwKICAgInByaWNlIiA6IDkwMC4wMDIxMDAwMDAwMDAwNCwKICAgInJlZm5vIiA6ICIxMDAwMSIKfQo='
    txt = '1.0,rulerisk_server:rrs001,ON_TRADE,thunder:thunder-01,1645442331,nosig,base64,ewogICAiYWNjb3VudCIgOiAiYWJjMDAxIiwKICAgImRpcmVjdGlvbiIgOiAiYnV5IiwKICAgImh5bmFtZSIgOiAiUkIwMSIsCiAgICJuYW1lXyIgOiAiT05fVFJBREUiLAogICAib3BlbmNsb3NlIiA6ICJvcGVuIiwKICAgInJlZm5vIiA6ICIxMDAwMSIsCiAgICJ0cmFkZW51bSIgOiAxMQp9Cg=='
    txt ='1.0,rulerisk_server:rrs001,THUNDER_STATUS,thunder:thunder-01,1645442720,nosig,base64,ewogICAiYWNjb3VudCIgOiAiYWJjMDAxIiwKICAgImRhdGUiIDogMjAyMDExMDEsCiAgICJuYW1lXyIgOiAiVEhVTkRFUl9TVEFUVVMiLAogICAic3RhdHVzIiA6ICIxMDAwMSIsCiAgICJ0aW1lIiA6IDExCn0K'
    txt ='1.0,rulerisk_server:rrs001,INFO_HY,thunder:thunder-01,1645442954,nosig,base64,ewogICAiYWNjb3VudCIgOiAiYWJjMDAxIiwKICAgImNhbmNlbG51bSIgOiAxMSwKICAgImh5bmFtZSIgOiAiaHluYW1lIiwKICAgIm5hbWVfIiA6ICJJTkZPX0hZIiwKICAgIm9wZW5udW0iIDogMTYsCiAgICJ0bG9uZyIgOiAxMiwKICAgInRzaG9ydCIgOiAxNCwKICAgInlsb25nIiA6IDEzLAogICAieXNob3J0IiA6IDE1Cn0K'
    txt ='1.0,rulerisk_server:rrs001,INFO_ORDER,thunder:thunder-01,1645443081,nosig,base64,ewogICAiYWNjb3VudCIgOiAiYWJjMDAxIiwKICAgImRpcmVjdGlvbiIgOiAiYnV5IiwKICAgImh5bmFtZSIgOiAiaHluYW1lIiwKICAgIm5hbWVfIiA6ICJJTkZPX09SREVSIiwKICAgIm51bSIgOiAxMiwKICAgIm9wZW5jbG9zZSIgOiAib3BlbiIsCiAgICJwcmljZSIgOiAxNSwKICAgInJlZm5vIiA6ICIxMSIsCiAgICJzdGF0dXMiIDogImIiCn0K'

    txt = MessageSendOrderResponse().set(rst=1,msg='aaa——bbb',account='act001',refno='001').marshall()
    print(txt)

    m = parseMessage(txt)
    if isinstance(m,MessageInfoOrder):
        pass
    print(m.__dict__ )
    print(type(m))

test_serde()
