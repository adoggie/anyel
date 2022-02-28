#coding:utf-8

import os,os.path,sys,time,datetime,traceback,json,re
from dateutil.parser import parse
import fire
import requests
import zmq
from elabs.app.core.command import HostRunningStatus

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
# cfgs = json.loads( open(FN).read())
system_broker_addr_p = 'tcp://172.16.30.3:31010'

def send_command( m:HostRunningStatus):
    sock = None
    if not sock:
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PUB)
        addr = system_broker_addr_p
        sock.connect(addr)  # 发送
        time.sleep(.5)

    text = m.marshall().encode()
    sock.send(text)
    # print(f"send pull request: {exchange}_{tt}_{period}_{symbol} {s} - {e}")

def run(ip=''):
    m = HostRunningStatus()
    m.data = get_host_info()
    m.data['ip'] = ip
    send_command(m)
    return json.dumps(m.data)

def get_host_info(): 
    CMD ='export LANGUAGE="en_US:UTF-8";free -m'
    text = os.popen(CMD).read()
    text = text.replace('\n',' ')
    # res = re.findall("Mem:\s+(\d+)\s+(\d+)\s+(\d+).*Swap:\s+(\d+)\s+(\d+)\s+(\d+).*",text)
    res = re.findall("Mem:\s+(\d+)\s+(\d+).*\s+(\d+)\s+Swap:\s+(\d+)\s+(\d+)\s+(\d+).*",text)
    # print(text)
    d = res[0]
    result = dict(
        mem=dict(total=d[0],used=d[1],free=d[2]),
        swap=dict(total=d[3], used=d[4], free=d[5]),
        store_data='',
        store_backup=''
    )
    # print(result)

    CMD = 'uptime'
    # "19:57:22 up 5 days, 19:56,  2 users,  load average: 9.50, 5.86, 5.34"
    text = os.popen(CMD).read()
    # text = text.replace('\n', ' ')
    # print(text)
    res = re.findall(".+average:\s+(.+),\s+(.+),\s+(.+)", text)
    # print(res)
    d = res[0]
    result['load_avg'] = dict(
        avg1=d[0],avg2=d[1],avg3= d[2]
    )

    CMD = 'uptime -s'
    text = os.popen(CMD).read().strip()

    try:
        CMD = 'du -hs /'
        #text = os.popen(CMD).read().strip()
        #result['store_data'] = text.split()[0]
        CMD = "df -h | awk {'print $2,$3,$4,$5 , $6 '} | grep /$" # 根分区使用情况
        text = os.popen(CMD).read().strip()
        # result['root_data'] = text.split()[0]
        size,used,avail,pc = text.split()[:4]
        result['root'] = dict( size=size,used=used,avail=avail,pc=pc)
    except:
        traceback.print_exc()

    try:
        result['cpu'] = 1
        CMD = "export LANGUAGE=en_US:UTF-8;lscpu | grep '^CPU(s)'"
        text = os.popen(CMD).read().strip()
        cpu = text.split()[-1].strip()
        result['cpu'] = int(cpu)
    except:
        traceback.print_exc()
    result['now'] = str(datetime.datetime.now())
    return result

if __name__ == '__main__':
    fire.Fire()
