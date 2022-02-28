# coding:utf-8
import os, sys, os.path, time, datetime, traceback
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import base64
import json
from io import BytesIO
from bson.objectid import ObjectId
from dateutil.parser import parse
import fire

import pymongo
import config

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
cfgs = json.loads( open(FN).read())

def get_db_conn():
    conn = pymongo.MongoClient(**cfgs.get('mongodb'))
    return conn

# MONGODB = config.MONGODB


def plot_host_uptime(ip, st=None, et=None,
                     figsize=(10., 3.), grid=True,
                     save_file='abc.png',
                     axis_show=True):
    dbconn = get_db_conn()
    # collname = '{}-{}'.format(ip.replace('.', '_'), port)
    coll = dbconn['HOST'][ip.replace('.', '_')]
    now = datetime.datetime.now()
    if not st:
        st = now - datetime.timedelta(days=1)
    elif isinstance(st, str):
        st = parse(st)
    if not et:
        et = now
    elif isinstance(et, str):
        et = parse(et)
    times = []
    avgs = []

    rs = coll.find({'datetime': {'$gte': st, '$lt': et}}).sort('datetime', 1)
    rs = list(rs)
    for row in rs:
        times.append(row['datetime'])
        avgs.append( float(row['load_avg']['avg1']))

    plt.close()
    plt.rcParams['figure.figsize'] = figsize
    plt.grid(grid)
    plt.legend()
    plt.plot(times, avgs)
    if not axis_show:
        plt.xticks([])
        plt.yticks([])
        plt.axis('off')

    buffer = BytesIO()
    plt.savefig(buffer, format='jpeg')
    plot_data = buffer.getvalue()
    # ?matplotlib?????HTML
    imb = base64.b64encode(plot_data)  # ?plot_data????
    # imb = base64.urlsafe_b64decode(plot_data)  # ?plot_data????
    # base64.standard_b64decode()
    # ims = imb.decode()
    ims = imb
    imd = "data:image/jpg;base64," + ims.decode()
    # open(save_file, 'w').write(plot_data)
    return imd


def plot_host_mem_data(ip, st=None, et=None,
                       figsize=(10., 3.),
                       grid=True,
                       save_file='abc.png',
                       axis_show=True):
    dbconn = get_db_conn()
    coll = dbconn['HOST'][ip.replace('.', '_')]
    now = datetime.datetime.now()
    if not st:
        st = now - datetime.timedelta(days=1)
    elif isinstance(st, str):
        st = parse(st)
    if not et:
        et = now
    elif isinstance(et, str):
        et = parse(et)

    times = []
    mem_use = []

    rs = coll.find({'datetime': {'$gte': st, '$lt': et}}).sort('datetime', 1)
    rs = list(rs)
    for r in rs:
        try:
            size = int(r['mem']['total'])
            avail = int(r['mem']['free'])
            times.append(r['datetime'])
            used = size - avail
            percent = int(used / float(size) * 100)
            mem_use.append(percent)
        except:
            pass

    plt.close()
    plt.rcParams['figure.figsize'] = figsize
    plt.grid(grid)
    plt.legend()
    plt.plot(times, mem_use)

    if not axis_show:
        plt.xticks([])
        plt.yticks([])
        plt.axis('off')
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

    buffer = BytesIO()
    plt.savefig(buffer, format='jpeg')
    plot_data = buffer.getvalue()
    # ?matplotlib?????HTML
    imb = base64.b64encode(plot_data)  # ?plot_data????
    # imb = base64.urlsafe_b64decode(plot_data)  # ?plot_data????
    # ims = imb.decode()
    ims = imb
    imd = "data:image/jpg;base64," + ims.decode()
    # open(save_file, 'w').write(plot_data)
    # print(imd)
    return imd


if __name__ == '__main__':
    fire.Fire()