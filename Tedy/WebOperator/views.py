#coding:utf-8
import datetime
import os,os.path
import json
import traceback
from dateutil.parser import parse
import flask
from flask import Flask,send_file
from flask import Response,request,redirect,url_for
from flask import render_template
from flask import Flask
# from elabs.fundamental.application import  instance
import pandas as pd
from bson import BSON
from bson.objectid import ObjectId
import base64
from app import  app

from flask import make_response,render_template
from functools import wraps, update_wrapper
# from elabs.fundamental.http.webapi import CallReturn,ErrorReturn
import pymongo
import nodeplot

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
cfgs = json.loads( open(FN).read())


def get_db_conn():
    conn = pymongo.MongoClient(**cfgs.get('mongodb'))
    return conn


def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers['Last-Modified'] = datetime.datetime.now()
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return update_wrapper(no_cache, view)

@app.route('/',methods=['GET'])
@nocache
def main():
    return render_template("main.html")


@app.route('/api/service_status',methods=['GET'])
@nocache
def get_service_status():
    conn = get_db_conn()
    coll = conn['RapidMarket']['service_keep_alive']
    rs = list(coll.find({},{'_id':False}).sort(
        # ('service_type',1),('service_id',1)
        'service_type',1
         ))
    for r in rs:
        r['timeout'] = int(abs((datetime.datetime.now() - r['uptime']).total_seconds()))
        r['now'] = str(r['now']).split('.')[0]
        r['start'] = str(r['start']).split('.')[0]
        r['uptime'] = str(r['uptime']).split('.')[0]

    jdata = json.dumps(rs)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp

# 查询报单服务列表
@app.route('/api/trade_service/list',methods=['GET'])
@nocache
def get_trade_service_list():
    conn = get_db_conn()
    coll = conn['RapidMarket']['service_keep_alive']
    rs = list(coll.find({'service_type':'trade'},{'_id':False,'service_type':1,'service_id':1}).sort(
        'service_id',1
         ))
    for r in rs :
        r['trade_id'] = r['service_id']
        r['name'] = r['service_id']

    jdata = json.dumps(rs)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp


# 查询 计算项目列表
@app.route('/api/tedy/projects', methods=['GET'])
@nocache
def get_project_names():
    conn = get_db_conn()
    db = conn['Task']
    names = db.list_collection_names()
    result = []
    for name in names:
        result.append(dict(id=name,name=name))
    jdata = json.dumps(result)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp

# 查询 任务参数组合 [2,10,10] 某一组参数
@app.route('/api/tedy/task_list', methods=['GET'])
@nocache
def get_task_list():
    project = request.values.get('project')
    conn = get_db_conn()
    db = conn['TaskEx']
    coll = db[project]
    rs = list(coll.find())
    result = []
    for r in rs:
        task = r['task']
        start = str(r['start'])
        end = str(r['end'])
        res = str(r['result'])
        data = dict(task = task , start = start , end = end, result = res)
        result.append(data)

    jdata = json.dumps(result)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp

# 查询 任务参数组合 [2,10,10] 某一组参数
@app.route('/api/tedy/worker_list', methods=['GET'])
@nocache
def get_worker_list():

    project = request.values.get('project')
    conn = get_db_conn()
    db = conn['Task']
    coll = db[project]
    rs = list(coll.find())
    result = []
    for r in rs:
        _id = str(r['_id'])
        node = str(r['node'])
        worker = str(r['worker'])
        task_list = str(r['task_list'])[:30]
        start = str(r['start'])
        end = str(r['end'])
        res = str(r['result'])
        err = str( r.get('error',''))

        data = dict(id=_id , node= node , worker = worker , task_list = task_list,start = start, end = end ,result = res ,error=err)
        result.append(data)

    jdata = json.dumps(result)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp


# 查询 监控主机运行状态
@app.route('/api/host_status', methods=['GET'])
@nocache
def get_host_run_status():
    conn = get_db_conn()
    db = conn['RapidMarket']
    coll = db['host-status']
    rs = list(coll.find())
    result = []
    for r in rs:
        ip = r['ip']
        img_mem = nodeplot.plot_host_mem_data(ip, figsize=(3, 1))
        img_uptime = nodeplot.plot_host_uptime(ip, figsize=(3, 1))
        data = dict(ip=r['ip'], cpu=r['cpu'] ,datetime= str(r['datetime']),
                    loads= f"{r['load_avg']['avg1']},{r['load_avg']['avg2']},{r['load_avg']['avg3']}" ,
                    mem = f"total:{ round(int(r['mem']['total'])/1024.,0) }G ,used:{ round(int(r['mem']['used'])/1024.,0) }G, free:{ round(int(r['mem']['free'])/1024.,0) }G",
                    swap = f"total:{ round(int(r['swap']['total'])/1024.,0) }G ,used:{ round(int(r['swap']['used'])/1024.,0) }G, free:{ round(int(r['swap']['free'])/1024.,0) }G",
                    root = f"total:{ r['root']['size'] } ,used:{ r['root']['used'] }, free:{ r['root']['avail']} , {r['root']['pc']}",
                    img_mem = img_mem,
                    img_uptime = img_uptime
                    )

        result.append(data)

    jdata = json.dumps(result)
    resp = Response(jdata)
    resp.headers['Content-Type'] = "application/json"
    return resp

