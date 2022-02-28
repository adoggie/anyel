#coding:utf-8

import sys,os,os.path,traceback,time,datetime
import copy
import json
import toml
import fire
import pymongo
import zmq
from bson.objectid import ObjectId
from elabs.tedy import logger
from elabs.tedy import model

# from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
# from tinyrpc.transports.http import HttpPostClientTransport
# from tinyrpc import RPCClient

from tedy import tedy_settings

from elabs.fundamental.utils.importutils import import_function
from elabs.tedy.context import Context
from elabs.tedy.logger import *
from elabs.tedy.command import *

PWD = os.path.dirname(os.path.abspath(__file__))

def init_database(name=''):
  settings = tedy_settings()
  conn = pymongo.MongoClient(**settings['mongodb'])
  if not name:
    name = 'ConTedy'
  return conn[name]

logger.init()
settings = tedy_settings()

zctx = zmq.Context()
sock = zctx.socket(zmq.PUB)
addr = settings['default']['system_broker_addr_p']
sock.connect(addr)
time.sleep(0.1)

def init(prj_id):
  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  task_split(prj_id)
  INFO('Project Init Okay!')

def task_split(prj_id):
  """任务分割，一个worker对应多个任务"""
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']

  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  path = prj_dir
  sys.path.append(prj_dir)
  
  path = os.path.join(path,'cone-project.toml')
  configs = toml.load(open(path))
  imp_entry = configs.get('handlers').get('task_split')  # 加载运行触发入口

  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #

  task_list = fx()  # main.task_split(ctx) 项目任务切割

  db = init_database('Task')
  coll = db[prj_id]
  coll.delete_many({})

  all_workers = 0
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    all_workers+=node['workers']
  task_num = len(task_list)

  # num = task_num // all_workers
  # if task_num > num * all_workers:
  #   num+=1
  node_workers = []
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    node_workers.append([name,node['workers'],[]])

  while task_list:
    for nw in node_workers:
      for w in range(nw[1]):  # workers
        if not task_list:
          continue
        t = task_list.pop(0)  # task input args
        args = nw[2]
        args.append(t)

  for nw in node_workers:
    task_num = len(nw[2])
    workers = nw[1]

    tasks_per_worker = task_num//workers
    if task_num % workers:
      tasks_per_worker = (task_num+workers)//workers

    worker = 0
    while nw[2]:
      tasks = nw[2][:tasks_per_worker]
      data=dict(node= nw[0],
                worker = worker,
                task_list= tasks,
                start = None ,
                end = None
                )
      coll.insert_one(data)
      nw[2] = nw[2][tasks_per_worker:]
      worker+=1
    # 一个node存放多个N个task
  INFO('Project Task Split Okay!')

def project_finished(prj_id):
  """项目全部运行完成，触发数据聚合操作"""
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']

  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))
  imp_entry = cfgs.get('handlers').get('project_finished')  # 加载运行触发入口

  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #
  fx()  # 项目数据聚合


def task_split_ex(prj_id):
  """任务分割，一个worker对应多个任务"""
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']

  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  path = prj_dir
  sys.path.append(prj_dir)

  path = os.path.join(path, 'cone-project.toml')
  configs = toml.load(open(path))
  imp_entry = configs.get('handlers').get('task_split_ex')  # 加载运行触发入口

  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #

  task_list = fx()  # main.task_split(ctx) 项目任务切割

  db = init_database('TaskEx')
  coll = db[prj_id]
  coll.delete_many({})

  all_workers = 0
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    all_workers += node['workers']
  task_num = len(task_list)

  # num = task_num // all_workers
  # if task_num > num * all_workers:
  #   num+=1
  node_workers = []
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    node_workers.append([name, node['workers'], []])

  while task_list:
    for nw in node_workers:
      for w in range(nw[1]):  # workers
        if not task_list:
          continue
        t = task_list.pop(0)  # task input args
        args = nw[2]
        args.append(t)

  for nw in node_workers:
    task_num = len(nw[2])
    workers = nw[1]

    tasks_per_worker = task_num // workers
    if task_num % workers:
      tasks_per_worker = (task_num + workers) // workers

    worker = 0
    while nw[2]:
      tasks = nw[2][:tasks_per_worker]
      data = dict(node=nw[0],
                  worker=worker,
                  task_list=tasks,
                  start=None,
                  end=None
                  )
      coll.insert_one(data)
      nw[2] = nw[2][tasks_per_worker:]
      worker += 1
    # 一个node存放多个N个task
  INFO('Project Task Split Okay!')



def deploy( prj_id ,nodename=''):
  """部署指定项目到集群
    打包拷贝项目文件到计算节点
  """
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']
  
  prj_dir = os.path.join(repo_dir,prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!",prj_dir)
    return

  cfg_file = os.path.join(prj_dir,'cone-project.toml')
  cfgs = toml.load( open(cfg_file))
  
  for name in cfgs['default']['nodes']:
    node = settings['nodes'].get(name)
    if nodename and  nodename != name:

      continue
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {prj_dir} {node['ssh_user']}@{node['ip']}:{node['repo_dir']}/ "

    # cmd = "rsync -av -e 'ssh -p {port} -o StrictHostKeyChecking=no ' {prj_dir} {user}@{ip}:{repo}".\
    #   format(prj_dir=prj_dir,user= node['user'],port=node['port'],ip=node['ip'],repo = node['repo_dir'])
    print(cmd )
    os.system(cmd)

    elabs_dir = os.path.join(PWD, '../elabs')
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {elabs_dir} {node['ssh_user']}@{node['ip']}:{node['repo_dir']}/{prj_id}/ "
    print(cmd)
    os.system(cmd)

    m = ProjectDeploy()
    m.dest_service = "node"
    m.dest_id = "node"
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    sock.send(m.marshall().encode())


def data_deploy(prj_id, par=False):
  """拷贝项目的数据到集群目录
   par - 是否并行
  """
  settings = tedy_settings()
  
  # data_dir = settings['default']['data_dir']
  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  
  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  # DEBUG(cfg_file)
  cfgs = toml.load(open(cfg_file))

  data_dir = cfgs['default']['data_dir']
  for name in cfgs['default']['nodes']:
    node = settings['nodes'].get(name)

    #拷贝本地数据到计算节点
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {data_dir} {node['ssh_user']}@{node['ip']}:{node['data_dir']}/ "
    print(cmd)
    os.system(cmd)

    # notify node
    m = DataDeploy()
    m.dest_service = "node"
    m.dest_id = "node"
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    sock.send(m.marshall().encode())

def run(prj_id , wait='wait'):
  settings = tedy_settings()
  db = init_database('Task')
  if prj_id not in db.list_collection_names():
    logger.ERROR("Project not Found!",prj_id)
    return

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))
  
  for node in cfgs['default']['nodes']:
    # notify node
    m = ProjectRun()
    m.dest_service = "node"
    m.dest_id = node
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    bytes = m.marshall().encode()
    sock.send(bytes)
    print(bytes)

  if wait == 'nowait':
    return

  # 轮训db中的项目task运行状态
  nodes =0
  workers = 0
  finished = 0
  start = ''
  end = ''
  while True:
    time.sleep(1)
    n,w,f,start,end = status(prj_id,silent=True)
    if n != nodes or w !=workers or f!=finished:
      print(f"Nodes:{n}  Workers:{w}  Finished:{f}")
    if workers == finished:
      break

  print("Project Finished!")
  print(f"Project:{prj_id} Nodes:{nodes} Workers:{workers} start:{start}  end:{end}")
  project_finished(prj_id)


def run_ex(prj_id,just_init=False):
  # 增强型：先主体串行，分支分解并行
  fx_finish = project_finished

  settings = tedy_settings()
  db = init_database('Task')
  if prj_id not in db.list_collection_names():
    logger.ERROR("Project not Found!", prj_id)
    return

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  path = prj_dir
  sys.path.append(prj_dir)

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))

  #------------------------------
  imp_entry = cfgs.get('handlers').get('task_split_ex')  # 加载运行触发入口
  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #
  task_list = fx()  # main.task_split(ctx) 项目任务切割

  all_workers = 0
  for name in cfgs['default']['nodes']:
    node = settings.get('nodes')[name]
    all_workers += node['workers']

  db = init_database('Task')
  coll = db[prj_id]

  db = init_database('TaskEx')
  collex = db[prj_id]

  collex.delete_many({})
  for task in task_list:
    data = dict(
                task= task,
                start=None,
                end=None,
                result = None
                )
    collex.insert_one(data)

  rs = collex.find({})
  # for task in task_list:

  for r in rs:
    task = r['task']
    coll.delete_many({})
    imp_entry = cfgs.get('handlers').get('data_div')  #
    if not imp_entry:
      return

    # task 内切割数据
    fx_data_div = import_function(imp_entry)
    data_div_list = fx_data_div(task)
    # 分配切割的列到不同的worker
    node_workers = []
    for name in cfgs['default']['nodes']:
      node = settings.get('nodes')[name]
      node_workers.append([name, node['workers'], []])

    while data_div_list:
      for nw in node_workers: # 将task分配到 node节点
        for w in range(nw[1]):  # workers
          if not data_div_list:
            continue
          t = data_div_list.pop(0)  # task input args
          args = nw[2]
          args.append(t)

    for nw in node_workers: # 将节点的task任务分配到worker
      task_num = len(nw[2])  # 一个worker分配到多个task分组 task => [ 1,2,3,4] , [5,6,7]
      workers = nw[1]

      tasks_per_worker = task_num // workers
      if task_num % workers:
        tasks_per_worker = (task_num + workers) // workers

      worker = 0
      while nw[2]:
        div_data = nw[2][:tasks_per_worker] # multiple taskes per worker.
        task_list = []
        for div in list(div_data):
          task_list.append( task + div ) # [ 1, 0.25, [a,b,c] ]
        data = dict(node=nw[0],
                    worker=worker,
                    # task_list= task + list(div_data),
                    task_list= task_list,
                    start=None,
                    end=None,
                    result = None  # 一个worker包含串行执行的多个tasklet运行结果
                    )
        coll.insert_one(data)
        nw[2] = nw[2][tasks_per_worker:]
        worker += 1

    if just_init:
      print(f"Project {prj_id} inited okay!")
      return

    # 开始启动
    collex.update_one({'_id':r['_id']},{'$set':{'start':datetime.datetime.now()}},upsert=True)

    for node in cfgs['default']['nodes']:
      # notify node
      time.sleep(.5)
      m = ProjectRun()
      m.dest_service = "node"
      m.dest_id = node
      m.from_service = "admin"
      m.from_id = "admin"
      m.prj_id = prj_id
      bytes = m.marshall().encode()
      sock.send(bytes)
      print(bytes)

    # 等待项目task运行全部完成
    nodes = 0
    workers = 0
    finished = 0
    start = ''
    end = ''
    while True:
      time.sleep(1)
      n, w, f, start, end = status(prj_id, silent=True)
      if n != nodes or w != workers or f != finished:
        print(f"Nodes:{n}  Workers:{w}  Finished:{f}")
        nodes = n
        workers = w
        finished = f
      if workers == finished and workers:
        break

    # task 一组参数运行结束
    detail = get_task_detail(prj_id)
    task_id = r['_id']
    collex.update_one({'_id': r['_id']}, {'$set': {'end': datetime.datetime.now(),'detail':detail}}, upsert=True)
    # 所有 task的 切割任务已经完成
    imp_entry = cfgs.get('handlers').get('task_finished')  #
    if not imp_entry:
      return

    fx_task_fin = import_function(imp_entry)
    rs = coll.find({})
    result = []
    for r in rs:
      if r['result']:
        result.extend( r['result'])
      # 将所有worker的result 收集起来（均是计算结果)
    collex.update_one({'_id': task_id}, {'$set': {'result': result}}, upsert=True)
    fx_task_fin(task,result)

  project_finished(prj_id)
  print(f"Project : {prj_id} finished! {str(datetime.datetime.now())}")

def status_ex(prj_id):
  nodes = 0
  workers = 0
  finished = 0
  start = ''
  end = ''
  while True:
    time.sleep(1)
    n, w, f, start, end = status(prj_id, silent=True)
    if n != nodes or w != workers or f != finished:
      print(f"Nodes:{n}  Workers:{w}  Finished:{f}")
      nodes = n
      workers = w
      finished = f
    if workers == finished and workers:
      break


  # task 一组参数运行结束
  # collex.update_one({'_id': r['_id']}, {'$set': {'end': datetime.datetime.now()}}, upsert=True)
  # 所有 task的 切割任务已经完成
  settings = tedy_settings()
  db = init_database('Task')
  if prj_id not in db.list_collection_names():
    logger.ERROR("Project not Found!", prj_id)
    return

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  sys.path.append(prj_dir)

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))
  imp_entry = cfgs.get('handlers').get('task_finished')  #
  if not imp_entry:
    return

  db = init_database('Task')
  coll = db[prj_id]
  fx_task_fin = import_function(imp_entry)
  rs = coll.find({})
  result = []
  for r in rs:
    if r['result']:
      result.extend(r['result'])
    # 将所有worker的result 收集起来（均是计算结果)
  # fx_task_fin(task, result)

def stop(prj_id):
  settings = tedy_settings()

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))
  time.sleep(1)
  for node in cfgs['default']['nodes']:
    # notify node
    time.sleep(.5)
    m = ProjectStop()
    m.dest_service = "node"
    m.dest_id = node
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    bytes = m.marshall().encode()
    sock.send(bytes)
    print(bytes)


def list_prj():
  """ list all projects in registry."""
  db = init_database('Task')
  names = db.list_collection_names()
  for name in names:
    print(f"Project: {name}")


def status(prj_id ,silent=False ):
  """查询运行状态
    nodes,workers,finished，start,end
    节点数，workers数，完成workers数，开始，结束时间
  """
  settings = tedy_settings()

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))

  db = init_database('Task')
  coll = db[prj_id]
  rs = coll.find({})
  nodes = {}
  worker_finished = 0
  rs = list(rs)
  start_times = []
  end_times = []
  max_worker_time_cost = 0
  for r in rs:
    if r['start']:
      start_times.append(r['start'])
    if r['end']:
      end_times.append(r['end'])
    if r['start'] and r['end']:
      cost = (r['end'] - r['start']).total_seconds()
      max_worker_time_cost = max(cost,max_worker_time_cost)

    fin = 1 if r['end'] else 0
    if r['node'] not in nodes:
      nodes[r['node']] = dict(workers=1,task_list=len(r['task_list']))

    else:
      node = nodes[r['node']]
      node['workers']+=1
      node['task_list']+= len(r['task_list'])
    worker_finished += fin

  if not silent:
    print("-" * 20)
    print(f"project:{prj_id}")
    print(f"nodes:{len(nodes)} , workers:{len(rs)} , finished: {worker_finished}")
    # print(f"workers:{len(rs)} , Finished: {worker_finished}")

  start_times.sort()
  end_times.sort()
  start =''

  if start_times:
    start = start_times[0]
  # print(f"start: {str(start).split('.')[0]}")
  end = datetime.datetime.now()
  if end_times:
    end = end_times[-1]

  try:
    print(f"start: {str(start).split('.')[0]} , end: {str(end).split('.')[0]}")
    task_time_elapsed = round((end - start).total_seconds() / 60, 2)
    print(f"total task time: {task_time_elapsed} mins")
    print(f"max worker time: {round(max_worker_time_cost / 60, 2)} mins")
  except:
    pass
  print("-"*20)
  return len(nodes),len(rs),worker_finished,start,end


def get_task_detail(prj_id ):
  """查询任务执行过程细节
  """
  db = init_database('Task')
  coll = db[prj_id]
  rs = coll.find({})
  result = []
  for r in rs :
    data = dict( node = r['node'],
                 task_list = len(r['task_list']),
                 start = r['start'],
                 end = r['end'],
                 elapsed = round(( r['end']- r['start']).total_seconds() / 60.,2),
                 result = r['result']
                 )
    result.append( data )
  return result

def init_ex(prj_id):
  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  task_split_ex(prj_id)
  INFO('Project Init Okay!')


if __name__ == '__main__':
  fire.Fire()

  