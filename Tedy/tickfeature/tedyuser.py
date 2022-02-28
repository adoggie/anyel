#coding:utf-8
import datetime
import time
from elabs.tedy.context import Context
from elabs.tedy.logger import INFO,DEBUG,WARN,ERROR,init as init_log
import tick_feature as this_project
import fire 

init_log()
# remote
def project_deploy(ctx:Context):
  """项目代码部署触发"""
  INFO("project deployed!")

# remote
def data_deploy(ctx:Context) :
  """数据部署触发"""
  INFO(">> data_deploy ..")


# local
# def task_split() -> list:
#   """任务切割"""
#   task_list = []
#   for lookback in [ 3,1, 5, 10, 20]:
#     for quantile in [0.25, 0.5]:
#       task_list.append([lookback, quantile])
#   return task_list

def task_split() -> list:
  """任务切割"""
  task_list = []
  for lookback in [ 3,1, 5, 10, 20]:
    task_list.append([lookback])
  return task_list

def data_div(task) -> list:
  return this_project.data_div(*task)

# remote
def computating( ctx:Context) :
  INFO(">> computing...")
  INFO("par n_cores:",ctx.ncores)
  result = {quantile:[] for quantile in [0.25,0.5,1]}
  fp = open(f"{ctx.project}.txt",'w')
  fp.write(f"task: {len(ctx.task_list)} \n")

  for n,args in enumerate(ctx.task_list):
    # args :   lookback,quantile,cols
    print(f"step: {n}/{len(ctx.task_list)} ")
    # ps = args[:-1]

    # for m,dat in enumerate(args[2]):
    #   ass = [dat, ps[1], ctx.ncores]
    ass = args[1:]
      # text = f"task:{n}/{len(ctx.task_list)} args: {m}/{len(args[2])} , { str(ass) }"
      # fp.write(str(datetime.datetime.now()) + " " + text+"\n")
      # fp.flush()

      # args =args[2:]
    ass.extend([ctx.ncores])
    print(ass)
    fp.write( str(ass))
    fp.write("\n")
    fp.flush()
    r = this_project.tick_feature(*ass)
    for i,quantile in enumerate([0.25,0.5,1]):
      result[quantile].append(r[i])
    # result.append(r) # returns is list
  fp.close()
  return result

def project_finished():
  this_project.project_finished()

def task_finished(task,result):
  """
    task : 一组参数组合
    result: worker 的计算输出返回 ,默认 worker运行结果的 'result 字段'（记录集文件名）
  """
  this_project.task_finished(task,result)

if __name__ == '__main__':
  fire.Fire() 

