#coding:utf-8

import time
from elabs.tedy.context import Context
from elabs.tedy.logger import INFO,DEBUG,WARN,ERROR,init as init_log
import ideal_Variation 
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
def task_split() -> list:
  """任务切割"""
  task_list = []
  mins_bin = [1,5,15,30,240]
  period_bin = [3,5,10,20,30]
  for mins in mins_bin:
    for period in period_bin:
      task_list.append((mins,period))
  return task_list

def data_div(task) -> list:
  return ideal_Variation.data_div()

# remote
def computating( ctx:Context) :
  INFO(">> computing...")
  INFO("par n_cores:",ctx.ncores)
  result = []
  for n,args in enumerate(ctx.task_list):
    print(f"step: {n}/{len(ctx.task_list)} ",args)
    args.append(ctx.ncores)
    # r = ideal_Variation.ideal_variation(*args)
    r = ideal_Variation.ideal_variation_multiprocessing(*args)
    result.append(r)
  return result

def project_finished():
  ideal_Variation.project_finished()

def task_finished(task,result):
  """
    task : 一组参数组合
    result: worker 的计算输出返回
  """
  ideal_Variation.task_finished(task,result)
if __name__ == '__main__':
  fire.Fire() 

