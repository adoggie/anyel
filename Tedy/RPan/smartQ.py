import sys
# sys.path.append('/home/rpan/stock_alpha/')
# from operators import *
import uuid

import pandas as pd
import numpy as np
import os
from joblib import Parallel, delayed
# from numpy_ext import rolling_apply
import warnings
from dateutil.relativedelta import relativedelta
import pyarrow.parquet as pq
import multiprocessing as mp
mp.set_start_method('fork')
import uuid

# result_path = '/mnt/Data/rpan/factor_pool/SmartQ/'
result_path = '/home/tedy/mnt/data.30.4/factor_pool/SmartQ'

try:
    os.mkdir('/dev/shm/tedyworker')
except:pass

if not os.path.exists(result_path):
    try:
        # os.removedirs('/dev/shm/tedyworker')
        # os.makedirs('/dev/shm/tedyworker')
        os.makedirs(result_path)
    except:pass


def sVWAPQ(volume,amount,rv_ratio):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        sorts = np.argsort(rv_ratio,axis=0)[::-1]
        volume = volume[sorts]
        amount = amount[sorts]
        threshold = np.nansum(volume,axis=0)*0.2
        cumsum = np.nancumsum(volume,axis=0)
        return (np.nansum(amount*(cumsum<threshold),axis=0)/np.nansum(volume*(cumsum<threshold),axis=0))/(np.nansum(amount,axis=0)/np.nansum(volume,axis=0))

def rollingSVWAPQ(volume,amount,rv_ratio,barsize,period):
    dates = pd.to_datetime(amount.index.to_series().dt.strftime('%Y-%m-%d').unique())
    return pd.Series(np.array([sVWAPQ(volume.loc[(volume.index>=dates[i])&(volume.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                amount.loc[(amount.index>=dates[i])&(amount.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                rv_ratio.loc[(rv_ratio.index>=dates[i])&(rv_ratio.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                ) for i in range(len(dates)-period+1)])
                                ,index=dates[(period-1):],name=amount.name).reindex(dates)

def SmartQ(mins,period,beta,n_jobs=1):
    print(f"worker start: [{ (mins,period,beta,n_jobs)}]")
    barsize = 240//mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        print("data reading..")
        print("read 1.")
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet').astype(np.float32)
        print("read 2.")
        amount = (pd.read_parquet(f'/data/stock_data/{path}/amount.parquet')*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 3.")
        volume = (pd.read_parquet(f'/data/stock_data/{path}/volume.parquet')*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 4.")
        openp = (pd.read_parquet(f'/data/stock_data/{path}/openprice.parquet')*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 5.")
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet')*if_jy).replace(0,np.nan).astype(np.float32)
    except:
        amount = pd.read_parquet(f'/data/stock_data/1min/amount.parquet').resample(f'{mins}T',label='right',closed='right').sum().replace(0,np.nan).dropna(how='all')
        volume = pd.read_parquet(f'/data/stock_data/1min/volume.parquet').resample(f'{mins}T',label='right',closed='right').sum().replace(0,np.nan).dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet').resample(f'{mins}T',label='right',closed='right').pad().reindex(amount.index).fillna(method='ffill')
        openp = (pd.read_parquet(f'/data/stock_data/1min/openprice.parquet').resample(f'{mins}T',label='right',closed='right').first().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')
        close = (pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet').resample(f'{mins}T',label='right',closed='right').last().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')
        amount = amount.reindex(openp.index)
        volume = volume.reindex(openp.index)
    print("read data end.")

    ret = (close/openp-1).replace([np.inf,-np.inf],np.nan)
    rv_ratio = ret.abs()/(volume**beta)
    print("Parallel start. ",n_jobs)

    res = pd.concat(Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(delayed(rollingSVWAPQ)(volume.iloc[:,i],amount.iloc[:,i],rv_ratio.iloc[:,i],barsize,period) for i in range(volume.shape[1])),axis=1).replace([np.inf,-np.inf,0],np.nan)
    print("Parallel end.")
    res.resample('D').last().dropna(how='all').to_parquet(f'{result_path}/SmartQ_beta{beta}_{mins}minute_lookback{period}.parquet')


# 1,3,0.25,['0001.xhg','0002.xhg'],0,40
def SmartQ_ex(mins,period,beta,column_names,idx,n_jobs=1):
    print(f"worker start: [{ (mins,period,beta,n_jobs)}]")
    barsize = 240//mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        print("data reading..")
        print("read 1.")
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet',columns=column_names).astype(np.float32)
        print("read 2.")
        amount = (pd.read_parquet(f'/data/stock_data/{path}/amount.parquet',columns=column_names)*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 3.")
        volume = (pd.read_parquet(f'/data/stock_data/{path}/volume.parquet',columns=column_names)*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 4.")
        openp = (pd.read_parquet(f'/data/stock_data/{path}/openprice.parquet',columns=column_names)*if_jy).replace(0,np.nan).astype(np.float32)
        print("read 5.")
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet',columns=column_names)*if_jy).replace(0,np.nan).astype(np.float32)
    except:
        print("data sythesizing..",mins)
        amount = pd.read_parquet(f'/data/stock_data/1min/amount.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').sum().replace(0,np.nan).dropna(how='all')
        volume = pd.read_parquet(f'/data/stock_data/1min/volume.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').sum().replace(0,np.nan).dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').pad().reindex(amount.index).fillna(method='ffill')
        openp = (pd.read_parquet(f'/data/stock_data/1min/openprice.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').first().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')
        close = (pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').last().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')
        amount = amount.reindex(openp.index)
        volume = volume.reindex(openp.index)
    print("read data end.")

    ret = (close/openp-1).replace([np.inf,-np.inf],np.nan)
    rv_ratio = ret.abs()/(volume**beta)
    print("Parallel start. ",n_jobs)
    res = pd.concat(Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all',backend='multiprocessing')(delayed(rollingSVWAPQ)(volume.iloc[:,i],amount.iloc[:,i],rv_ratio.iloc[:,i],barsize,period) for i in range(volume.shape[1])),axis=1).replace([np.inf,-np.inf,0],np.nan)
    print("Parallel end.")
    fn = f'{result_path}/SmartQ_ex_beta{beta}_{mins}minute_lookback{period}_{idx}.parquet'
    res.resample('D').last().dropna(how='all').to_parquet(fn)
    return fn


def work_process(mins, period, beta, column_names, q):
    barsize = 240 // mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        # print("data reading..")
        # print("read 1.")
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet', columns=column_names).astype(np.float32)
        # print("read 2.")
        amount = (pd.read_parquet(f'/data/stock_data/{path}/amount.parquet', columns=column_names) * if_jy).replace(0,
                                                                                                                    np.nan).astype(
            np.float32)
        # print("read 3.")
        volume = (pd.read_parquet(f'/data/stock_data/{path}/volume.parquet', columns=column_names) * if_jy).replace(0,
                                                                                                                    np.nan).astype(
            np.float32)
        # print("read 4.")
        openp = (pd.read_parquet(f'/data/stock_data/{path}/openprice.parquet', columns=column_names) * if_jy).replace(0,
                                                                                                                      np.nan).astype(
            np.float32)
        # print("read 5.")
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet', columns=column_names) * if_jy).replace(
            0, np.nan).astype(np.float32)
    except:
        print("data sythesizing..", mins)
        amount = pd.read_parquet(f'/data/stock_data/1min/amount.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                         label='right',
                                                                                                         closed='right').sum().replace(
            0, np.nan).dropna(how='all')
        volume = pd.read_parquet(f'/data/stock_data/1min/volume.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                         label='right',
                                                                                                         closed='right').sum().replace(
            0, np.nan).dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                        label='right',
                                                                                                        closed='right').pad().reindex(
            amount.index).fillna(method='ffill')
        openp = (pd.read_parquet(f'/data/stock_data/1min/openprice.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                            label='right',
                                                                                                            closed='right').first().dropna(
            how='all') * if_jy).replace(0, np.nan).dropna(how='all')
        close = (pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                             label='right',
                                                                                                             closed='right').last().dropna(
            how='all') * if_jy).replace(0, np.nan).dropna(how='all')
        amount = amount.reindex(openp.index)
        volume = volume.reindex(openp.index)
    # print("read data end.")

    ret = (close / openp - 1).replace([np.inf, -np.inf], np.nan)
    rv_ratio = ret.abs() / (volume ** beta)
    # print("Parallel start. ", n_jobs)

    for i in range(volume.shape[1]):
        res = rollingSVWAPQ( volume.iloc[:, i], amount.iloc[:, i], rv_ratio.iloc[:, i], barsize, period)
        # print('put result into q..')
        fn = f"/dev/shm/tedyworker/{uuid.uuid4().hex}.parquet"
        df = pd.DataFrame(res)
        df.to_parquet(fn)
        # print('write to ',fn)
        q.put(fn)
        # print('put okay!')

    # print("process ended!")


def _SmartQ_ex(mins,period,beta,column_names,idx,n_jobs=1):
    print(f"worker start: [{ (mins,period,beta,n_jobs)}]")
    column_names = np.array_split(column_names,n_jobs)
    q = mp.Queue()
    plist =  []
    for cols  in column_names:
        p = mp.Process(target=work_process, args=(mins,period,beta,cols,q))
        plist.append(p)
        p.start()


    for n,p  in enumerate(plist):
        # print('worker end join: --------------- ',n)
        p.join()
        # print(f"worker process end: {n}/len(plist)")
    datas = []
    while q.qsize():
        fn = q.get()
        df = pd.read_parquet(fn)
        datas.append(df)

    # print("Parallel end.")
    res = pd.concat( datas, axis=1).replace([np.inf, -np.inf, 0], np.nan)
    fn = f'{result_path}/SmartQ_ex_beta{beta}_{mins}minute_lookback{period}_{idx}.parquet'
    res.resample('D').last().dropna(how='all').to_parquet(fn)
    return fn


def data_div() -> list:
  """
  @return:
    [   ['00001.XHG , 00002.XHG ] ,
        ['00003.XHG , 00004.XHG ] ,
        ]

  """

  name = '../volume.parquet'
  pq_file = pq.ParquetFile(name)
  WORKERS = 9

  column_indices = range(len(pq_file.schema))
  # column_indices = range(100)   # 仅仅启用100列数据快速测试运行

  column_names = [pq_file.schema[i].name for i in column_indices]
  last = column_names[-1]
  if last =='time':
      column_names = column_names[:-1]
  column_names = np.array_split(column_names,WORKERS)
  result = []
  for n,cols in enumerate(column_names):
      result.append( [list(cols),n] )
  return  result

# ['0001.xhg',0002.xhg',0]
# ['0003.xhg',0004.xhg',1]

def project_finished():
    pass

def task_finished(task,result):
    pass



if __name__ == '__main__':
    mins_bin = [1,5,15]
    period_bin = [1,3,5,10,20,30]
    beta_bin = [0.25]
    for mins in mins_bin:
        for period in period_bin:
            for beta in beta_bin:
                print(f'{mins}minute(s)_{period}day(s)_beta{beta}')
                SmartQ(mins,period,beta,128)