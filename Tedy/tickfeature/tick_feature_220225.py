import sys
sys.path.append('/home/rpan/stock_alpha/')
from operators import *
import pandas as pd
import numpy as np
import os,os.path
from joblib import Parallel, delayed
from numpy_ext import rolling_apply
import warnings
from dateutil.relativedelta import relativedelta
import multiprocessing as mp
import time
import random
import uuid

data_root =  "/home/tedy/mnt/data.30"


tmp_path = f"{data_root}/temp"

# data_path = '/mnt/Data/rpan/l2_data_combined_tradingday_parquet/market_data/'

# data_path = os.path.join( data_root ,'rpan/market_data' )
data_path = os.path.join( data_root ,'/mnt/data7t/market_data' )
result_path = os.path.join(data_root,'rpan/factor_pool/tick_feature')
daily_data_path = os.path.join(data_path,'stock_data/daily')
try:
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
except:
    pass

# def tick_feature_day_ticker(arg,i,quantile,lookback):
#     aa = arg[i]
#     aa['datetime'] = pd.to_datetime(aa['date'].apply(str)+' '+aa['updatetime'].apply(str))
#     aa = aa.iloc[pd.DatetimeIndex(aa['datetime']).indexer_between_time('09:30','15:00')]
#     aa = aa.loc[aa.tradv>0]
#     if aa.empty:
#         pd.DataFrame().to_parquet(f'{result_path}/tmp_{lookback}_{quantile}/{i}.parquet')
#     else:
#         aa['spread'] = aa['ask1'] - aa['bid1']  # 买卖价差
#         aa['bs_spread_rate'] = ((aa['ask1'] - aa['bid1']) / aa['bid1']).where(aa['bid1'] > 0, np.nan)

#         aa['bid_tick_diff'] = (aa['bid1'] - aa['bid10'])/9
#         aa['ask_tick_diff'] = (aa['ask1'] - aa['ask10'])/9
#         aa['ba_tick_diff_ratio'] =  (aa['bid_tick_diff'] / aa['ask_tick_diff']).where(aa['ask_tick_diff'] > 0,np.nan)

#         aa['wb_price_diff'] = (aa['bid2'] + aa['bid3'] + aa['bid4'] + aa['bid5']) / aa['bid1'] / 4  # 委买挂单价格差异性，描述主动卖时难易程度
#         aa['wa_price_diff'] = (aa['ask2'] + aa['ask3'] + aa['ask4'] + aa['ask5']) / aa['ask1'] / 4  # 委卖挂单价格差异性，描述主动卖时难易程度
#         aa['wba_price_diff_ratio'] = aa['wb_price_diff'] / aa['wa_price_diff']

#         aa['bidv_std'] = aa[['bidv1', 'bidv2', 'bidv3', 'bidv4', 'bidv5', 'bidv6', 'bidv7', 'bidv8', 'bidv9', 'bidv10']].std(axis=1)  # 挂单量离散程度
#         aa['askv_std'] = aa[['askv1', 'askv2', 'askv3', 'askv4', 'askv5', 'askv6', 'askv7', 'askv8', 'askv9', 'askv10']].std(axis=1)
#         aa['avgbidv'] = aa[['bidv1', 'bidv2', 'bidv3', 'bidv4', 'bidv5', 'bidv6', 'bidv7', 'bidv8', 'bidv9', 'bidv10']].mean(axis=1)  # 挂单量平均值
#         aa['avgaskv'] = aa[['askv1', 'askv2', 'askv3', 'askv4', 'askv5', 'askv6', 'askv7', 'askv8', 'askv9', 'askv10']].mean(axis=1)

#         aa['bidv_std_mean'] = (aa['bidv_std'] / aa['avgbidv']).where(aa['avgbidv'] > 0, np.nan)  # 去除挂单总量后的挂单离散程度
#         aa['askv_std_mean'] = (aa['askv_std'] / aa['avgaskv']).where(aa['avgaskv'] > 0, np.nan)

#         aa['midpri'] = (aa['bid1'] + aa['askv1'])/2
        
#         aa['wavgmidpri1'] = (aa['bid1'] * aa['askv1'] + aa['ask1'] * aa['bidv1']) / (
#                 aa['askv1'] + aa['bidv1'])  # 根据最优价量加权的中间价
#         aa['wavgmidpri2'] = (aa['bid2'] * aa['askv2'] + aa['ask2'] * aa['bidv2']) / (
#                 aa['askv2'] + aa['bidv2'])  # 根据次优价量加权的中间价
#         aa['wap_diff'] = aa['wavgmidpri1'] - aa['wavgmidpri2']

#         aa['wavgmidpri1_ratio'] = (aa['wavgmidpri1']/aa['midpri']).where(aa['midpri']>0,np.nan)
#         aa['wavgmidpri2_ratio'] = (aa['wavgmidpri2']/aa['midpri']).where(aa['midpri']>0,np.nan)
        
#         aa['bidask_strength'] = ((aa['midpri'] - aa['wavgbidpri'])/(aa['wavgaskpri'] - aa['midpri'])).where(aa['wavgaskpri'] - aa['midpri']>0,np.nan)
        
#         aa['mainbidvol'] = (aa['bidv1'] + aa['bidv2'])  # 提供流动性买方力量
#         aa['mainbidvol_rate'] = aa['mainbidvol'] / aa['totalbidvol']  # 流动性买方所占比例
#         aa['mainaskvol'] = (aa['askv1'] + aa['askv2'])
#         aa['mainaskvol_rate'] = aa['mainaskvol'] / aa['totalaskvol']
#         aa['wbs_imbalance'] = aa['totalbidvol'] - aa['totalaskvol']
#         aa['totalwvol'] = aa['totalbidvol'] + aa['totalaskvol']
#         aa['wbs_imbalance_rate'] = (aa['wbs_imbalance']/aa['totalwvol']).where(aa['totalwvol']>0,np.nan)
#         aa['mainwvol'] = aa['mainbidvol'] + aa['mainaskvol']
#         aa['wbs_main_imbalance'] = aa['mainbidvol'] - aa['mainaskvol']
#         aa['wbs_main_imbalance_rate'] = (aa['wbs_main_imbalance']/aa['mainwvol']).where(aa['mainwvol']>0,np.nan)
#         aa['mainwvol_rate']=aa['mainbidvol']/aa['mainaskvol']
#         aa['totalwvol_rate']=aa['totalbidvol']/aa['totalaskvol']

#         aa['tradv_quantile'] = aa.groupby('securityid')['tradv'].transform('quantile',quantile)
#         aa = aa.loc[aa.tradv<aa.tradv_quantile]
#         pd.DataFrame(np.array([aa.groupby('securityid')['spread'].mean().values,aa.groupby('securityid')['bs_spread_rate'].mean().values,aa.groupby('securityid')['bid_tick_diff'].mean().values,
#                     aa.groupby('securityid')['ask_tick_diff'].mean().values,aa.groupby('securityid')['ba_tick_diff_ratio'].mean().values,aa.groupby('securityid')['wb_price_diff'].mean().values,
#                     aa.groupby('securityid')['wa_price_diff'].mean().values,aa.groupby('securityid')['wba_price_diff_ratio'].mean().values,aa.groupby('securityid')['bidv_std'].mean().values,
#                     aa.groupby('securityid')['askv_std'].mean().values,aa.groupby('securityid')['avgbidv'].mean().values,aa.groupby('securityid')['avgaskv'].mean().values,
#                     aa.groupby('securityid')['bidv_std_mean'].mean().values,aa.groupby('securityid')['askv_std_mean'].mean().values,aa.groupby('securityid')['wap_diff'].mean().values,
#                     aa.groupby('securityid')['wavgmidpri1_ratio'].mean().values,aa.groupby('securityid')['wavgmidpri2_ratio'].mean().values,aa.groupby('securityid')['bidask_strength'].mean().values,
#                     aa.groupby('securityid')['mainbidvol_rate'].mean().values,aa.groupby('securityid')['mainaskvol_rate'].mean().values,aa.groupby('securityid')['wbs_imbalance_rate'].mean().values,
#                     aa.groupby('securityid')['wbs_main_imbalance_rate'].mean().values,aa.groupby('securityid')['mainwvol_rate'].mean().values,aa.groupby('securityid')['totalwvol_rate'].mean().values]).T,
#                 index=aa.groupby('securityid')['spread'].mean().index,
#                 columns=['spread','bs_spread_rate','bid_tick_diff','ask_tick_diff','ba_tick_diff_ratio','wb_price_diff',
#                         'wa_price_diff','wba_price_diff_ratio','bidv_std','askv_std','avgbidv','avgaskv','bidv_std_mean',
#                         'askv_std_mean','wap_diff','wavgmidpri1_ratio','wavgmidpri2_ratio','bidask_strength','mainbidvol_rate',
#                         'mainaskvol_rate','wbs_imbalance_rate','wbs_main_imbalance_rate','mainwvol_rate','totalwvol_rate']
#                 ).to_parquet(f'{result_path}/tmp_{lookback}_{quantile}/{i}.parquet')

# def tick_feature_day(dfs,date,lookback,quantile,n_jobs):
#     with warnings.catch_warnings():
#         warnings.filterwarnings('ignore')
#         start = time.time()
#         df = pd.DataFrame()
#         for ddf in dfs:
#             df = df.append(ddf,ignore_index=True)
#         tickers = list(df.securityid.unique())
#         ticker_sets=[]
#         res = pd.DataFrame()
#         for i in range(n_jobs):
#             if i != n_jobs-1:
#                 ticker_sets.append(tickers[(i*(len(tickers)//(n_jobs))):(i*(len(tickers)//(n_jobs))+(len(tickers)//(n_jobs)))])
#             else:
#                 ticker_sets.append(tickers[(i*(len(tickers)//(n_jobs))):])
#         end = time.time()
#         print(end - start)
#         mgr = mp.Manager()
#         arg = mgr.list([df.loc[df.securityid.isin(ticker_sets[i])] for i in range(len(ticker_sets))])
#         start = time.time()
#         p = mp.Pool(n_jobs)
#         p.starmap(tick_feature_day_ticker,[(arg,i,quantile,lookback) for i in range(len(ticker_sets))])
#         # res = pd.DataFrame(np.array([x.values[j] for x in p.starmap(tick_feature_day_ticker,[(arg,i,quantile) for i in range(len(ticker_sets))]) for j in range(len(x))]),
#         #                     columns=['securityid','spread','bs_spread_rate','bid_tick_diff','ask_tick_diff','ba_tick_diff_ratio','wb_price_diff',
#         #                             'wa_price_diff','wba_price_diff_ratio','bidv_std','askv_std','avgbidv','avgaskv','bidv_std_mean',
#         #                             'askv_std_mean','wap_diff','wavgmidpri1_ratio','wavgmidpri2_ratio','bidask_strength','mainbidvol_rate',
#         #                             'mainaskvol_rate','wbs_imbalance_rate','wbs_main_imbalance_rate','mainwvol_rate','totalwvol_rate'])
#         for i in range(len(ticker_sets)):
#             res = res.append(pd.read_parquet(f'{result_path}/tmp_{lookback}_{quantile}/{i}.parquet'))
#         end = time.time()
#         print(end - start)
#         start = time.time()
#         res['datetime'] = pd.to_datetime(date,format='%Y%m%d')
#         end = time.time()
#         print(end - start)
#         return res.reset_index()

# def tick_feature(lookback,quantile,n_jobs):
#     dates_all = sorted(list(map(lambda x: x[:-8], [aa for aa in os.listdir('/mnt/Data/rpan/l2_data_combined_tradingday_parquet/market_data/') if 'parquet' in aa])))
#     dates = dates_all[(lookback-1+1000):]
#     dfs = []
#     res = []
#     close = pd.read_parquet(f'/mnt/Data/rpan/data/daily/closeprice.parquet')
#     for date in dates:
#         print(pd.to_datetime(date).strftime('%Y-%m-%d'))
#         dats = dates_all[(dates_all.index(date)-lookback+1):(dates_all.index(date)+1)]
#         if dfs:
#             del dfs[0]
#             dfs.append(pd.read_parquet(f'/mnt/Data/rpan/l2_data_combined_tradingday_parquet/market_data/{date}.parquet'))
#         else:
#             for dat in dats:
#                 dfs.append(pd.read_parquet(f'/mnt/Data/rpan/l2_data_combined_tradingday_parquet/market_data/{dat}.parquet'))
#         tmp = tick_feature_day(dfs,date,lookback,quantile,n_jobs)
#         res.append(tmp)
#     res = pd.concat(res,axis=0,ignore_index=True)
#     feature_cols = [ff for ff in res.columns if ff not in ['datetime','securityid']]
#     for feature in feature_cols:
#         res[['datetime','securityid',feature]].pivot(index='datetime',columns='securityid',values=feature).reindex(close.index,columns=close.columns).fillna(0).to_parquet(f'{result_path}/{feature}_lookback{lookback}_quantile{quantile}.parquet')

def tick_feature_day_ticker(ticker,dates,quantile):
    aa = pd.DataFrame()
    for date in dates:
        if os.path.exists(f'{data_path}/{date}/{ticker}.parquet'):
            aa = aa.append(pd.read_parquet(f'{data_path}/{date}/{ticker}.parquet'))
    if aa.empty:
        return pd.DataFrame()
    else:
        aa['datetime'] = pd.to_datetime(aa['date'].apply(str)+' '+aa['updatetime'].apply(str))
        aa = aa.iloc[pd.DatetimeIndex(aa['datetime']).indexer_between_time('09:30','15:00')]
        aa = aa.loc[aa.tradv>0]
        if aa.empty:
            return pd.DataFrame()
        aa['spread'] = aa['ask1'] - aa['bid1']  # 买卖价差
        aa['bs_spread_rate'] = ((aa['ask1'] - aa['bid1']) / aa['bid1']).where(aa['bid1'] > 0, np.nan)

        aa['bid_tick_diff'] = (aa['bid1'] - aa['bid10'])/9
        aa['bid_tick_diff_ratio'] = (((aa['bid1'] - aa['bid10'])/9) / aa['bid1']).where(aa['bid1'] > 0,np.nan)
        aa['ask_tick_diff'] = (aa['ask10'] - aa['ask1'])/9
        aa['ask_tick_diff_ratio'] = (((aa['ask10'] - aa['ask1'])/9) / aa['ask1']).where(aa['ask1'] > 0,np.nan)
        aa['ba_tick_diff_ratio'] =  (aa['bid_tick_diff'] / aa['ask_tick_diff']).where(aa['ask_tick_diff'] > 0,np.nan)

        aa['wb_price_diff'] = (aa['bid2'] + aa['bid3'] + aa['bid4'] + aa['bid5']) / aa['bid1'] / 4  # 委买挂单价格差异性，描述主动卖时难易程度
        aa['wa_price_diff'] = (aa['ask2'] + aa['ask3'] + aa['ask4'] + aa['ask5']) / aa['ask1'] / 4  # 委卖挂单价格差异性，描述主动卖时难易程度
        aa['wba_price_diff_ratio'] = aa['wb_price_diff'] / aa['wa_price_diff']

        aa['bidv_std'] = aa[['bidv1', 'bidv2', 'bidv3', 'bidv4', 'bidv5', 'bidv6', 'bidv7', 'bidv8', 'bidv9', 'bidv10']].std(axis=1)  # 挂单量离散程度
        aa['askv_std'] = aa[['askv1', 'askv2', 'askv3', 'askv4', 'askv5', 'askv6', 'askv7', 'askv8', 'askv9', 'askv10']].std(axis=1)
        aa['avgbidv'] = aa[['bidv1', 'bidv2', 'bidv3', 'bidv4', 'bidv5', 'bidv6', 'bidv7', 'bidv8', 'bidv9', 'bidv10']].mean(axis=1)  # 挂单量平均值
        aa['avgaskv'] = aa[['askv1', 'askv2', 'askv3', 'askv4', 'askv5', 'askv6', 'askv7', 'askv8', 'askv9', 'askv10']].mean(axis=1)

        aa['bidv_std_mean'] = (aa['bidv_std'] / aa['avgbidv']).where(aa['avgbidv'] > 0, np.nan)  # 去除挂单总量后的挂单离散程度
        aa['askv_std_mean'] = (aa['askv_std'] / aa['avgaskv']).where(aa['avgaskv'] > 0, np.nan)

        aa['midpri'] = (aa['bid1'] + aa['ask1'])/2
        
        aa['wavgmidpri1'] = (aa['bid1'] * aa['askv1'] + aa['ask1'] * aa['bidv1']) / (
                aa['askv1'] + aa['bidv1'])  # 根据最优价量加权的中间价
        aa['wavgmidpri2'] = (aa['bid2'] * aa['askv2'] + aa['ask2'] * aa['bidv2']) / (
                aa['askv2'] + aa['bidv2'])  # 根据次优价量加权的中间价
        aa['wamp_diff'] = aa['wavgmidpri1'] - aa['wavgmidpri2']
        aa['wamp_diff_ratio'] = (aa['wamp_diff']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri1_ratio'] = (aa['wavgmidpri1']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri2_ratio'] = (aa['wavgmidpri2']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri1_gap'] = (aa['wavgmidpri1']-aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri2_gap'] = (aa['wavgmidpri2']-aa['midpri']).where(aa['midpri']>0,np.nan)
        tmp_a = 0
        tmp_b = 0
        for i in range(1,11):
            tmp_a += aa[f'bid{i}'] * aa[f'askv{i}'] + aa[f'ask{i}'] * aa[f'bidv{i}']
            tmp_b += aa[f'askv{i}'] + aa[f'bidv{i}']
        aa['wavgmidpri'] = tmp_a/tmp_b
        aa['wavgmidpri_ratio'] = (aa['wavgmidpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri_gap'] = (aa['wavgmidpri']-aa['midpri']).where(aa['midpri']>0,np.nan)
        
        aa['wavgpri1'] = (aa['bid1'] * aa['bidv1'] + aa['ask1'] * aa['askv1']) / (
                aa['askv1'] + aa['bidv1'])  # 根据最优价量加权的中间价
        aa['wavgpri2'] = (aa['bid2'] * aa['bidv2'] + aa['ask2'] * aa['askv2']) / (
                aa['askv2'] + aa['bidv2'])  # 根据次优价量加权的中间价
        aa['wap_diff'] = aa['wavgpri1'] - aa['wavgpri2']
        aa['wap_diff_ratio'] = (aa['wap_diff']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri1_ratio'] = (aa['wavgpri1']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri2_ratio'] = (aa['wavgpri2']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri1_gap'] = (aa['wavgpri1']-aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri2_gap'] = (aa['wavgpri2']-aa['midpri']).where(aa['midpri']>0,np.nan)
        tmp_a = 0
        tmp_b = 0
        for i in range(1,11):
            tmp_a += aa[f'bid{i}'] * aa[f'bidv{i}'] + aa[f'ask{i}'] * aa[f'askv{i}']
            tmp_b += aa[f'askv{i}'] + aa[f'bidv{i}']
        aa['wavgpri'] = tmp_a/tmp_b
        aa['wavgpri_ratio'] = (aa['wavgpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri_gap'] = (aa['wavgpri']-aa['midpri']).where(aa['midpri']>0,np.nan)
        
        aa['wavbidask_ratio'] = (aa['wavgbidpri']*aa['totalaskvol'])/(aa['wavgaskpri']*aa['totalbidvol']).where(aa['wavgaskpri']*aa['totalbidvol']>0,np.nan)
        aa['wavbidask_gap'] = aa['wavgbidpri']*aa['totalaskvol'] - aa['wavgaskpri']*aa['totalbidvol']
        
        aa['bidask_strength'] = ((aa['midpri'] - aa['wavgbidpri'])/(aa['wavgaskpri'] - aa['midpri'])).where(aa['wavgaskpri'] - aa['midpri']>0,np.nan)
        
        aa['mainbidvol'] = (aa['bidv1'] + aa['bidv2'])  # 提供流动性买方力量
        aa['mainbidvol_rate'] = aa['mainbidvol'] / aa['totalbidvol']  # 流动性买方所占比例
        aa['mainaskvol'] = (aa['askv1'] + aa['askv2'])
        aa['mainaskvol_rate'] = aa['mainaskvol'] / aa['totalaskvol']
        aa['wbs_imbalance'] = aa['totalbidvol'] - aa['totalaskvol']
        aa['totalwvol'] = aa['totalbidvol'] + aa['totalaskvol']
        aa['wbs_imbalance_rate'] = (aa['wbs_imbalance']/aa['totalwvol']).where(aa['totalwvol']>0,np.nan)
        aa['mainwvol'] = aa['mainbidvol'] + aa['mainaskvol']
        aa['wbs_main_imbalance'] = aa['mainbidvol'] - aa['mainaskvol']
        aa['wbs_main_imbalance_rate'] = (aa['wbs_main_imbalance']/aa['mainwvol']).where(aa['mainwvol']>0,np.nan)
        aa['mainwvol_rate']=aa['mainbidvol']/aa['mainaskvol']
        aa['totalwvol_rate']=aa['totalbidvol']/aa['totalaskvol']

        aa['sweep_buy'] = (aa[[f'bid{i}' for i in range(1,11)]]>aa['ask1'].shift().values[:,None]).sum(axis=1)
        aa['sweep_sell'] = (aa[[f'ask{i}' for i in range(1,11)]]<aa['bid1'].shift().values[:,None]).sum(axis=1)
        
        aa['tradv_quantile'] = aa['tradv'].quantile(quantile)
        aa = aa.loc[aa.tradv<aa.tradv_quantile]
        if aa.empty:
            return pd.DataFrame()
        tmp = pd.DataFrame(np.array([[aa['spread'].mean(),aa['bs_spread_rate'].mean(),aa['bid_tick_diff'].mean(),aa['bid_tick_diff_ratio'].mean(),
                    aa['ask_tick_diff'].mean(),aa['ask_tick_diff_ratio'].mean(),aa['ba_tick_diff_ratio'].mean(),aa['wb_price_diff'].mean(),
                    aa['wa_price_diff'].mean(),aa['wba_price_diff_ratio'].mean(),aa['bidv_std'].mean(),
                    aa['askv_std'].mean(),aa['avgbidv'].mean(),aa['avgaskv'].mean(),
                    aa['bidv_std_mean'].mean(),aa['askv_std_mean'].mean(),aa['wamp_diff'].mean(),aa['wamp_diff_ratio'].mean(),
                    aa['wavgmidpri1_ratio'].mean(),aa['wavgmidpri2_ratio'].mean(),aa['wavgmidpri1_gap'].mean(),
                    aa['wavgmidpri2_gap'].mean(),aa['wavgmidpri_ratio'].mean(),aa['wavgmidpri_gap'].mean(),
                    aa['wap_diff'].mean(), aa['wap_diff_ratio'].mean(), aa['wavgpri1_ratio'].mean(),aa['wavgpri2_ratio'].mean(),
                    aa['wavgpri1_gap'].mean(),aa['wavgpri2_gap'].mean(),aa['wavgpri_ratio'].mean(),
                    aa['wavgpri_gap'].mean(),aa['wavbidask_ratio'].mean(),aa['wavbidask_gap'].mean(),aa['bidask_strength'].mean(),
                    aa['mainbidvol_rate'].mean(),aa['mainaskvol_rate'].mean(),aa['wbs_imbalance_rate'].mean(),
                    aa['wbs_main_imbalance_rate'].mean(),aa['mainwvol_rate'].mean(),aa['totalwvol_rate'].mean(),
                    aa['sweep_buy'].mean(),aa['sweep_sell'].mean()]]),
                index=[aa['securityid'].iloc[0]],
                columns=['spread','bs_spread_rate','bid_tick_diff','bid_tick_diff_ratio','ask_tick_diff','ask_tick_diff_ratio','ba_tick_diff_ratio',
                        'wb_price_diff','wa_price_diff','wba_price_diff_ratio','bidv_std','askv_std','avgbidv','avgaskv','bidv_std_mean',
                        'askv_std_mean','wamp_diff','wamp_diff_ratio','wavgmidpri1_ratio','wavgmidpri2_ratio','wavgmidpri1_gap','wavgmidpri2_gap',
                        'wavgmidpri_ratio','wavgmidpri_gap','wap_diff','wap_diff_ratio','wavgpri1_ratio','wavgpri2_ratio','wavgpri1_gap',
                        'wavgpri2_gap','wavgpri_ratio','wavgpri_gap','wavbidask_ratio','wavbidask_gap','bidask_strength','mainbidvol_rate',
                        'mainaskvol_rate','wbs_imbalance_rate','wbs_main_imbalance_rate','mainwvol_rate','totalwvol_rate','sweep_buy','sweep_sell']
                )
        tmp.index.name = 'securityid'
        return tmp.reset_index()

def tick_feature_day(dates,quantile,n_jobs):
    print(">> ",dates,quantile,n_jobs)
    print(".."*30)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        tickers = [x[:-8] for x in os.listdir(f'{data_path}/{dates[-1]}/')]
        res = pd.DataFrame()
        p = mp.Pool(n_jobs)
        res = pd.DataFrame(np.array([x.values[j] for x in p.starmap(tick_feature_day_ticker,[(tickers[i],dates,quantile) for i in range(len(tickers))]) for j in range(len(x))]),
                            columns=['securityid','spread','bs_spread_rate','bid_tick_diff','bid_tick_diff_ratio','ask_tick_diff','ask_tick_diff_ratio','ba_tick_diff_ratio',
                                    'wb_price_diff','wa_price_diff','wba_price_diff_ratio','bidv_std','askv_std','avgbidv','avgaskv','bidv_std_mean',
                                    'askv_std_mean','wamp_diff','wamp_diff_ratio','wavgmidpri1_ratio','wavgmidpri2_ratio','wavgmidpri1_gap','wavgmidpri2_gap',
                                    'wavgmidpri_ratio','wavgmidpri_gap','wap_diff','wap_diff_ratio','wavgpri1_ratio','wavgpri2_ratio','wavgpri1_gap',
                                    'wavgpri2_gap','wavgpri_ratio','wavgpri_gap','wavbidask_ratio','wavbidask_gap','bidask_strength','mainbidvol_rate',
                                    'mainaskvol_rate','wbs_imbalance_rate','wbs_main_imbalance_rate','mainwvol_rate','totalwvol_rate','sweep_buy','sweep_sell'])
        res['datetime'] = pd.to_datetime(dates[-1],format='%Y%m%d')
        # return res

        fn = f"{tmp_path}/{uuid.uuid4().hex}.parquet"
        res.to_parquet(fn)
        return fn  # write back to database

# def tick_feature(lookback,quantile,n_jobs):
#     dates_all = sorted(list(map(lambda x: x, [aa for aa in os.listdir(f'{data_path}') if 'parquet' not in aa])))
#     dates = [date for date in dates_all[1000:]]
#     random.shuffle(dates)
#     res = []
#     close = pd.read_parquet(f'{data_root}/stock_data/daily/closeprice.parquet')
#     for date in dates:
#         print(pd.to_datetime(date).strftime('%Y-%m-%d'))
#         dats = dates_all[(dates_all.index(date)-lookback+1):(dates_all.index(date)+1)]
#         tmp = tick_feature_day(dats,quantile,n_jobs)
#         res.append(tmp)
#     res = pd.concat(res,axis=0,ignore_index=True)
#     feature_cols = [ff for ff in res.columns if ff not in ['datetime','securityid']]
#     for feature in feature_cols:
#         res[['datetime','securityid',feature]].pivot(index='datetime',columns='securityid',values=feature).reindex(close.index,columns=close.columns).fillna(0).to_parquet(f'{result_path}/{feature}_lookback{lookback}_quantile{quantile}.parquet')

def project_finished():
    pass

def task_finished(task,result):
    """
        task: [ lookback,quantile ]
        result: [ worker 输出的文件名]  全部 worker 生成的数据文件名集合
    """
    print( task , result)

    # res = pd.concat(res, axis=0, ignore_index=True)
    # feature_cols = [ff for ff in res.columns if ff not in ['datetime', 'securityid']]
    # for feature in feature_cols:
    #     res[['datetime', 'securityid', feature]].pivot(index='datetime', columns='securityid', values=feature).reindex(
    #         close.index, columns=close.columns).fillna(0).to_parquet(
    #         f'{result_path}/{feature}_lookback{lookback}_quantile{quantile}.parquet')


def data_div(lookback,quantile) -> list:
    TASKNUM = 9
    data_root = "/Users/admin/mnt/30.4.1-tedy"
    data_path = os.path.join(data_root, 'rpan/market_data')

    dates_all = sorted(list(map(lambda x: x, [aa for aa in os.listdir(f'{data_path}') if 'parquet' not in aa])))
    dates = [date for date in dates_all[1132:]]
    random.shuffle(dates)

    # close = pd.read_parquet(f'{data_root}/stock_data/daily/closeprice.parquet')
    # for date in dates:
    #     print(pd.to_datetime(date).strftime('%Y-%m-%d'))
    #     dats = dates_all[(dates_all.index(date) - lookback + 1):(dates_all.index(date) + 1)]
    #     tmp = tick_feature_day(dats, quantile, n_jobs)
    #     res.append(tmp)

    # res = pd.concat(res, axis=0, ignore_index=True)
    # feature_cols = [ff for ff in res.columns if ff not in ['datetime', 'securityid']]
    # for feature in feature_cols:
    #     res[['datetime', 'securityid', feature]].pivot(index='datetime', columns='securityid', values=feature).reindex(
    #         close.index, columns=close.columns).fillna(0).to_parquet(
    #         f'{result_path}/{feature}_lookback{lookback}_quantile{quantile}.parquet')

    result = []
    for date in dates:
      dats = dates_all[(dates_all.index(date) - lookback + 1):(dates_all.index(date) + 1)]
      # result.append( [ list(dats),quantile])
      result.append( list(dats))
    # result = list( map( lambda _: _.tolist(),np.array_split(result,WORKERS)) )

    datas = np.array_split(result,TASKNUM)
    result = []
    for n, cols in enumerate(datas):
      result.append([cols.tolist()])       # [list(cols),p3,p4] => [p1,p2] + [list(cols),p3,p4] =>  p1,p2,[cols],p3,p4...
    return result


if __name__ == '__main__':
    mp.set_start_method('fork')
    for lookback in [1,3,5,10,20]:
        for quantile in [0.25,0.5]:
            tick_feature(lookback,quantile,n_jobs=128)