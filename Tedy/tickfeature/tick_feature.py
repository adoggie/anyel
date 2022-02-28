import sys
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

def tick_feature_day_ticker(ticker,dates):
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
        aa['midpri'] = (aa['bid1'] + aa['ask1'])/2

        #订单失衡OFI因子

        for i in range(1,11):
            aa[f'last_bidv{i}'] = aa[f'bidv{i}'].shift()
            aa[f'last_bid{i}'] = aa[f'bid{i}'].shift()
            aa[f'last_askv{i}'] = aa[f'askv{i}'].shift()
            aa[f'last_ask{i}'] = aa[f'ask{i}'].shift()

            aa[f'delta_bidv{i}'] = aa[f'bidv{i}']
            aa.loc[aa[f'bid{i}']<aa[f'last_bid{i}'],f'delta_bidv{i}'] = -aa.loc[aa[f'bid{i}']<aa[f'last_bid{i}'],f'last_bidv{i}']
            aa.loc[aa[f'bid{i}']==aa[f'last_bid{i}'],f'delta_bidv{i}'] = aa.loc[aa[f'bid{i}']==aa[f'last_bid{i}'],f'bidv{i}']-aa.loc[aa[f'bid{i}']==aa[f'last_bid{i}'],f'last_bidv{i}']

            aa[f'delta_askv{i}'] = aa[f'askv{i}']
            aa.loc[aa[f'ask{i}']>aa[f'last_ask{i}'],f'delta_askv{i}'] = -aa.loc[aa[f'ask{i}']>aa[f'last_ask{i}'],f'last_askv{i}']
            aa.loc[aa[f'ask{i}']==aa[f'last_ask{i}'],f'delta_askv{i}'] = aa.loc[aa[f'ask{i}']==aa[f'last_ask{i}'],f'askv{i}']-aa.loc[aa[f'ask{i}']==aa[f'last_ask{i}'],f'last_askv{i}']

            aa[f'OFI{i}'] = aa[f'delta_bidv{i}'] - aa[f'delta_askv{i}']

        aa[f'MOFI_half'] = aa[[f'OFI{i}' for i in range(1,6)]].mean(axis=1)
        aa[f'MOFI_half_weighted'] = sum([aa[f'OFI{i}'].replace(np.nan,0)*i for i in range(1,6)])/sum(range(1,6))
        aa[f'MOFI'] = aa[[f'OFI{i}' for i in range(1,11)]].mean(axis=1)
        aa[f'MOFI_weighted'] = sum([aa[f'OFI{i}'].replace(np.nan,0)*i for i in range(1,11)])/sum(range(1,11))

        #订单斜率LogQuoteSlope因子
        for i in range(1,11):
            aa[f'LogQuoteSlope{i}'] = ((np.log(aa[f'ask{i}'])-np.log(aa[f'bid{i}']))/(np.log(aa[f'askv{i}'])+np.log(aa[f'bidv{i}']))).where((aa[f'askv{i}']>0)&(aa[f'bidv{i}']>0),np.nan).replace([np.inf,-np.inf],np.nan)

        aa[f'MLogQuoteSlope_half'] = aa[[f'LogQuoteSlope{i}' for i in range(1,6)]].mean(axis=1)
        aa[f'MLogQuoteSlope_half_weighted'] = sum([aa[f'LogQuoteSlope{i}'].replace(np.nan,0)*i for i in range(1,6)])/sum(range(1,6))
        aa[f'MLogQuoteSlope'] = aa[[f'LogQuoteSlope{i}' for i in range(1,11)]].mean(axis=1)
        aa[f'MLogQuoteSlope_weighted'] = sum([aa[f'LogQuoteSlope{i}'].replace(np.nan,0)*i for i in range(1,11)])/sum(range(1,11))

        #逐档订单失衡率SOIR因子
        for i in range(1,11):
            aa[f'SOIR{i}'] = ((aa[f'bidv{i}'] - aa[f'askv{i}'])/(aa[f'bidv{i}'] + aa[f'askv{i}'])).where(aa[f'bidv{i}'] + aa[f'askv{i}']>0,np.nan)

        aa[f'MSOIR_half'] = aa[[f'SOIR{i}' for i in range(1,6)]].mean(axis=1)
        aa[f'MSOIR_half_weighted'] = sum([aa[f'SOIR{i}']*i for i in range(1,6)])/sum(range(1,6))
        aa[f'MSOIR'] = aa[[f'SOIR{i}' for i in range(1,11)]].mean(axis=1)
        aa[f'MSOIR_weighted'] = sum([aa[f'SOIR{i}']*i for i in range(1,11)])/sum(range(1,11))

        #中间价变化率MPC
        aa['last_midpri'] = aa['midpri'].shift()
        aa[f'midpri_1min'] = aa['midpri'].shift(20*1)
        aa[f'midpri_5min'] = aa['midpri'].shift(20*5)
        aa[f'MPC3S'] = ((aa['midpri'] - aa[f'last_midpri'])/aa['last_midpri']).where(aa['last_midpri']>0,np.nan)
        aa[f'MPC1'] = ((aa['midpri'] - aa[f'midpri_1min'])/aa['midpri_1min']).where(aa['midpri_1min']>0,np.nan)
        aa[f'MPC5'] = ((aa['midpri'] - aa[f'midpri_5min'])/aa['midpri_5min']).where(aa['midpri_5min']>0,np.nan)



        #Other tick features
        aa['spread'] = ((aa['ask1'] - aa['bid1'])/(aa['ask1'] + aa['bid1'])).where(aa['bid1']+aa['ask1'] > 0, np.nan)  # 买卖价差
        aa['bs_spread_rate'] = ((aa['ask1'] - aa['bid1']) / aa['bid1']).where(aa['bid1'] > 0, np.nan)

        aa['bid_tick_diff'] = ((aa['bid1'] - aa['bid10'])/(aa['bid1'] + aa['bid10'])).where(aa['bid1'] + aa['bid10'] > 0,np.nan)
        aa['bid_tick_diff_ratio'] = (((aa['bid1'] - aa['bid10'])/9) / aa['bid1']).where(aa['bid1'] > 0,np.nan)
        aa['ask_tick_diff'] = ((aa['ask1'] - aa['ask10'])/(aa['ask1'] + aa['ask10'])).where(aa['ask1'] + aa['ask10'] > 0,np.nan)
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

        aa['wavgmidpri1'] = (aa['bid1'] * aa['askv1'] + aa['ask1'] * aa['bidv1']) / (
                aa['askv1'] + aa['bidv1'])  # 根据最优价量加权的中间价
        aa['wavgmidpri2'] = (aa['bid2'] * aa['askv2'] + aa['ask2'] * aa['bidv2']) / (
                aa['askv2'] + aa['bidv2'])  # 根据次优价量加权的中间价
        aa['wamp_diff'] = aa['wavgmidpri1'] - aa['wavgmidpri2']
        aa['wamp_diff_ratio'] = (aa['wamp_diff']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wamp_diff'] = (aa['wamp_diff']/(aa['wavgmidpri1'] + aa['wavgmidpri2'])).where(aa['wavgmidpri1'] + aa['wavgmidpri2']>0,np.nan)
        aa['wavgmidpri1_ratio'] = (aa['wavgmidpri1']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri2_ratio'] = (aa['wavgmidpri2']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri1_gap'] = ((aa['wavgmidpri1']-aa['midpri'])/(aa['wavgmidpri1']+aa['midpri'])).where(aa['wavgmidpri1']+aa['midpri']>0,np.nan)
        aa['wavgmidpri2_gap'] = ((aa['wavgmidpri2']-aa['midpri'])/(aa['wavgmidpri2']+aa['midpri'])).where(aa['wavgmidpri2']+aa['midpri']>0,np.nan)
        tmp_a = 0
        tmp_b = 0
        for i in range(1,11):
            tmp_a += aa[f'bid{i}'] * aa[f'askv{i}'] + aa[f'ask{i}'] * aa[f'bidv{i}']
            tmp_b += aa[f'askv{i}'] + aa[f'bidv{i}']
        aa['wavgmidpri'] = tmp_a/tmp_b
        aa['wavgmidpri_ratio'] = (aa['wavgmidpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmidpri_gap'] = ((aa['wavgmidpri']-aa['midpri'])/(aa['wavgmidpri']+aa['midpri'])).where(aa['wavgmidpri']+aa['midpri']>0,np.nan)
        
        aa['wavgpri1'] = (aa['bid1'] * aa['bidv1'] + aa['ask1'] * aa['askv1']) / (
                aa['askv1'] + aa['bidv1'])  # 根据最优价量加权的中间价
        aa['wavgpri2'] = (aa['bid2'] * aa['bidv2'] + aa['ask2'] * aa['askv2']) / (
                aa['askv2'] + aa['bidv2'])  # 根据次优价量加权的中间价
        aa['wap_diff'] = aa['wavgpri1'] - aa['wavgpri2']
        aa['wap_diff_ratio'] = (aa['wap_diff']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wap_diff'] = (aa['wap_diff']/(aa['wavgpri1'] + aa['wavgpri2'])).where(aa['wavgpri1'] + aa['wavgpri2']>0,np.nan)
        aa['wavgpri1_ratio'] = (aa['wavgpri1']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri2_ratio'] = (aa['wavgpri2']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri1_gap'] = ((aa['wavgpri1']-aa['midpri'])/(aa['wavgpri1']+aa['midpri'])).where(aa['wavgpri1']+aa['midpri']>0,np.nan)
        aa['wavgpri2_gap'] = ((aa['wavgpri2']-aa['midpri'])/(aa['wavgpri2']+aa['midpri'])).where(aa['wavgpri2']+aa['midpri']>0,np.nan)
        tmp_a = 0
        tmp_b = 0
        for i in range(1,11):
            tmp_a += aa[f'bid{i}'] * aa[f'bidv{i}'] + aa[f'ask{i}'] * aa[f'askv{i}']
            tmp_b += aa[f'askv{i}'] + aa[f'bidv{i}']
        aa['wavgpri'] = tmp_a/tmp_b
        aa['wavgpri_ratio'] = (aa['wavgpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgpri_gap'] = ((aa['wavgpri']-aa['midpri'])/(aa['wavgpri']+aa['midpri'])).where(aa['wavgpri']+aa['midpri']>0,np.nan)

        aa['wavgmainbidpri'] = ((aa['bid1']*aa['bidv1']+aa['bid2']*aa['bidv2'])/(aa['bidv1']+aa['bidv2'])).where(aa['bidv1']+aa['bidv2']>0,np.nan)
        aa['wavgmainbidpri_ratio'] = (aa['wavgmainbidpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmainbidpri_gap'] = ((aa['wavgmainbidpri']-aa['midpri'])/(aa['wavgmainbidpri']+aa['midpri'])).where(aa['wavgmainbidpri']+aa['midpri']>0,np.nan)
        aa['wavgbidpri_ratio'] = (aa['wavgbidpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgbidpri_gap'] = ((aa['wavgbidpri']-aa['midpri'])/(aa['wavgbidpri']+aa['midpri'])).where(aa['wavgbidpri']+aa['midpri']>0,np.nan)
        aa['wavgmainaskpri'] = ((aa['ask1']*aa['askv1']+aa['ask2']*aa['askv2'])/(aa['askv1']+aa['askv2'])).where(aa['askv1']+aa['askv2']>0,np.nan)
        aa['wavgmainaskpri_ratio'] = (aa['wavgmainaskpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgmainaskpri_gap'] = ((aa['wavgmainaskpri']-aa['midpri'])/(aa['wavgmainaskpri']+aa['midpri'])).where(aa['wavgmainaskpri']+aa['midpri']>0,np.nan)
        aa['wavgaskpri_ratio'] = (aa['wavgaskpri']/aa['midpri']).where(aa['midpri']>0,np.nan)
        aa['wavgaskpri_gap'] = ((aa['wavgaskpri']-aa['midpri'])/(aa['wavgaskpri']+aa['midpri'])).where(aa['wavgaskpri']+aa['midpri']>0,np.nan)

        aa['DolVol_ask'] = (aa[[f'askv{i}' for i in range(1,11)]]*aa[[f'ask{i}' for i in range(1,11)]]).sum(axis=1)
        aa['DolVol_bid'] = (aa[[f'bidv{i}' for i in range(1,11)]]*aa[[f'bid{i}' for i in range(1,11)]]).sum(axis=1)

        aa['MCI_ask'] = ((aa['wavgaskpri_ratio']-1)/aa['DolVol_ask']).where(aa['DolVol_ask']>0,np.nan)
        aa['MCI_bid'] = ((1-aa['wavgbidpri_ratio'])/aa['DolVol_bid']).where(aa['DolVol_bid']>0,np.nan)
        aa['MCI_imbalance'] = ((aa['MCI_bid']-aa['MCI_ask'])/(aa['MCI_bid']+aa['MCI_ask'])).where(aa['MCI_bid']+aa['MCI_ask']>0,np.nan)
        
        aa['wavbidask_ratio'] = (aa['wavgbidpri']*aa['totalaskvol'])/(aa['wavgaskpri']*aa['totalbidvol']).where(aa['wavgaskpri']*aa['totalbidvol']>0,np.nan)
        aa['wavbidask_gap'] = ((aa['wavgbidpri']*aa['totalaskvol'] - aa['wavgaskpri']*aa['totalbidvol'])/(aa['wavgbidpri']*aa['totalaskvol'] + aa['wavgaskpri']*aa['totalbidvol'])).where(aa['wavgbidpri']*aa['totalaskvol'] + aa['wavgaskpri']*aa['totalbidvol']>0,np.nan)
        
        aa['bidask_gap'] = ((aa['wavgaskpri'] - aa['wavgbidpri'])/(aa['wavgaskpri'] + aa['wavgbidpri'])).where(aa['wavgaskpri'] + aa['wavgbidpri']>0,np.nan)
        aa['mainbidask_gap'] = ((aa['wavgmainaskpri'] - aa['wavgmainbidpri'])/(aa['wavgmainaskpri'] + aa['wavgmainbidpri'])).where(aa['wavgmainaskpri'] + aa['wavgmainbidpri']>0,np.nan)
        aa['bidask_strength'] = ((aa['midpri'] - aa['wavgbidpri'])/(aa['wavgaskpri'] - aa['midpri'])).where(aa['wavgaskpri'] - aa['midpri']>0,np.nan)
        aa['mainbidask_strength'] = ((aa['midpri'] - aa['wavgmainbidpri'])/(aa['wavgmainaskpri'] - aa['midpri'])).where(aa['wavgmainaskpri'] - aa['midpri']>0,np.nan)

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
        
        aa['tradv_quantile25'] = aa['tradv'].quantile(0.25)
        aa['tradv_quantile50'] = aa['tradv'].quantile(0.5)
        aa_25 = aa.loc[aa.tradv<aa.tradv_quantile25]
        aa_50 = aa.loc[aa.tradv<aa.tradv_quantile50]
        res = []
        for df in [aa_25,aa_50,aa,]:
            if df.empty:
                res.append(pd.DataFrame())
            else:
                tmp = pd.DataFrame(np.array([[df['OFI1'].mean(),df['OFI2'].mean(),df['OFI5'].mean(),df['OFI10'].mean(),
                            df['MOFI_half'].mean(),df['MOFI_half_weighted'].mean(),df['MOFI'].mean(),df['MOFI_weighted'].mean(),
                            df['LogQuoteSlope1'].mean(),df['LogQuoteSlope2'].mean(),df['LogQuoteSlope5'].mean(),df['LogQuoteSlope10'].mean(),
                            df['MLogQuoteSlope_half'].mean(),df['MLogQuoteSlope_half_weighted'].mean(),df['MLogQuoteSlope'].mean(),df['MLogQuoteSlope_weighted'].mean(),
                            df['SOIR1'].mean(),df['SOIR2'].mean(),df['SOIR5'].mean(),df['SOIR10'].mean(),
                            df['MSOIR_half'].mean(),df['MSOIR_half_weighted'].mean(),df['MSOIR'].mean(),df['MSOIR_weighted'].mean(),
                            df['MPC3S'].mean(),df['MPC3S'].max(),df['MPC3S'].skew(),df['MPC3S'].kurt(),df['MPC1'].mean(),df['MPC1'].max(),
                            df['MPC1'].skew(),df['MPC1'].kurt(),df['MPC5'].mean(),df['MPC5'].max(),df['MPC5'].skew(),df['MPC5'].kurt(),
                            df['spread'].mean(),df['bs_spread_rate'].mean(),df['bid_tick_diff'].mean(),df['bid_tick_diff_ratio'].mean(),
                            df['ask_tick_diff'].mean(),df['ask_tick_diff_ratio'].mean(),df['ba_tick_diff_ratio'].mean(),df['wb_price_diff'].mean(),
                            df['wa_price_diff'].mean(),df['wba_price_diff_ratio'].mean(),df['bidv_std'].mean(),
                            df['askv_std'].mean(),df['avgbidv'].mean(),df['avgaskv'].mean(),df['bidv_std_mean'].mean(),df['askv_std_mean'].mean(),
                            df['wamp_diff'].mean(),df['wamp_diff_ratio'].mean(),df['wavgmidpri1_ratio'].mean(),df['wavgmidpri2_ratio'].mean(),
                            df['wavgmidpri1_gap'].mean(),df['wavgmidpri2_gap'].mean(),df['wavgmidpri_ratio'].mean(),df['wavgmidpri_gap'].mean(),
                            df['wap_diff'].mean(), df['wap_diff_ratio'].mean(), df['wavgpri1_ratio'].mean(),df['wavgpri2_ratio'].mean(),
                            df['wavgpri1_gap'].mean(),df['wavgpri2_gap'].mean(),df['wavgpri_ratio'].mean(),df['wavgpri_gap'].mean(),
                            df['wavgbidpri_ratio'].mean(),df['wavgbidpri_gap'].mean(),df['wavgaskpri_ratio'].mean(),df['wavgaskpri_gap'].mean(),
                            df['MCI_ask'].mean(),df['MCI_bid'].mean(),df['MCI_imbalance'].mean(),df['wavbidask_ratio'].mean(),
                            df['wavbidask_gap'].mean(),df['bidask_strength'].mean(),df['mainbidvol_rate'].mean(),df['mainaskvol_rate'].mean(),
                            df['wbs_imbalance_rate'].mean(),df['wbs_main_imbalance_rate'].mean(),df['mainwvol_rate'].mean(),df['totalwvol_rate'].mean(),
                            df['sweep_buy'].mean(),df['sweep_sell'].mean(),pd.to_datetime(dates[-1],format='%Y%m%d')]]),
                        index=[df['securityid'].iloc[0]],
                        columns=['OFI1','OFI2','OFI5','OFI10','MOFI_half','MOFI_half_weighted','MOFI','MOFI_weighted',
                                'LogQuoteSlope1','LogQuoteSlope2','LogQuoteSlope5','LogQuoteSlope10',
                                'MLogQuoteSlope_half','MLogQuoteSlope_half_weighted','MLogQuoteSlope','MLogQuoteSlope_weighted',
                                'SOIR1','SOIR2','SOIR5','SOIR10','MSOIR_half','MSOIR_half_weighted','MSOIR','MSOIR_weighted',
                                'MPC3S','MPC3S_max','MPC3S_skew','MPC3S_kurt','MPC1','MPC1_max','MPC1_skew','MPC1_kurt','MPC5','MPC5_max','MPC5_skew','MPC5_kurt',
                                'spread','bs_spread_rate','bid_tick_diff','bid_tick_diff_ratio','ask_tick_diff','ask_tick_diff_ratio','ba_tick_diff_ratio',
                                'wb_price_diff','wa_price_diff','wba_price_diff_ratio','bidv_std','askv_std','avgbidv','avgaskv','bidv_std_mean',
                                'askv_std_mean','wamp_diff','wamp_diff_ratio','wavgmidpri1_ratio','wavgmidpri2_ratio','wavgmidpri1_gap','wavgmidpri2_gap',
                                'wavgmidpri_ratio','wavgmidpri_gap','wap_diff','wap_diff_ratio','wavgpri1_ratio','wavgpri2_ratio','wavgpri1_gap',
                                'wavgpri2_gap','wavgpri_ratio','wavgpri_gap','wavgbidpri_ratio','wavgbidpri_gap','wavgaskpri_ratio',
                                'wavgaskpri_gap','MCI_ask','MCI_bid','MCI_imbalance','wavbidask_ratio','wavbidask_gap','bidask_strength','mainbidvol_rate',
                                'mainaskvol_rate','wbs_imbalance_rate','wbs_main_imbalance_rate','mainwvol_rate','totalwvol_rate','sweep_buy','sweep_sell','datetime']
                        )
                tmp.index.name = 'securityid'
                res.append(tmp.reset_index())
        return res

def tick_feature(dattes,n_jobs):
    print(".."*30)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        res = Parallel(n_jobs=n_jobs,verbose=3,pre_dispatch='100*n_jobs',batch_size=50,)(delayed(tick_feature_day_ticker)(ticker,dats) for dats in dattes for ticker in [x[:-8] for x in os.listdir(f'{data_path}/{dats[-1]}/')])
        # return res
        result = []
        for i,quantile in enumerate([0.25,0.5,1]):
            fn = f"{tmp_path}/{uuid.uuid4().hex}_{quantile}.parquet"
            pd.concat([res[j][i] for j in range(len(res))],axis=0,copy=False).to_parquet(fn)
            result.append(fn)
        return result  # write back to database

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


def data_div(lookback) -> list:
    TASKNUM = 9 + 12
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
        tick_feature(lookback,n_jobs=128)
