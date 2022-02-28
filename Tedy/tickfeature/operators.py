"""
@author: Ruochen Pan
"""
import os
import datetime
import pandas as pd
import numpy as np
from talib import *
from numpy.lib import pad
# import rollingrank
from scipy.signal import filtfilt, butter
from numpy.lib.stride_tricks import as_strided
from numpy import ma
from statsmodels.regression.rolling import RollingOLS
from joblib import Parallel, delayed
import warnings
def roc(df, d=1):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().pct_change(d).replace([np.inf,-np.inf],np.nan),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().pct_change(d).replace([np.inf,-np.inf],np.nan),index=df.index,name=df.name)

def corr2_coeff(A, B):
    # Rowwise mean of input arrays & subtract from input arrays themeselves
    A_mA = A - np.nanmean(A,axis=1,keepdims=True)
    B_mB = B - np.nanmean(B,axis=1,keepdims=True)

    # Sum of squares across rows
    ssA = np.nansum(A_mA**2,axis=1)
    ssB = np.nansum(B_mB**2,axis=1)

    # Finally get corr coeff
    return np.matmul(A_mA, B_mB.T) / np.sqrt(np.matmul(ssA[:, None],ssB[None]))

def corr2_coeff_diag(A, B):
    # Rowwise mean of input arrays & subtract from input arrays themeselves
    A_mA = A - np.nanmean(A,axis=1,keepdims=True)
    B_mB = B - np.nanmean(B,axis=1,keepdims=True)

    # Sum of squares across rows
    ssA = np.nansum(A_mA**2,axis=1)
    ssB = np.nansum(B_mB**2,axis=1)

    # Finally get corr coeff
    return np.nansum(A_mA*B_mB,axis=1) / np.sqrt(ssA*ssB)

def corr3_coeff(A, B):
    A_mA = A - np.nanmean(A,axis=2,keepdims=True)
    B_mB = B - np.nanmean(B,axis=2,keepdims=True)
    
    ssA = np.nansum(A_mA**2,axis=2)
    ssB = np.nansum(B_mB**2,axis=2)
    
    return np.matmul(A_mA, np.transpose(B_mB,(0,2,1))) / np.sqrt(np.matmul(ssA[:, :, None],ssB[:, None, :]))

def corr3_coeff_diag(A, B):
    A_mA = A - np.nanmean(A,axis=2,keepdims=True)
    B_mB = B - np.nanmean(B,axis=2,keepdims=True)
    
    ssA = np.nansum(A_mA**2,axis=2)
    ssB = np.nansum(B_mB**2,axis=2)
    
    return np.nansum(A_mA*B_mB,axis=2) / np.sqrt(ssA*ssB)

def corr1d(ts1,ts2,period):
    time = ts1[((~np.isnan(ts1))&(~np.isnan(ts2)))].index
    return pd.Series(CORREL(ts1.loc[time],ts2.loc[time],period),index=time,name=ts1.name).reindex(ts1.index)

def CORRELATION(df1,df2,period):
    if len(df1.shape) == 2 and len(df2.shape) == 2:
        return df1.apply(lambda x: corr1d(x,df2[x.name],period) if ((~np.isnan(x))&(~np.isnan(df2[x.name]))).sum()!=0 else x*np.nan)
    elif len(df1.shape) == 2 and len(df2.shape) == 1:
        return df1.apply(lambda x: corr1d(x,df2,period) if ((~np.isnan(x))&(~np.isnan(df2))).sum()!=0 else x*np.nan)
    elif len(df1.shape) == 1 and len(df2.shape) == 2:
        return df2.apply(lambda x: corr1d(x,df1,period) if ((~np.isnan(x))&(~np.isnan(df1))).sum()!=0 else x*np.nan)
    elif len(df1.shape) == 1 and len(df2.shape) == 1:
        return corr1d(df1,df2,period) if ((~np.isnan(df1))&(~np.isnan(df2))).sum()!=0 else df1*np.nan

# def CORRELATION(df1,df2,period):
#     size = df1.values.dtype.itemsize
#     if len(df1.shape) == 2 and len(df2.shape) == 2:
#         MA = as_strided(df1.values,shape=(len(df1)-period,period,df1.shape[1]),strides=(df1.shape[1]*size,df1.shape[1]*size,size),writeable=False)
#         MB = as_strided(df2.values,shape=(len(df2)-period,period,df2.shape[1]),strides=(df2.shape[1]*size,df2.shape[1]*size,size),writeable=False)
#         return pd.DataFrame(corr3_coeff_diag(MA.transpose((0,2,1)),MB.transpose((0,2,1))),index=df1.index[period:],columns=df1.columns).reindex(df1.index)
#     elif len(df1.shape) == 2 and len(df2.shape) == 1:
#         MA = as_strided(df1.values,shape=(len(df1)-period,period,df1.shape[1]),strides=(df1.shape[1]*size,df1.shape[1]*size,size),writeable=False)
#         MB = as_strided(df2.values,shape=(len(df2)-period,period),strides=(size,size),writeable=False)
#         return pd.DataFrame(corr3_coeff_diag(MA.transpose((0,2,1)),MB[:,None,:]),index=df1.index[period:],columns=df1.columns).reindex(df1.index)
#     elif len(df1.shape) == 1 and len(df2.shape) == 2:
#         MA = as_strided(df1.values,shape=(len(df1)-period,period),strides=(size,size),writeable=False)
#         MB = as_strided(df2.values,shape=(len(df2)-period,period,df2.shape[1]),strides=(df2.shape[1]*size,df2.shape[1]*size,size),writeable=False)
#         return pd.DataFrame(corr3_coeff_diag(MA[:,None,:],MB.transpose((0,2,1))),index=df2.index[period:],columns=df2.columns).reindex(df2.index)
#     elif len(df1.shape) == 1 and len(df2.shape) == 1:
#         MA = as_strided(df1.values,shape=(len(df1)-period,period),strides=(size,size),writeable=False)
#         MB = as_strided(df2.values,shape=(len(df2)-period,period),strides=(size,size),writeable=False)
#         return pd.Series(corr2_coeff_diag(MA.T,MB.T),index=df1.index[period:],name=df1.name).reindex(df1.index)
    
def rollingMEAN(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().rolling(period).mean(),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().rolling(period).mean(),index=df.index,name=df.name)

# def rollingMEAN(df,period):
#     size = df.values.dtype.itemsize
#     if len(df.shape) == 2:
#         M = as_strided(np.ascontiguousarray(df.values),shape=(len(df)-period,period,df.shape[1]),strides=(df.shape[1]*size,df.shape[1]*size,size),writeable=False)
#         return pd.DataFrame(np.nanmean(M,axis=1),index=df.index[period:],columns=df.columns).reindex(df.index)
#     elif len(df.shape) == 1:
#         M = as_strided(np.ascontiguousarray(df.values),shape=(len(df)-period,period),strides=(size,size),writeable=False)
#         return pd.Series(np.nanmean(M,axis=1),index=df.index[period:],name=df.name).reindex(df.index)

# def rollingMEAN(df,period):
#     return df.rolling(period,min_periods = period//5).mean()

def rollingSTD(df,period,ddof=1):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().rolling(period).std(ddof=ddof),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().rolling(period).std(ddof=ddof),index=df.index,name=df.name)

# def rollingSTD(df,period):
#     size = df.values.dtype.itemsize
#     if len(df.shape) == 2:
#         M = as_strided(df.values,shape=(len(df)-period,period,df.shape[1]),strides=(df.shape[1]*size,df.shape[1]*size,size),writeable=False)
#         return pd.DataFrame(np.nanstd(M,axis=1),index=df.index[period:],columns=df.columns).reindex(df.index)
#     elif len(df.shape) == 1:
#         M = as_strided(df.values,shape=(len(df)-period,period),strides=(size,size),writeable=False)
#         return pd.Series(np.nanstd(M,axis=1),index=df.index[period:],name=df.name).reindex(df.index)

# def rollingSTD(df,period):
#     return df.rolling(period,min_periods = period//5).std()

def zscore1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series((ts.loc[time]-ts.loc[time].rolling(period).mean())/ts.loc[time].rolling(period).std(),index=time,name=ts.name).replace([np.inf,-np.inf],np.nan).reindex(ts.index)

def zscore(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: zscore1d(x,period))
    else:
        return zscore1d(df,period)

# def zscore(df,period):
#     size = df.values.dtype.itemsize
#     if len(df.shape) == 2:
#         M = as_strided(df.values,shape=(len(df)-period,period,df.shape[1]),strides=(df.shape[1]*size,df.shape[1]*size,size),writeable=False)
#         return pd.DataFrame((df.values[period:] - np.nanmean(M,axis=1))/np.nanstd(M,axis=1),index=df.index[period:],columns=df.columns).reindex(df.index)
#     elif len(df.shape) == 1:
#         M = as_strided(df.values,shape=(len(df)-period,period),strides=(size,size),writeable=False)
#         return pd.Series((df.values[period:] - np.nanmean(M,axis=1))/np.nanstd(M,axis=1),index=df.index[period:],name=df.name).reindex(df.index)

def rollingVAR(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().rolling(period).var(),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().rolling(period).var(),index=df.index,name=df.name)

# def rollingVAR(df,period):
#     size = df.values.dtype.itemsize
#     if len(df.shape) == 2:
#         M = as_strided(df.values,shape=(len(df)-period,period,df.shape[1]),strides=(df.shape[1]*size,df.shape[1]*size,size),writeable=False)
#         return pd.DataFrame(np.nanvar(M,axis=1),index=df.index[period:],columns=df.columns).reindex(df.index)
#     elif len(df.shape) == 1:
#         M = as_strided(df.values,shape=(len(df)-period,period),strides=(size,size),writeable=False)
#         return pd.Series(np.nanvar(M,axis=1),index=df.index[period:],name=df.name).reindex(df.index)

# def rollingVAR(df,period):
#     return df.rolling(period,min_periods=period//5).var()

def autoCORR(df,period):
    return CORRELATION(df,df.shift(1),period)

def autoBETA(df,period):
    rollingstd = rollingSTD(df,period)
    return autoCORR(df,period)*rollingstd/rollingstd.shift(1)

def autoINTERCEPT(df,period):
    rollingmean = rollingMEAN(df,period)
    return rollingmean-(autoBETA(df,period)*rollingmean.shift(1))

def beta1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(LINEARREG_SLOPE(ts.loc[time],period),index=time,name=ts.name).reindex(ts.index)

def BETA(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: beta1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan,axis=0)
    else:
        return beta1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def intercept1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(LINEARREG_INTERCEPT(ts.loc[time],period),index=time,name=ts.name).reindex(ts.index)

def INTERCEPT(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: intercept1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan,axis=0)
    else:
        return intercept1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def angle1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(LINEARREG_ANGLE(ts.loc[time],period),index=time,name=ts.name).reindex(ts.index)

def ANGLE(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: angle1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan,axis=0)
    else:
        return angle1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def corruni1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(CORREL(ts.loc[time],np.arange(len(time)).astype(float),period),index=time,name=ts.name).reindex(ts.index)

def CORR(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: corruni1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan,axis=0)
    else:
        return corruni1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def rankcorr1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(CORREL(ts.loc[time].rank(),np.arange(len(time)).astype(float),period),index=time,name=ts.name).reindex(ts.index)

def rankCORR(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: rankcorr1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan,axis=0)
    else:
        return rankcorr1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def rankBETA(df,period):
    return rankCORR(df,period)

def rankINTERCEPT(df,period):
    return -(rankBETA(df,period)*np.arange(period).mean() - np.arange(period).mean())

def rankANGLE(df,period):
    return np.arctan(rankBETA(df,period))

def ols_fb(data,yvar,xvar):
    data = pd.concat([data[yvar],data[xvar]],axis=1).replace([np.inf,-np.inf],np.nan).dropna()
    gamma,_,_,_ = np.linalg.lstsq(data[xvar],data[yvar],rcond=None)
    return pd.Series(gamma)

def FamaMacBeth(Y,Xs):
    data = pd.concat([Y.unstack()]+[X.reindex(index=Y.index,columns=Y.columns).unstack() for X in Xs],axis=1).reset_index()
    data.columns = ['ticker','time','Y'] + [f'X{i}' for i in range(len(Xs))]
    res = data.groupby('time').apply(ols_fb,'Y',data.columns[3:]).replace(0,np.nan)
    res.columns = list(data.columns[3:])
    return res

def FF_helper(mod):
    return mod.fit(params_only=True).params

def rollingMSE(Y,X,params,period):
    return Y.rolling(period,min_periods=1).apply(lambda y: np.nanvar(y - (X.loc[y.index,:]*params.loc[y.index[-1],:].values[None,:]).sum(axis=1)))

def FF_helper2(mod):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        return mod.fit()

def FamaFrench(Y,X,period,n_jobs=-1):
    mods = [RollingOLS(Y[col],X,period) for col in Y.columns if not np.isnan(Y[col]).all()]
    cols = [col for col in Y.columns if not np.isnan(Y[col]).all()]
    mods = Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(delayed(FF_helper2)(mod) for mod in mods)
    resid_mse = pd.concat([mod.mse_resid for mod in mods],axis=1).replace(0,np.nan)
    resid_mse.columns=cols
    R2 = pd.concat([mod.rsquared_adj for mod in mods],axis=1).replace(0,np.nan)
    R2.columns = cols
    params = np.array([mod.params.values for mod in mods]).T
    return ([pd.DataFrame(params[i],index=Y.index,columns=cols).reindex(columns=Y.columns) for i in range(len(params))],R2.reindex(columns=Y.columns),resid_mse.reindex(columns=Y.columns))
    
# def FamaFrench(Y,X,period,n_jobs=-1):
#     mods = [RollingOLS(Y[col],X,period) for col in Y.columns if not np.isnan(Y[col]).all()]
#     cols = [col for col in Y.columns if not np.isnan(Y[col]).all()]
#     params = Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(delayed(FF_helper)(mod) for mod in mods)
#     resid_mse = pd.concat(Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(delayed(rollingMSE)(Y[col],X,params[i],period) for i,col in enumerate(cols)),axis=1)
#     count = Y[cols].rolling(period,min_periods=1).count()
#     R2 = ((resid_mse*count)/((Y[cols].rolling(period,min_periods=1).var()+Y[cols].rolling(period,min_periods=1).mean()**2)*count)).replace(0,np.nan).reindex(Y.index,columns=Y.columns)
#     resid_mse = resid_mse.reindex(Y.index,columns=Y.columns)
#     params = np.array([param.values for param in params]).T
#     return ([pd.DataFrame(params[i],index=Y.index,columns=cols).reindex(columns=Y.columns) for i in range(len(params))],R2,resid_mse)
    
# def FamaFrench(Y,X,period,n_jobs=-1):
#     mods = [RollingOLS(Y[col],X,period) for col in Y.columns if not np.isnan(Y[col]).all()]
#     cols = [col for col in Y.columns if not np.isnan(Y[col]).all()]
#     params = np.array(Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(delayed(FF_helper)(mod) for mod in mods))
#     size = X.values.dtype.itemsize
#     MX = as_strided(np.ascontiguousarray(X.values),shape=(X.shape[0]-period,period,X.shape[1]),strides=(X.shape[1]*size,X.shape[1]*size,size),writeable=False)
#     MY = as_strided(np.ascontiguousarray(Y[cols].values),shape=(Y[cols].shape[0]-period,period,Y[cols].shape[1]),strides=(Y[cols].shape[1]*size,Y[cols].shape[1]*size,size),writeable=False)
#     tmp = np.transpose(np.nansum(params[:,period:,None,:]*MX[None,:,:,:],axis=3),(1,2,0))
#     resid = MY - tmp
#     resid_mse = pd.DataFrame(np.nanvar(resid,axis=1),index=Y.index[period:],columns=cols).reindex(Y.index,columns=Y.columns)
#     # R2 = pd.DataFrame(1-(np.nansum(resid**2,axis=1)/np.nansum((MY-np.nanmean(MY,axis=1,keepdims=True))**2,axis=1)),index=Y.index[period:],columns=cols).replace(0,np.nan).reindex(Y.index,columns=Y.columns)
#     count = Y[cols].rolling(period,min_periods=1).count()
#     R2 = ((resid_mse[cols]*count)/((Y[cols].rolling(period,min_periods=1).var()+Y[cols].rolling(period,min_periods=1).mean()**2)*count)).replace(0,np.nan).reindex(Y.index,columns=Y.columns)
#     params = params.T
#     return ([pd.DataFrame(params[i],index=Y.index,columns=cols).reindex(columns=Y.columns) for i in range(len(params))],R2,resid_mse)
    
def rollingBETA(Y,X,period):
    if len(X.shape) == len(Y.shape):
        return CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0)
    elif len(X.shape) == 1:
        return CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0).values[:,np.newaxis]
    else:
        return CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]/rollingSTD(X,period,ddof=0)

def rollingINTERCEPT(Y,X,period):
    if len(X.shape) == len(Y.shape):
        return rollingMEAN(Y,period) - rollingBETA(Y,X,period)*rollingMEAN(X,period)
    elif len(X.shape) == 1:
        return rollingMEAN(Y,period) - rollingBETA(Y,X,period)*rollingMEAN(X,period).values[:,np.newaxis]
    else:
        return -rollingBETA(Y,X,period)*rollingMEAN(X,period) + rollingMEAN(Y,period).values[:,np.newaxis]

def rollingPRED(Y,X,X_NEW,period):
    if len(X.shape) == len(Y.shape):
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0)
        intercept = rollingMEAN(Y,period) - slope*rollingMEAN(X,period)
    elif len(X.shape) == 1:
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0).values[:,np.newaxis]
        intercept = rollingMEAN(Y,period) - slope*rollingMEAN(X,period).values[:,np.newaxis]
    else:
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]/rollingSTD(X,period,ddof=0)
        intercept = -slope*rollingMEAN(X,period) + rollingMEAN(Y,period).values[:,np.newaxis]
    if len(X.shape) != len(Y.shape) and len(X.shape) == 1:
        return intercept+ slope*X_NEW.values[:,np.newaxis]
    else:
        return intercept+ slope*X_NEW

def rollingRESID(Y,X,period):
    if len(X.shape) != len(Y.shape) and len(Y.shape) == 1:
        return -rollingPRED(Y,X,X,period) + Y.values[:,np.newaxis]
    else:
        return Y - rollingPRED(Y,X,X,period)

def rollingRsquared(Y,X,period):
    ymean = rollingMEAN(Y,period)
    if len(X.shape) == len(Y.shape):
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0)
        intercept = ymean - slope*rollingMEAN(X,period)
    elif len(X.shape) == 1:
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)/rollingSTD(X,period,ddof=0).values[:,np.newaxis]
        intercept = ymean - slope*rollingMEAN(X,period).values[:,np.newaxis]
    else:
        slope = CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]/rollingSTD(X,period,ddof=0)
        intercept = -slope*rollingMEAN(X,period) + ymean.values[:,np.newaxis]
    if len(X.shape) != len(Y.shape) and len(X.shape) == 1:
        return (sum([(slope*X.shift(p).values[:,np.newaxis]+intercept-ymean)**2 for p in range(period)])/period)/rollingMEAN((Y-ymean)**2,period)
    else:
        return (sum([(slope*X.shift(p)+intercept-ymean)**2 for p in range(period)])/period)/rollingMEAN((Y-ymean)**2,period)

# def rollingRsquared(Y,X,period):
#     size = Y.values.dtype.itemsize
#     if len(Y.shape) == 2 and len(X.shape) == 2:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period,Y.shape[1]),strides=(Y.shape[1]*size,Y.shape[1]*size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period,X.shape[1]),strides=(X.shape[1]*size,X.shape[1]*size,size),writeable=False)
#         ymean = np.nanmean(MY,axis=1)
#         slope = corr3_coeff_diag(MY.transpose((0,2,1)),MX.transpose((0,2,1)))*np.nanstd(MY,axis=1)/np.nanstd(MX,axis=1)
#         intercept = ymean - slope*np.nanmean(MX,axis=1)
#         R2 = np.nansum((slope[:,None,:]*MX+intercept[:,None,:]-ymean[:,None,:])**2,axis=1)/np.nansum((MY-ymean[:,None,:])**2,axis=1)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 2 and len(X.shape) == 1:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period,Y.shape[1]),strides=(Y.shape[1]*size,Y.shape[1]*size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period),strides=(size,size),writeable=False)
#         ymean = np.nanmean(MY,axis=1)
#         slope = corr3_coeff_diag(MY.transpose((0,2,1)),MX[:,None,:])*np.nanstd(MY,axis=1)/np.nanstd(MX,axis=1,keepdims=True)
#         intercept = ymean - slope*np.nanmean(MX,axis=1,keepdims=True)
#         R2 = np.nansum((slope[:,None,:]*MX[:,:,None]+intercept[:,None,:]-ymean[:,None,:])**2,axis=1)/np.nansum((MY-ymean[:,None,:])**2,axis=1)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 1 and len(X.shape) == 2:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period),strides=(size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period,X.shape[1]),strides=(X.shape[1]*size,X.shape[1]*size,size),writeable=False)
#         ymean = np.nanmean(MY,axis=1,keepdims=True)
#         slope = corr3_coeff_diag(MY[:,None,:],MX.transpose((0,2,1)))*np.nanstd(MY,axis=1,keepdims=True)/np.nanstd(MX,axis=1)
#         intercept = ymean - slope*np.nanmean(MX,axis=1)
#         R2 = np.nansum((slope[:,None,:]*MX+intercept[:,None,:]-ymean[:,None,:])**2,axis=1)/np.nansum((MY[:,:,None]-ymean[:,None,:])**2,axis=1)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 1 and len(X.shape) == 1:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period),strides=(size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period),strides=(size,size),writeable=False)
#         ymean = np.nanmean(MY,axis=1)
#         slope = corr2_coeff_diag(MY.T,MX.T)*np.nanstd(MY,axis=1)/np.nanstd(MX,axis=1)
#         intercept = ymean - slope*np.nanmean(MX,axis=1)
#         R2 = np.nansum((slope[:,None]*MX+intercept[:,None]-ymean[:,None])**2,axis=1)/np.nansum((MY-ymean[:,None])**2,axis=1)
#         return pd.Series(R2,index=Y.index[period:],name=Y.name).reindex(Y.index)

def rollingBETA_fixed(Y,X,period,Y_fixed=0,X_fixed=0):
    if len(X.shape) == len(Y.shape):
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        return (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std+(rollingMEAN(Y,period)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
    elif len(X.shape) == 1:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        return (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std.values[:,np.newaxis]+(rollingMEAN(Y,period)-Y_fixed)*x_mean.values[:,np.newaxis])/(x_std**2+x_mean**2).values[:,np.newaxis]
    else:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        return (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]*x_std+(rollingMEAN(Y,period)-Y_fixed).values[:,np.newaxis]*x_mean)/(x_std**2+x_mean**2)

def rollingPRED_fixed(Y,X,X_NEW,period,Y_fixed=0,X_fixed=0):
    if len(X.shape) == len(Y.shape):
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std+(rollingMEAN(Y,period)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
    elif len(X.shape) == 1:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std.values[:,np.newaxis]+(rollingMEAN(Y,period)-Y_fixed)*x_mean.values[:,np.newaxis])/(x_std**2+x_mean**2).values[:,np.newaxis]
    else:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]*x_std+(rollingMEAN(Y,period)-Y_fixed).values[:,np.newaxis]*x_mean)/(x_std**2+x_mean**2)
    if len(X.shape) != len(Y.shape) and len(X.shape) == 1:
        return slope*X_NEW.values[:,np.newaxis]
    else:
        return slope*X_NEW

def rollingRESID_fixed(Y,X,period,Y_fixed=0,X_fixed=0):
    if len(X.shape) != len(Y.shape) and len(Y.shape) == 1:
        return -rollingPRED_fixed(Y,X,X,period,Y_fixed,X_fixed) + Y.values[:,np.newaxis]
    else:
        return Y - rollingPRED_fixed(Y,X,X,period,Y_fixed,X_fixed)

def rollingRsquared_fixed(Y,X,period,Y_fixed=0,X_fixed=0):
    if len(X.shape) == len(Y.shape):
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std+(rollingMEAN(Y,period)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
    elif len(X.shape) == 1:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0)*x_std.values[:,np.newaxis]+(rollingMEAN(Y,period)-Y_fixed)*x_mean.values[:,np.newaxis])/(x_std**2+x_mean**2).values[:,np.newaxis]
    else:
        x_std = rollingSTD(X,period,ddof=0)
        x_mean = rollingMEAN(X,period)-X_fixed
        slope = (CORRELATION(Y,X,period)*rollingSTD(Y,period,ddof=0).values[:,np.newaxis]*x_std+(rollingMEAN(Y,period)-Y_fixed).values[:,np.newaxis]*x_mean)/(x_std**2+x_mean**2)
    if len(X.shape) != len(Y.shape) and len(X.shape) == 1:
        return (sum([(slope*X.shift(p).values[:,np.newaxis]-Y_fixed)**2 for p in range(period)])/period)/rollingMEAN((Y-Y_fixed)**2,period)
    else:
        return (sum([(slope*X.shift(p)-Y_fixed)**2 for p in range(period)])/period)/rollingMEAN((Y-Y_fixed)**2,period)

# def rollingRsquared_fixed(Y,X,period,Y_fixed=0,X_fixed=0):
#     size = Y.values.dtype.itemsize
#     if len(Y.shape) == 2 and len(X.shape) == 2:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period,Y.shape[1]),strides=(Y.shape[1]*size,Y.shape[1]*size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period,X.shape[1]),strides=(X.shape[1]*size,X.shape[1]*size,size),writeable=False)
#         x_mean = np.nanmean(MX,axis=1)
#         x_std = np.nanstd(MX,axis=1)
#         slope = (corr3_coeff_diag(MY.transpose((0,2,1)),MX.transpose((0,2,1)))*np.nanstd(MY,axis=1)*x_std+(np.nanmean(MY,axis=1)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
#         R2 = np.nansum((slope[:,None,:]*MX-Y_fixed)**2,axis=1)/np.nansum((MY-Y_fixed)**2,axis=1)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 2 and len(X.shape) == 1:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period,Y.shape[1]),strides=(Y.shape[1]*size,Y.shape[1]*size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period),strides=(size,size),writeable=False)
#         x_mean = np.nanmean(MX,axis=1,keepdims=True)
#         x_std = np.nanstd(MX,axis=1,keepdims=True)
#         slope = (corr3_coeff_diag(MY.transpose((0,2,1)),MX[:,None,:])*np.nanstd(MY,axis=1)*x_std+(np.nanmean(MY,axis=1)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
#         R2 = np.nansum((slope[:,None,:]*MX[:,:,None]-Y_fixed)**2,axis=1)/np.nansum((MY-Y_fixed)**2,axis=1)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 1 and len(X.shape) == 2:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period),strides=(size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period,X.shape[1]),strides=(X.shape[1]*size,X.shape[1]*size,size),writeable=False)
#         x_mean = np.nanmean(MX,axis=1)
#         x_std = np.nanstd(MX,axis=1)
#         slope = (corr3_coeff_diag(MY[:,None,:],MX.transpose((0,2,1)))*np.nanstd(MY,axis=1,keepdims=True)*x_std+(np.nanmean(MY,axis=1,keepdims=True)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
#         R2 = np.nansum((slope[:,None,:]*MX-Y_fixed)**2,axis=1)/np.nansum((MY-Y_fixed)**2,axis=1,keepdims=True)
#         return pd.DataFrame(R2,index=Y.index[period:],columns=Y.columns).reindex(Y.index)
#     elif len(Y.shape) == 1 and len(X.shape) == 1:
#         MY = as_strided(Y.values,shape=(len(Y)-period,period),strides=(size,size),writeable=False)
#         MX = as_strided(X.values,shape=(len(X)-period,period),strides=(size,size),writeable=False)
#         x_mean = np.nanmean(MX,axis=1)
#         x_std = np.nanstd(MX,axis=1)
#         slope = (corr2_coeff_diag(MY.T,MX.T)*np.nanstd(MY,axis=1)*x_std+(np.nanmean(MY,axis=1)-Y_fixed)*x_mean)/(x_std**2+x_mean**2)
#         R2 = np.nansum((slope[:,None]*MX-Y_fixed)**2,axis=1)/np.nansum((MY-Y_fixed)**2,axis=1)
#         return pd.Series(R2,index=Y.index[period:],name=Y.name).reindex(Y.index)

def rollingCentrality(M,period,n_components,mean=True,std=False):
    tt = M.dropna()
    size = tt.values.dtype.itemsize
    tt = as_strided(tt.values,shape=(len(tt)-period+1,period,tt.shape[1]),strides=(tt.shape[1]*size,tt.shape[1]*size,size),writeable=False)
    tt_std = np.nanstd(tt,axis=1)
    if mean:
        tt = tt - np.nanmean(tt,axis=1,keepdims=True)
    if mean and std:
        tt = tt/tt_std[:,np.newaxis,:]
    U, s, Vt = np.linalg.svd(tt, full_matrices=False)
    V = np.transpose(Vt,(0,2,1))
    AR = (s**2/(tt.shape[1]-1))/np.nansum((tt_std**2),axis=1)[:,None]
    loadings_sum = np.nansum(np.abs(V),axis=1,keepdims=True)
    AC = np.nansum((AR[:,None,:]*np.abs(V)/loadings_sum)[:,:,:n_components],axis=2)/np.nansum(AR[:,:n_components],axis=1,keepdims=True)
    return pd.DataFrame(AC,index=M.dropna().index[(period-1):],columns=M.columns).reindex(M.index)

def tls1d(ts1,ts2,period,mean=True,std=False):
    time = ts1[((~np.isnan(ts1))&(~np.isnan(ts2)))].index
    tt = np.concatenate((ts1.loc[time].values[:,np.newaxis],ts2.loc[time].values[:,np.newaxis]),axis=1)
    size = tt.dtype.itemsize
    tt = as_strided(tt,shape=(len(tt)-period+1,period,tt.shape[1]),strides=(tt.shape[1]*size,tt.shape[1]*size,size),writeable=False)
    tt_std = np.nanstd(tt,axis=1)
    if mean:
        tt = tt - np.nanmean(tt,axis=1,keepdims=True)
    if mean and std:
        tt = tt/tt_std[:,np.newaxis,:]
    U, s, Vt = np.linalg.svd(tt, full_matrices=False)
    V = np.transpose(Vt,(0,2,1))
    return pd.Series(V[:,0,0]/V[:,1,0],index=time[(period-1):],name=ts1.name).reindex(ts1.index).replace([np.inf,-np.inf],np.nan)

def rollingTLS_beta(Y,X,period,mean=True,std=False):
    if mean and std:
        Y_std = rollingSTD(Y,period,ddof=0)
        X_std = rollingSTD(X,period,ddof=0)
    else:
        Y_std, X_std = Y.copy(), X.copy()
        Y_std.loc[:] = 1
        X_std.loc[:] = 1
    if len(Y.shape) == 2 and len(X.shape) == 2:
        return Y.apply(lambda y: tls1d(y,X[y.name],period,mean=mean,std=std) if ((~np.isnan(y))&(~np.isnan(X[y.name]))).sum()>=period else y*np.nan)*Y_std/X_std
    elif len(Y.shape) == 2 and len(X.shape) == 1:
        return Y.apply(lambda y: tls1d(y,X,period,mean=mean,std=std) if ((~np.isnan(y))&(~np.isnan(X))).sum()>=period else y*np.nan)*Y_std/X_std.values[:,np.newaxis]
    elif len(Y.shape) == 1 and len(X.shape) == 2:
        return X.apply(lambda x: tls1d(Y,x,period,mean=mean,std=std) if ((~np.isnan(x))&(~np.isnan(Y))).sum()>=period else x*np.nan)*Y_std.values[:,np.newaxis]/X_std
    elif len(Y.shape) == 1 and len(X.shape) == 1:
        return tls1d(Y,X,period,mean=mean,std=std)*Y_std/X_std if ((~np.isnan(Y))&(~np.isnan(X))).sum()>=period else Y*np.nan

def rollingTLS_alpha(Y,X,period,std=False):
    Y_mean = rollingMEAN(Y,period)
    X_mean = rollingMEAN(X,period)
    if std:
        Y_std = rollingSTD(Y,period,ddof=0)
        X_std = rollingSTD(X,period,ddof=0)
    else:
        Y_std, X_std = Y.copy(), X.copy()
        Y_std.loc[:] = 1
        X_std.loc[:] = 1
    if len(Y.shape) == 2 and len(X.shape) == 2:
        beta = Y.apply(lambda y: tls1d(y,X[y.name],period,std=std) if ((~np.isnan(y))&(~np.isnan(X[y.name]))).sum()>=period else y*np.nan)*Y_std/X_std
        return Y_mean - beta*X_mean
    elif len(Y.shape) == 2 and len(X.shape) == 1:
        beta = Y.apply(lambda y: tls1d(y,X,period,std=std) if ((~np.isnan(y))&(~np.isnan(X))).sum()>=period else y*np.nan)*Y_std/X_std.values[:,np.newaxis]
        return Y_mean - beta*X_mean.values[:,np.newaxis]
    elif len(Y.shape) == 1 and len(X.shape) == 2:
        beta = X.apply(lambda x: tls1d(Y,x,period,std=std) if ((~np.isnan(x))&(~np.isnan(Y))).sum()>=period else x*np.nan)*Y_std.values[:,np.newaxis]/X_std
        return -beta*X_mean + Y_mean.values[:,np.newaxis]
    elif len(Y.shape) == 1 and len(X.shape) == 1:
        beta = tls1d(y,X,period,std=std)*Y_std/X_std if ((~np.isnan(Y))&(~np.isnan(X))).sum()>=period else Y*np.nan
        return Y_mean - beta*X_mean

def rollingTLS_PRED(Y,X,X_NEW,period,mean=True,std=False):
    if mean:
        Y_mean = rollingMEAN(Y,period)
        X_mean = rollingMEAN(X,period)
    else:
        Y_mean, X_mean = Y.copy(), X.copy()
        Y_mean.loc[:] = 0
        X_mean.loc[:] = 0
    if mean and std:
        Y_std = rollingSTD(Y,period,ddof=0)
        X_std = rollingSTD(X,period,ddof=0)
    else:
        Y_std, X_std = Y.copy(), X.copy()
        Y_std.loc[:] = 1
        X_std.loc[:] = 1
    if len(Y.shape) == 2 and len(X.shape) == 2:
        beta = Y.apply(lambda y: tls1d(y,X[y.name],period,mean=mean,std=std) if ((~np.isnan(y))&(~np.isnan(X[y.name]))).sum()>=period else y*np.nan)*Y_std/X_std
        alpha = Y_mean - beta*X_mean
    elif len(Y.shape) == 2 and len(X.shape) == 1:
        beta = Y.apply(lambda y: tls1d(y,X,period,mean=mean,std=std) if ((~np.isnan(y))&(~np.isnan(X))).sum()>=period else y*np.nan)*Y_std/X_std.values[:,np.newaxis]
        alpha = Y_mean - beta*X_mean.values[:,np.newaxis]
    elif len(Y.shape) == 1 and len(X.shape) == 2:
        beta = X.apply(lambda x: tls1d(Y,x,period,mean=mean,std=std) if ((~np.isnan(x))&(~np.isnan(Y))).sum()>=period else x*np.nan)*Y_std.values[:,np.newaxis]/X_std
        alpha = -beta*X_mean + Y_mean.values[:,np.newaxis]
    elif len(Y.shape) == 1 and len(X.shape) == 1:
        beta = tls1d(Y,X,period,mean=mean,std=std)*Y_std/X_std if ((~np.isnan(Y))&(~np.isnan(X))).sum()>=period else Y*np.nan
        alpha = Y_mean - beta*X_mean
    if len(X.shape) != len(Y.shape) and len(X.shape) == 1:
        return alpha+ beta*X_NEW.values[:,np.newaxis]
    else:
        return alpha+ beta*X_NEW

def rollingTLS_RESID(Y,X,period,mean=True,std=False):
    if len(X.shape) != len(Y.shape) and len(Y.shape) == 1:
        return -rollingTLS_PRED(Y,X,X,period,mean,std) + Y.values[:,None]
    else:
        return Y - rollingTLS_PRED(Y,X,X,period,mean,std)

def hurst1d(ts,lag_max,period):
    time = ts[(~np.isnan(ts))].index
    ts = ts.loc[time]
    lags = np.linspace(1,lag_max,100,dtype=int)
    tau = np.log(pd.concat([(ts-ts.shift(lag)) for lag in lags],axis=1).rolling(period).std())
    lags = np.log(lags)[np.newaxis,:]
    res = tau.std(axis=1)*corr2_coeff(tau.values,lags).ravel()/np.std(lags)
    res.name = ts.name
    return res

def hurst(df,period):
    lag_max = df.resample('W').count().max()
    if len(df.shape) == 2:
        return df.apply(lambda x: hurst1d(x,lag_max[x.name],period))
    else:
        return hurst1d(df,period)

def rank(df, pct=True):
	return df.rank(axis = 1, pct=pct)

def delay(df, period):
	return df.shift(period)

def delta(df, period):
	return df.diff(period)

def ts_min(df, period):    
	return df.rolling(window=period).min()

def ts_max(df, period):    
	return df.rolling(window=period).max()

def ts_argmax(df, period):    
	return df.rolling(window=period).apply(lambda x: x.argmax(), raw=True) + 1

def ts_argmin(df, period):    
	return df.rolling(window=period).apply(lambda x: x.argmin(), raw=True) + 1

# def ts_rank1d(df,period,pct=True):
#     return pd.Series(rollingrank.rollingrank(df.dropna().values,window=period,pct=pct),index=df.dropna().index,name=df.name).reindex(df.index)

def ts_rank(df,period,pct=True):
    if len(df.shape) == 1:
        return ts_rank1d(df,period,pct)
    else:
        return df.apply(ts_rank1d,axis=1)

def lpf_no_lag_filtfilt(x, order, corner_periods):
    if not isinstance(x, pd.Series):
        raise Exception('Input x not pd.Series, but {}'.format(type(x)))
    b, a = butter(order, 1.0 / corner_periods)
    y = filtfilt(b, a, x, padtype='odd')
    # y = pd.Series(y, index=x.index)
    # utilities.plot_price_comparison([x, y])
    # it's important to drop the head and tail, due to padding!
    y[:corner_periods] = np.nan
    y[-corner_periods:] = np.nan
    return y

def bidirect_smooth1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(lpf_no_lag_filtfilt(ts.loc[time],order=1,corner_periods=period),index=time,name=ts.name).reindex(ts.index) if len(time)>6 else ts

def bidirect_smooth(df, period):
    if len(df.shape) == 2:
        return df.apply(lambda x: bidirect_smooth1d(x,period))
    else:
        return bidirect_smooth1d(df,period)

def lowday(df, period):
    return df.rolling(period).apply(lambda x: period - x.argmin(), raw = True)

def highday(df, period):
    return df.rolling(period).apply(lambda x: period - x.argmax(), raw = True)

def ts_decay_linear_moving_average(df,period):
    weights = np.arange(1, period + 1).astype(float)
    return df.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True, engine='numba')

def ts_decay_exponential_moving_average(df,period):
    return df.ewm(span=period,ignore_na=True).mean()

def ts_decay_exponential_std(df,period):
    return df.ewm(span=period,ignore_na=True).std()

def ts_skewness(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().rolling(period).skew(),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().rolling(period).skew(),index=df.index,name=df.name)

def ts_kurtosis(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().rolling(period).kurt(),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().rolling(period).kurt(),index=df.index,name=df.name)

def ts_abs_chg_sum(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pd.Series(x.dropna().diff(1).abs().rolling(period).sum(),index=x.index,name=x.name))
    else:
        return pd.Series(df.dropna().diff(1).abs().rolling(period).sum(),index=df.index,name=df.name)

def ts_normalized_cid_ce(df,period):
    return df.rolling(period).apply(lambda x:np.sqrt(np.dot(np.diff((x - np.nanmean(x)) / np.nanstd(x)),
                                                            np.diff((x - np.nanmean(x)) / np.nanstd(x)))),
                                    raw=True,engine='numba')

def ts_cid_ce(df,period):
    return df.rolling(period).apply(lambda x:np.sqrt(np.dot(np.diff(x), np.diff(x))),raw=True,engine='numba')

def ts_variation_coefficient(df,period):
    return df.rolling(period).std()/df.rolling(period).mean().replace(0,np.nan)

def ts_rms(df,period):
    return df.rolling(period).apply(lambda x:np.sqrt(np.nanmean(np.square(x))),raw=True, engine='numba')

def ts_half_quantile(df,period):
    return df.rolling(period).quantile(0.5)

def ts_75th_quantile(df,period):
    return df.rolling(period).quantile(0.75)

def ts_25th_quantile(df,period):
    return df.rolling(period).quantile(0.25)

def ts_mean_second_derivative_central(df,period):
    return df.rolling(period).apply(lambda x:(x[-1] - x[-2] - x[1] + x[0]) / (2 * (len(x) - 2)) if len(x) > 2 else np.NaN,raw=True)

def ts_mean_change(df,period):
    return df.diff(period)/period

def ts_mean_abs_change(df,period):
    return df.diff(period).abs()/period

def rsi1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(RSI(ts.dropna().values,period),index=time,name=ts.name).reindex(ts.index)

def rsi(df,period=14):
    if len(df.shape) == 2:
        return df.apply(lambda x: rsi1d(x,period) if (~np.isnan(x)).sum()!=0 else x*np.nan)
    else:
        return rsi1d(df,period) if (~np.isnan(df)).sum()!=0 else df*np.nan

def macd1d(ts,fastperiod,slowperiod,signalperiod):
    time = ts[(~np.isnan(ts))].index
    return pd.Series(MACD(ts.dropna().values,fastperiod,slowperiod,signalperiod)[1],index=time,name=ts.name).reindex(ts.index)

def macd(df,fastperiod=12,slowperiod=26,signalperiod=9):
    if len(df.shape) == 2:
        return df.apply(lambda x: macd1d(x,fastperiod,slowperiod,signalperiod) if (~np.isnan(x)).sum()!=0 else x*np.nan)
    else:
        return macd1d(df,fastperiod,slowperiod,signalperiod) if (~np.isnan(df)).sum()!=0 else df*np.nan

def pos1d(ts,period):
    time = ts[(~np.isnan(ts))].index
    tt = ts.loc[time]
    H = tt.rolling(period).max()
    L = tt.rolling(period).min()
    return pd.Series(((tt - L)/(H - L) - 0.5),index=time,name=ts.name).reindex(ts.index)

def pos(df,period):
    if len(df.shape) == 2:
        return df.apply(lambda x: pos1d(x,period))
    else:
        return pos1d(df,period)

def calculate_factor(op,df):
    bi_op = op.split(' ')
    period = int(bi_op[1])
    if bi_op[0] == 'mean':
        return mean(df,period)
    elif bi_op[0] == 'autocorr':
        return autoCORR(df,period)
    elif bi_op[0] == 'autobeta':
        return autoBETA(df,period)
    elif bi_op[0] == 'autointercept':
        return autoINTERCEPT(df,period)
    elif bi_op[0] == 'corr':
        return CORR(df,period)
    elif bi_op[0] == 'beta':
        return BETA(df,period)
    elif bi_op[0] == 'intercept':
        return INTERCEPT(df,period)
    elif bi_op[0] == 'rankcorr':
        return rankCORR(df,period)
    elif bi_op[0] == 'rankbeta':
        return rankBETA(df,period)
    elif bi_op[0] == 'rankintercept':
        return rankINTERCEPT(df,period)
    elif bi_op[0] == 'zscore':
        return zscore(df,period)
    elif bi_op[0] == 'delay':
        return delay(df,period)
    elif bi_op[0] == 'delta':
        return delta(df,period)
    elif bi_op[0] == 'decay_linear':
        return decay_linear(df,period)
    elif bi_op[0] == 'ts_min':
        return ts_min(df,period)
    elif bi_op[0] == 'ts_max':
        return ts_max(df,period)
    elif bi_op[0] == 'ts_argmax':
        return ts_argmax(df,period)
    elif bi_op[0] == 'ts_argmin':
        return ts_argmin(df,period)
    elif bi_op[0] == 'ts_rank':
        return ts_rank(df,period)
    elif bi_op[0] == 'lowday':
        return lowday(df,period)
    elif bi_op[0] == 'highday':
        return highday(df,period)


if __name__ == '__main__':
    idi_vol = pd.read_parquet(f'/mnt/Data/rpan/stock_alpha/idi_vol/idi_vol_ols{10}.parquet').dropna(how='all')
    idi_rate = pd.read_parquet(f'/mnt/Data/rpan/stock_alpha/idi_vol/idi_rate_ols{10}.parquet').dropna(how='all')
    const = pd.DataFrame(np.ones_like(idi_vol),index=idi_vol.index,columns=idi_vol.columns)
    params_vol = FamaMacBeth(np.log(idi_vol)**3,[const,np.log(idi_vol)])