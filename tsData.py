# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 19:46:01 2016
deal with time series data
use pytable,pandas
File  HDF5

1 clean the data
2 save as HDF5 file
3 add new data
4 get data
5 example liquide line

@author: yangxs
"""
import math
from datetime import datetime,timedelta
import pandas as pd
from WindPy import w
import numpy as np
import tables
def fDuplicated(frame):
    """
    万得宏汇的期权数据因为时间戳的问题，有很多重复的
    以L2 数据为例 可能出现1秒二个或者三个tick的例子
    在一秒内插值
    """
    grp=frame.groupby(by=['datetime']).groups
    
    for key,value in grp.items():
        k=len(value) #int
        if k>1:
            frame.loc[value,'time']=frame.loc[value,'time']\
            .add(1000*np.arange(k)/k)

                
def fDelErrorData(frame):
    '''
    删除9：15之前的以及15：00之后的数据
    ''' 
    frame.drop(frame[frame.time<91500000].index,inplace=True)
    
    
def fCreatTimestamp(frame,name):    
    '''
    将生成timestamp,date+time
    一般datetime  n_timestamp
    '''
    frame[name]=pd.to_datetime((frame.date.apply(str)+frame.time.apply(str)),\
    format='%Y%m%d%H%M%S%f')
    
    