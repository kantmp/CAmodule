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
import tables as tbl

base_url=''

class Particle(tbl.IsDescription):
    #timestamp = tbl.Time64Col(pos=0)
    timestamp = tbl.Int64Col(pos=0)    
    volume    = tbl.UInt32Col(pos=1)
    turover   = tbl.UInt32Col(pos=2)
    position  = tbl.UInt32Col(pos=3)
    price     = tbl.UInt32Col(pos=4)
    ask1      = tbl.UInt32Col(pos=5)
    ask2      = tbl.UInt32Col(pos=6)
    bid1      = tbl.UInt32Col(pos=7)
    bid2      = tbl.UInt32Col(pos=8)
    asize1    = tbl.UInt32Col(pos=9)
    asize2    = tbl.UInt32Col(pos=10)
    bsize1    = tbl.UInt32Col(pos=11)
    bsize2    = tbl.UInt32Col(pos=12)
    


def fDuplicated2(frame):
    """
    use numpy,more quick
    """
    counts=frame.groupby(by=['time']).time.count()
    step=1000/counts.get_values()
    ar=np.array([],dtype='i8')
    
    for i in step:
        ar=np.append(ar,np.arange(0,999,i))
    
    #new time
    frame['time']=frame.time+pd.Series(ar,index=frame.index)
    frame[u'timestamp_num']=frame.date*100000000+frame.time


def fDuplicated(frame):
    """
    万得宏汇的期权数据因为时间戳的问题，有很多重复的
    以L2 数据为例 可能出现1秒二个或者三个tick的例子
    在一秒内插值
    now slow
    """
    grp=frame.groupby(by=['datetime']).groups
    
    for key,value in grp.items():
        k=len(value) #int
        if k>1:
            frame.loc[value,'time']=frame.loc[value,'time']\
            .add(1000*np.arange(k)/k)
    
    frame[u'timestamp_num']=frame.date*1000000000+frame.time

                
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


def tableAppend(frame,table):
    rec=frame.to_records(index=False,convert_datetime64=True)
    tt=rec[['timestamp','volume','turover','position',\
    'price','ask1','ask2','bid1','bid2','asize1','asize2','bsize1','bsize2']]
    tt=tt.astype([('timestamp', '<i8'), ('volume', '<u4'), ('turover', '<u4'),\
    ('position', '<u4'), ('price', '<u4'), ('ask1', '<u4'), ('ask2', '<u4'), \
    ('bid1', '<u4'), ('bid2', '<u4'), ('asize1', '<u4'), ('asize2', '<u4'), \
    ('bsize1', '<u4'), ('bsize2', '<u4')])
    table.append(tt)
    table.flush()
         