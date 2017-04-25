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
# import math
# from datetime import datetime,timedelta
import pandas as pd
# from WindPy import w
import numpy as np
import tables as tbl
import os
# import seaborn


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

class Particle_K(tbl.IsDescription):
    timestamp=tbl.Int64Col(pos=0)
    p_open=tbl.UInt32Col(pos=1)
    p_high=tbl.UInt32Col(pos=2)
    p_low=tbl.UInt32Col(pos=3)
    p_close=tbl.UInt32Col(pos=4)
    

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


def tableAppend(frame,fileh,group):
    #frame的index不是timestamp类型    
    #存储tick，储存分钟K线
    rec=frame.to_records(index=False,convert_datetime64=True)
    tt=rec[['timestamp','volume','turover','position',\
    'price','ask1','ask2','bid1','bid2','asize1','asize2','bsize1','bsize2']]
    tt=tt.astype([('timestamp', '<i8'), ('volume', '<u4'), ('turover', '<u4'),\
    ('position', '<u4'), ('price', '<u4'), ('ask1', '<u4'), ('ask2', '<u4'), \
    ('bid1', '<u4'), ('bid2', '<u4'), ('asize1', '<u4'), ('asize2', '<u4'), \
    ('bsize1', '<u4'), ('bsize2', '<u4')])
    try:    
        table=fileh.create_table(group,'tick',Particle,'tick data')
        table.append(tt)
       
    except tbl.NodeError:
        print 'tick table is all ready'
    
    #需要剔除11：31到 12：59的 数据 
    frame.set_index(frame.timestamp,inplace=True)
    t1=frame.price.resample('min',how='ohlc')    
    t1['timestamp']=t1.index
    rec1=t1.to_records(index=False,convert_datetime64=True)
    tt1=rec1[['timestamp','open','high','low','close']]
    tt1=tt1.astype([('timestamp','<i8'),('open','<u4'),('high','<u4')\
    ,('low','<u4'),('close','<u4')])
    
    try:
        table=fileh.create_table(group,'min1',Particle_K,'1 minute k bar')
        table.append(tt1)
       
    except tbl.NodeError:
        print '1mintute table is all ready'
        
    fileh.flush()
    
    
def PartitionCreate(fileh,table):
    '''
    检查是否有，如果有则打印，不进行处理
    从上倒下检查，标的名，年，月，日
    如果没有则则创建整个路径
    
    此版本从根目录开始检查
    '''
    ostr='OP'+table.iloc[1].wind_code[:8]
    odate1=pd.to_datetime(table.date.apply(str)).iloc[1]
    #op
    try : 
        ogroup=fileh.get_node(fileh.root_uep+ostr)
    except tbl.NoSuchNodeError:
        ogroup=fileh.create_group(fileh.root,ostr)
    
    #y
    try:
        y_group=ogroup._f_get_child('y'+str(odate1.year))
    except tbl.NoSuchNodeError:
        y_group = fileh.create_group(ogroup,'y'+str(odate1.year))
        
    #m
    try:
        m_group=y_group._f_get_child('m'+str(odate1.month))
    except tbl.NoSuchNodeError:
        m_group=fileh.create_group(y_group,'m'+str(odate1.month))
        
    #d
    try:
        d_group=m_group._f_get_child('d'+str(odate1.day))
    except tbl.NoSuchNodeError:
        d_group=fileh.create_group(m_group,'d'+str(odate1.day))
        
    return d_group
        
def FileIter(root):
    assert os.path.isdir(root)
    result=[]
    for rt,dirs,files in os.walk(root):
        for fl in files:
            result.append(os.path.join(rt,fl))
            
    return result
            

def fetchPartition(fileh,opt,dt):
    #通过 opt和 dt来处理得到
    #opt=‘OP8位数’ dt按datetime来处理
    try:
        ystr='y'+str(dt.year)
        mstr='m'+str(dt.month)
        dstr='d'+str(dt.day)
        gstr=opt+'/'+ystr+'/'+mstr+'/'+dstr
        group=fileh.get_node(fileh.root,gstr)
        return group
    except (KeyError,tbl.NoSuchNodeError):
        return False
    
    return group

def fetchTable(group,tname):
    #取得不同的table表
    #取得逐日的数据，暂时不处理内部问题（如果表太大，无法读入内存再说）
    try:    
        tt=group._f_get_child(tname)
        return tt.read()
    except tbl.NoSuchNodeError:
        return False

def readRange(fileh,opt,st,dt,tname,key=False):
    #key=False 将9：30之前的处理掉-
    #合并返回数据 
    #对输入暂时不进行处理   
    start_date=pd.to_datetime(st)
    end_date=pd.to_datetime(dt)
    opt_name='OP'+str(opt)
    dtt=dtt=pd.date_range(start=start_date,end=end_date)
    #get Par    
    GG=[]
    for ik in dtt:
        tmp=fetchPartition(fileh,opt_name,ik)
        if tmp:
            GG.append(tmp)
    #get df,clean it ,concut it
    DD=pd.DataFrame()
    for jk in GG:
        data=fetchTable(jk,tname)
        df=pd.DataFrame.from_records(data,index=data['timestamp'].\
        astype('datetime64[ns]'),exclude=['timestamp'])
        #filter
        y=df.index[1].year
        m=df.index[1].month
        d=df.index[1].day
        t_filter=((df.index>pd.datetime(y,m,d,9,30))\
        &(df.index<=pd.datetime(y,m,d,11,30))) | (df.index>pd.datetime(y,m,d,13,0))
        DD=pd.concat([DD,df[t_filter]])
    
    return DD
    

__version__= '0.2'

if __name__ == '__main__':
    print 'tsData is ready'
    import sys
    sys.path.append('d:/HUB')

    base_url='D:/temp/h5/'
    #fileh = tbl.open_file(base_url+"test3.h5", mode="a")
    
    fileh=tbl.open_file(base_url+"test3.h5", mode="r")
    