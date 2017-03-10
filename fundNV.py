# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 14:40:26 2017
1 数据处理并合成月度数据
1)将时间收益数据单独导入,进行column的日期化(df2)
2)ffill数据
3)将前部数据信息导入,注意日期化问题(df1)
4) Df1  与   df2 合并,按index合并(join 函数)
5) 增加最初数据点和最后数据点（月度）
	
2 使用resample进行抽样，进行基础统计
1)按月度进行抽样，如果数据点小于18个月的去掉
2）对于符合的基金，按照规模等权重，计算一个拟合的peer group 基金 
3）计算年华收益率，计算波动率（月 ）
4）计算分位数（按年度，11-17年）

@author: yangxs
"""
import pyfolio as pf
import pandas as pd
import matplotlib
import numpy as np
from datetime import datetime
import statsmodels.api as sm
import statsmodels.formula.api as smf


df=pd.read_csv('D:/temp/fund/fund20170215.csv')

df1=pd.read_csv('D:/temp/fund/fund20170215.csv',\
usecols=df.columns[0:7],parse_dates=[2])
df2=pd.read_csv('D:/temp/fund/fund20170215.csv',\
usecols=df.columns[7:df.columns.size])
df2.columns = pd.to_datetime(df2.columns)
#df2.fillna(method='ffill',axis=1,inplace=True)

df2=df2.groupby(df2.columns.to_period('M'), axis=1)\
.apply(lambda x:x.fillna(method='ffill',axis=1))

df_A=df2.groupby(df2.columns.to_period('A'), axis=1).last()
df_M=df2.groupby(df2.columns.to_period('M'), axis=1).last()
df_A_pct=df_A.pct_change(axis=1)
df_M_pct=df_M.pct_change(axis=1)
df_M_std=df_M_pct.groupby((lambda x:x.year),axis=1).std()

#output
fund_month_pct=df1.join(df_M_pct)
fund_year_pct=df1.join(df_A_pct)
fund_year_std=df1.join(df_M_std)

#fileter 18 month
fund_filter18_m=fund_month_pct[df_M.count(axis=1,numeric_only=True)>=18]
fund_filter18_y=fund_year_pct[df_M.count(axis=1,numeric_only=True)>=18]
fund_filter18_std=fund_year_std[df_M.count(axis=1,numeric_only=True)>=18]

#[i for i in pd.period_range(start='2011-1',end='2017-1',freq='M')]


# 计算平均值和std

k1=fund_filter18_y.groupby(by='规模分组').mean()
k2=fund_filter18_std.groupby(by='规模分组').mean()
k1.to_csv('D:/temp/fund/k1.csv')
k2.to_csv('D:/temp/fund/k2.csv')


#qcut
# d.qcut(fund_filter18_y[pd.Period('2012')],5,labels=False)
#按年份划分 
# 分层四大类
# 得到几类分组 fund_filter18_m['规模分组'].unique()
# 筛选分组后 进行cut，然后用cut的值进行group
# 或者尝试apply的 计算方法和多重groupby
# 基金整体排名
lables=['<20%','20%-40%','40%-60%','60%-80%','>80%']
for i in pd.period_range(start='2012',end='2017',freq='A'):
    t13=pd.qcut(fund_filter18_y[i],5,lables)
    fund_filter18_y=fund_filter18_y.join(t13,rsuffix='_q')    
    
#按规模分组，可以尝试一下multiindex
    
#test pyfolio
t16=df_M_pct.loc[481]
t16.index=t16.index.to_timestamp(how='end')
t16.index=t16.index.tz_localize('UTC')
pf.create_returns_tear_sheet(t16[:-3])