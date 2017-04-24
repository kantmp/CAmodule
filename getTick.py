# -*- coding: utf-8 -*-

'''
get the option tick
格式为
gettick tick.csv
'''
import tables as tbl
import os
import tsData
import pandas as pd
import sys
import getopt

#
__version__ = '0.1'
baseurl = os.getcwd()


def openHDFfile(hdf_file):
    '''
    open the hdf5 file
    need in the cwd
    '''

    try:
        fileh = tbl.open_file(baseurl+'\\' + hdf_file, mode='r')
        return fileh
    except Exception, e:
        print Exception, ":", e


def readOPfile(fileh, input_file):
    '''
    read op file ***.csv
    csv is column  op_num | start_date | end_date
    use pandas
    read hdf5
    put it to csv
    '''
    try:
        baseurl = os.getcwd()
        df = pd.read_csv(baseurl+'\\' + input_file)
    except Exception, e:
        print Exception, ":", e
    print df
    for index,  row in df.iterrows():
        DD = tsData.readRange(fileh, row.op_num, row.start_date, row.end_date, 'tick')
        # print DD
        DD.to_csv(baseurl + '\\' + str(row.op_num)+'_' + str(index) + '.csv')


def main(argv):
    '''
    main 函数，解析argv
    need to close hdf5
    '''
    try:
        opts, args = getopt.getopt(argv[1:], "i:f:")
        # print args
    except getopt.GetoptError:
        sys.exit()
    # 在工作文件夹下面的相对地址
    for name, value in opts:
        if name in ("-i"):
            print "option list is " + baseurl + "\\" + value
            input_file = value
        if name in ("-f"):
            print "database is " + baseurl + "\\" + value
            hdf_file = value

    fileh = openHDFfile(hdf_file)
    readOPfile(fileh, input_file)


if __name__ == '__main__':
    print 'getTick is start'
    main(sys.argv)
    print "getTick is end "

