#!/usr/local/anaconda2/bin/python
#encoding:utf-8
import sys,os
sys.path.append('/usr/local/anaconda2')
os.environ['PYSPARK_PYTHON'] = '/usr/local/anaconda2/bin/python'
from pyspark.sql.functions import collect_list, struct
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

import datetime
import pandas as pd
import numpy as np
import math
import datetime
import decimal

# 大数据工具类
from common.tools import MysqlRW
'''
载货率
根据满半空持续时间，计算各自占比
'''

datestr = sys.argv[1] if len(sys.argv)>=2 else (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# 删除需要重跑日期的mysql数据，避免重复
MysqlRW.read('sirius', 'delete from cache_track_loadfactor_rate where pdt="%s"'%datestr)

sc = SparkSession.builder.master("yarn").\
    appName("loadfactor").\
    config("spark.yarn.queue", "online").\
    enableHiveSupport().getOrCreate()

# conf = SparkConf().setMaster('local')
# sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)


# 匹配载重判断顺序
# 特殊车 -> 牵引车 -> 其他车
# 牵引车
QianQi_Load = {
    u'6×4':[20, 35],
    u'6×2':[20, 35],
    u'4×2':[17, 35],
    u'8×4':[15, 25],
    u'8×2':[15, 25]
}

# 特殊车
Spe_Load = {
    (u'4×2', u'虎V'): [3, 6],
    (u'4×2', u'J6F'): [3, 6],
}

# 载货，自卸，轻卡
Other_Load = {
    u'6×4':[14, 20],
    u'6×2':[12, 19],
    u'4×2':[5, 10],
    u'8×4':[15, 25],
    u'8×2':[15, 25],
    u'4×4':[3.5, 4.2],
}

'''
输出：
    6×4牵引车，悍V，30
输出：
    载重状态
'''
def load_factor(car_model, car_series, payload):
    car_wheel, car_type = car_model[:3], car_model[3:]
    # 如果车辆是特殊车
    if car_series in (u'虎V', u'J6F') and car_wheel==u'4×2':
        load = [3, 6]
    else:
        if car_type == u'牵引车':
            load = QianQi_Load.get(car_wheel)
        else:
            load = Other_Load.get(car_wheel)
    # 无法获取载重表
    if not load:
        return u'未知'
    # 满载
    if payload>load[1]:
        return u'满载'
    elif payload>load[0] and payload<=load[1]:
        return u'半载'
    else:
        return u'空载'

def sum_duration(tpdf):
    try:
        duration = round(sum(v[1] - v[0] for v in zip(tpdf['trip_starttime'], tpdf['trip_endtime'])) / 3600.0, 2)
    except:
        duration = -999
    pdt = tpdf['pdt'].tolist()[0]
    car_plat = tpdf['car_plat'].tolist()[0]
    load_status = tpdf['load_status'].tolist()[0]
    return [car_plat, load_status, duration, pdt]

# 归一化ratio
def normalizing_ratio(ratios):
    ratios_list = []
    ratio_sum = 0
    for index, v in enumerate(ratios):
        if index==len(ratios)-1:
            ratios_list.append(1-ratio_sum)
        else:
            ratio = round(v, 4)
            ratios_list.append(ratio)
            ratio_sum += ratio
    return ratios_list

# 计算某系的信息
def cal_loadstatus(rowdata):
    df = pd.DataFrame(rowdata[2], columns=['vid',
                                       'car_series',
                                       'car_model',
                                       'car_plat',
                                       'payload',
                                       'trip_starttime',
                                       'trip_endtime',
                                       'pdt'])

    df['payload'] = df['payload'].astype(float)
    df['trip_starttime'] = df['trip_starttime'].astype(float)
    df['trip_endtime'] = df['trip_endtime'].astype(float)
    # 获取满半空
    df['load_status'] = df.apply(lambda row: load_factor(row['car_model'], row['car_series'], row['payload']), axis=1)
    # 过滤掉 "未知" 载重状态
    df = df[np.where(df['load_status']==u'未知', False, True)]
    df['trip_duration'] = df['trip_endtime'] - df['trip_starttime']
    rdf = df.groupby('load_status').agg({
        'load_status':'max',
        'car_plat':'max',
        'trip_duration':'sum',
        'pdt':'max'
    })
    total_duration = float(rdf['trip_duration'].sum())
    rdf['ratio'] = rdf['trip_duration']/total_duration
    rdf['ratio'] = normalizing_ratio(rdf['ratio'].values)
    resdf = pd.DataFrame(rdf, columns=['car_plat', 'load_status', 'trip_duration', 'ratio', 'pdt'])
    # 将时间转换为小时
    resdf['trip_duration'] = round(resdf['trip_duration']/3600.0, 2)
    return resdf.values.tolist()

def main():
    sql = '''
    select
        t2.vid,
        t2.car_series,
        t2.car_model,
        t2.car_plat,
        t1.payload,
        t1.trip_starttime,
        t1.trip_endtime,
        t1.pdt
    from dw_warehouse.dws_lc_payload_phi t1
    join dim.dim_car_info t2
    on t1.terminalid=t2.vid
    where t1.pdt='%s'
    '''%datestr

    data = hiveContext.sql(sql).groupBy(['pdt','car_plat']).agg(collect_list(struct('vid',
                                                               'car_series',
                                                               'car_model',
                                                               'car_plat',
                                                               'payload',
                                                               'trip_starttime',
                                                               'trip_endtime',
                                                               'pdt')))


    load_rdd = data.rdd.map(lambda x: cal_loadstatus(x)).flatMap(lambda x: x)
    #写入mysql
    # mysql shema，与mysql字段名和类型保持一致
    schema = StructType([StructField('car_series', StringType()),
                         StructField('load_status', StringType()),
                         StructField('duration', DoubleType()),
                         StructField('ratio', DoubleType()),
                         StructField('pdt', StringType())
                         ])
    mysql_uri = "jdbc:mysql://192.168.135.219:3306/sirius?user=Sirius&password=72kVFvXvCpWI76oHQIGJsRw6"
    # 不支持自动重跑，如需重跑，需要手动删除mysql相应数据，否则会重复
    load_rdd.toDF(schema=schema).write.jdbc(mode="append", url=mysql_uri, table="cache_track_loadfactor_rate",properties={"driver":"com.mysql.jdbc.Driver"})



main()
















