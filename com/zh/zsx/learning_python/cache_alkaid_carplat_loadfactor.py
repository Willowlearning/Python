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
from common.tools import MysqlRW, ConnToMysql

'''
油耗分析大屏:各车型油耗统计（分马力，分车型）
增加空载率
'''
datestr = sys.argv[1] if len(sys.argv)>=2 else (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
appname = 'cache_alkaid_carplat_loadfactor_%s'%datestr
sc = SparkSession.builder.master("yarn").\
    appName(appname).\
    config('hive.exec.dynamic.partition.mode', 'nonstrict').\
    config("spark.sql.shuffle.partitions",200).\
    config("spark.memory.fraction",0.7).\
    config("spark.memory.storageFraction",0.1).\
    config("spark.yarn.am.memory",1024*3).\
    config("spark.yarn.queue", "default").\
    enableHiveSupport().getOrCreate()

hiveContext = HiveContext(sc)


# 获取需要计算空载率的数据信息
carsql = 'select id, car_series, car_plat, ps, car_model from alkaid.cache_ps_series_pdi where pdt="%s"'%(datestr)
carplat_infos = MysqlRW.read('cobra_online', carsql)
carplat_cache = {}
vid_id_cache = {}
for car in carplat_infos:
    id, car_series, car_plat, ps, car_model = car
    # print id, car_series, car_plat, ps, car_model
    # 根据平台、品系、马力、驱动形式查询对应的车辆vid
    vids_sql = "select DISTINCT vid from dim.dim_car_info where car_series='{car_plat}' and car_plat='{car_series}' and ps='{ps}' and car_model like '%{car_model}%'".format(car_series=car_series, car_plat=car_plat, ps=ps, car_model=car_model)
    vids = MysqlRW.read('cloud_37', vids_sql)
    for vid in vids:
        vid_id_cache[vid[0]] = id
    carplat_cache[id] = [car_series, car_plat, ps, car_model]

# 广播数据
carplat_dict = sc.sparkContext.broadcast(carplat_cache)
vid_id_dict = sc.sparkContext.broadcast(vid_id_cache)

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

# 获取对应车辆归属到alkaid.cache_ps_series_pdi哪个id
def get_vid_id(vid):
    return vid_id_dict.value.get(vid)

def get_id_info(id):
    return carplat_dict.value.get(int(id))

# 过滤出需要的数据，并且为数据打上标签
def filter_need(row):
    vid, car_series, car_model, car_plat, payload, trip_starttime, trip_endtime, pdt = row
    id = get_vid_id(vid)
    if not id:
        return []
    return [id, car_series, car_model, car_plat, payload, trip_starttime, trip_endtime, pdt]

def process(x):
    id = x[0]
    pdt = x[1]
    df = pd.DataFrame(x[2], columns=['id',
                                     'car_series',
                                     'car_model',
                                     'car_plat',
                                     'payload',
                                     'trip_starttime',
                                     'trip_endtime',
                                     'pdt'])
    df['payload'] = df['payload'].astype(float)
    df['trip_starttime'] = df['trip_starttime'].astype(int)
    df['trip_endtime'] = df['trip_endtime'].astype(int)
    # 获取满半空
    df['load_status'] = df.apply(lambda row: load_factor(row['car_model'], row['car_series'], row['payload']), axis=1)
    # 过滤掉 "未知" 载重状态
    df = df[np.where(df['load_status'] == u'未知', False, True)]
    df['trip_duration'] = df['trip_endtime'] - df['trip_starttime']

    total_duration = float(sum(df['trip_duration']))
    kz_duration = float(sum(df[np.where(df.load_status==u'空载', True, False)]['trip_duration']))
    kz_load_factor = round(kz_duration/total_duration, 4)
    car_series, car_plat, ps, car_model = get_id_info(id)
    return [id, car_series, car_plat, ps, car_model, kz_load_factor, pdt]

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

schema = StructType([StructField('id', StringType()),
                     StructField('car_series', StringType()),
                     StructField('car_model', StringType()),
                     StructField('car_plat', StringType()),
                     StructField('payload', StringType()),
                     StructField('trip_starttime', StringType()),
                     StructField('trip_endtime', StringType()),
                     StructField('pdt', StringType())
                     ])

# tt = hiveContext.sql(sql)
# import pdb;pdb.set_trace()

tdf = hiveContext.sql(sql).rdd.map(lambda x: filter_need(x)).filter(lambda x: len(x)>0).toDF(schema=schema).groupBy(['id', 'pdt']).agg(collect_list(struct('id',
                                                                                                                                             'car_series',
                                                                                                                                             'car_model',
                                                                                                                                             'car_plat',
                                                                                                                                             'payload',
                                                                                                                                             'trip_starttime',
                                                                                                                                             'trip_endtime',
                                                                                                                                             'pdt')))

# 获取空载率数据
kz_datas = tdf.rdd.map(lambda x: process(x)).collect()

# 更新mysql数据库的空载率
for kd in kz_datas:
    id, car_series, car_plat, ps, car_model, kz_load_factor, pdt = kd
    update_kz_sql = 'update alkaid.cache_ps_series_pdi set empty_loading_ratio=%s where id=%s'%(kz_load_factor, id)
    MysqlRW.read('cobra_online', update_kz_sql)

