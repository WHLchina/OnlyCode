# coding: utf-8

# 股票实时行情 Level01 DBF文件解析工具(Python3.+)
# version 2.0.20180524
# update 2018-05-24 14:50:00 PM

# requirement.txt
# dbfread==2.0.7
# pymongo==3.6.1

import pymongo
import time
import datetime
import os

from multiprocessing import Process
from dbfread import DBF
from read_dbf import dbfreader


# 测试开关
DEBUG = False


# MONGODB连接配置信息
MONGODB_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "db": "stock",
    "table": "stock_kline",
}


# 股票Level01实时行情DBF文件
DBF_FILES = {
    'SH': './genius_level01/mktdt00.txt',  # 上海交易所实时行情DBF文件
    'SZ': './genius_level01/sjshq.dbf',  # 深圳交易所实时行情DBF文件
}


# 今日日期
TODAY = time.strftime("%Y%m%d", time.localtime())

# 中午开始休息时间
SLEEP_START = 114500

# 中午结束休息时间
SLEEP_END = 124500

# 中午休息间隔
SLEEP = 3700


def inster_mongo_data(data):
    """添加mongodb数据
    @param data 分时数据列表
    @return: 已添加数据条数"""
    # 建立连接
    client = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
    db = client[MONGODB_CONFIG['db']]
    col = db[MONGODB_CONFIG['table']]
    rest = 0
    try:
        rest = col.insert_many(data, ordered=False)
        if rest:
            print('[StockDBFUtil] insert_mongo_data ok.')
    except Exception as e:
        print("[StockDBFUtil] insert_mongo_data failed! Error:", e)
    finally:
        client.close()
    return rest


def remove_all_data():
    """删除所有数据"""
    
    # 建立连接
    client = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
    db = client[MONGODB_CONFIG['db']]
    col = db[MONGODB_CONFIG['table']]
    rest = 0
    try:
        rest = col.remove({'tr_date':{"$gt":'1'}})
        if rest:
            print('[StockDBFUtil] remove_mongo_data ok.')
    except Exception as e:
        print("[StockDBFUtil] remove_mongo_data failed! Error:", e)
    finally:
        client.close()


def get_stock_sz_kline_data():
    """解析深市实时行情数据，添加入mongoDB"""
    
    NOW_TIME = '101734'
    tr_date = ''
    cur = ''
    times = ''
    ex = 1
    while True:
        # 判断中午休市时间
        wait_time = time.strftime("%H%M%S", time.localtime())
        if SLEEP_START < int(wait_time) < SLEEP_END:
            time.sleep(SLEEP)
        
        start = time.clock()
        file_name = DBF_FILES.get('SZ')
        f = open(file_name, 'rb')
        fsize = os.path.getsize(file_name)
        # 判断文件大小，如果为零，则跳过
        if not fsize:
            continue
        
        # 解析dbf文件
        db = dbfreader(f)
        result_list = []
        for record in db:
            temp = record[0]
            # 去除字段信息，字段信息类型为元组
            if isinstance(temp, tuple):
                continue
            
            # 解析头信息
            if temp == b'000000':
                tr_date = record[1].decode('GBK').strip()  # 日期
                cur = record[5].decode('GBK').strip()  # 是否开市
                times = record[7].decode('GBK').strip()  # 时间
                ex_zs = record[2].decode('GBK').strip().split('\x00')[0]
                ex = float(ex_zs)  # 指数系数
            # cur:0 开市， cur:1 闭市
            elif cur == '0' and times != NOW_TIME and tr_date == TODAY and times:
                if temp.startswith(b'000') or temp.startswith(b'002') or temp.startswith(b'300') or temp.startswith(b'399'):
                    
                    data = {}
                    
                    if temp.startswith(b'399'):
                        # 标记为 深圳 指数
                        code = temp.decode('GBK').strip() + '.SZSH.ZS'
                        yclose = float(record[2].decode('GBK').strip().replace(" ", '')) * ex  # 股票昨日收盘价
                        topen = float(record[3].decode('GBK').strip().replace(" ", '')) * ex  # 股票开盘价
                        close = float(record[4].decode('GBK').strip().replace(" ", '')) * ex  # 股票收盘价
                        high = float(record[8].decode('GBK').strip().replace(" ", '')) * ex  # 股票最高价
                        low = float(record[9].decode('GBK').strip().replace(" ", '')) * ex  # 股票最低价
                    else:
                        # 个股
                        code = temp.decode('GBK').strip() + '.SZ.GG'
                        yclose = float(record[2].decode('GBK').strip().replace(" ", ''))  # 股票昨日收盘价
                        topen = float(record[3].decode('GBK').strip().replace(" ", ''))  # 股票开盘价
                        close = float(record[4].decode('GBK').strip().replace(" ", ''))  # 股票收盘价
                        high = float(record[8].decode('GBK').strip().replace(" ", ''))  # 股票最高价
                        low = float(record[9].decode('GBK').strip().replace(" ", ''))  # 股票最低价
                    
                    name = record[1].decode('GBK').strip()  # 股票简称
                    volume = float(record[5].decode('GBK').strip())  # 股票成交量
                    amount = float(record[6].decode('GBK').strip())  # 股票成交金额
                    now = tr_date[:4] + '-' + tr_date[4:6] + '-' + tr_date[6:] + ' ' + times[:2] + ':' + times[2:4] + ':' + times[4:6]
                    tr_time = tr_date + times[:6]  # 时间
                    
                    data['code'] = code
                    data['tr_date'] = tr_date
                    data['tr_time'] = tr_time
                    data['date_type'] = -1
                    data['ex_type'] = 0
                    data['name'] = name.strip()
                    data['yclose'] = yclose if yclose else 0
                    data['open'] = topen if topen else 0
                    data['close'] = close if close else 0
                    data['volume'] = volume if volume else 0
                    data['amount'] = amount if amount else 0
                    data['high'] = high if high else 0
                    data['low'] = low if low else 0
                    data['market'] = 'SZ'
                    data['abhuj'] = 'A'
                    data['remark'] = ''
                    data['time'] = now
                    # 指定字段addr 说明这是深圳交易所
                    data['addr'] = 'SZ'
                    
                    if DEBUG:
                        print("[StockDBFUtil] 股票实时行情")
                        print('[股票代码] =', code)
                        print('[股票简称] =', name)
                        print('[tr_date] =', tr_date)
                        print('[tr_time] =', name)
                        print('[股票昨日收盘价] =', yclose)
                        print('[股票开盘价] =', topen)
                        print('[股票收盘价] =', close)
                        print('[股票成交量] =', volume)
                        print('[股票成交金额] =', amount)
                        print('[股票最高价] =', high)
                        print('[股票最低价] =', low)
                        print('[股票时间] =', now)
                        print('___________________________________________________________')
                    else:
                        if topen <= float('0.000000'):
                            continue
                        result_list.append(data)
                        # 一次最多插入500条数据
                        if len(result_list) >= 500:
                            row = inster_mongo_data(result_list)
                            print('[StockDBFUtil] SZ inster_mongo_data row = %d' % len(result_list))
                            del result_list[:]
        if result_list:
            row = inster_mongo_data(result_list)
            print('[StockDBFUtil] SZ inster_mongo_data row = %d' % len(result_list))
        
        print('[StockDBFUtil] SZ TIME BY FILE:', times)
        print('[StockDBFUtil] SZ NOE TIME BY CACHE:', NOW_TIME)
        
        NOW_TIME = times
        if cur == '1' and times == NOW_TIME and tr_date == TODAY:
            print('[StockDBFUtil] get_stock_sz_kline_data is finish')
            break
        
        # 打印程序运行时间
        end = time.clock()
        print('[StockDBFUtil] get_stock_sz_kline_data run time:', end - start)
        
        f.close()
        time.sleep(0.8)


def check_sh_data(data):
    """检查上海交易所分时mongodb数据
    @param data 分时数据列表"""
    # 建立连接
    client = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
    db = client[MONGODB_CONFIG['db']]
    col = db[MONGODB_CONFIG['table']]
    try:
        rest = col.find(data)
        if rest:
            return list(rest)
        else:
            return None
    except Exception as e:
        print(e)
    finally:
        client.close()


def get_stock_sh_kline_data():
    """解析沪市实时行情数据，添加入mongoDB"""
    
    NOW_TIME = '20180124-10:17:19.944'
    while True:
        # 判断中午休市时间
        wait_time = time.strftime("%H%M%S", time.localtime())
        if SLEEP_START < int(wait_time) < SLEEP_END:
            time.sleep(SLEEP)
        
        # 程序开始时间
        start = time.clock()
        
        file_name = DBF_FILES.get('SH')
        with open(file_name, 'rb') as f:
            data = f.readline()
            if data:
                headers = str(data).split('|')
                # 获取日期
                loacl_time = time.strftime("%Y%m%d-%H:%M:%S", time.localtime())
                times = headers[6] if len(headers) > 6 else loacl_time
                tr_date = times.split('-')[0]
                # 判断是否为结束符号。 1：通讯， 0：结束
                cur = headers[-1].replace(' ', '').replace("'", '')[-3]
                
                result_list = []
                if TODAY == tr_date and times != NOW_TIME :
                    print('[StockDBFUtil] SH TIME BY FILE:', times)
                    print('[StockDBFUtil] SH NOW TIME BY CACHE:', NOW_TIME)
                    NOW_TIME = times
                    data_list = f.readlines()
                    for item in data_list:
                        try:
                            item = item.decode('gb18030')
                        except Exception as e:
                            print("[StockDBFUtil] SH ITEM IS ERROE:", e)
                            continue
                        # 判断是否为指数或个股
                        if item.startswith('MD001') or item.startswith('MD002'):
                            data = {}
                            temp_list = item.split('|')
                            if len(temp_list) < 10:
                                continue
                            code_data = temp_list[1]
                            if item.startswith('MD001'):
                                # 标记为上海指数
                                code = code_data + '.SZSH.ZS'
                            else:
                                code = code_data + '.SH.GG'
                            
                            name = temp_list[2]  # 股票简称
                            yclose = float(temp_list[5].strip())  # 股票昨日收盘价
                            topen = float(temp_list[6].strip())  # 股票开盘价
                            close = float(temp_list[9].strip())  # 股票收盘价
                            volume = float(temp_list[3].strip())  # 股票成交量
                            amount = float(temp_list[4].strip())  # 股票成交金额
                            high = float(temp_list[7].strip())  # 股票最高价
                            low = float(temp_list[8].strip())  # 股票最低价
                            ftime = temp_list[-1].replace('\n', '').split('.')[0]
                            # 拼接时间
                            now = tr_date[:4] + '-' + tr_date[4:6] + '-' + tr_date[6:] + ' ' + ftime
                            tr_time = tr_date + ftime.replace(':', '')
                            # hl_range 暂不处理
                            # hl_range = round(((high - low)/ close)*100, 4) if close else 0  # 最高价与最低价波动幅度
                            
                            data['code'] = code
                            data['tr_date'] = tr_date
                            data['tr_time'] = tr_time
                            data['date_type'] = -1
                            data['ex_type'] = 0
                            data['name'] = name.strip()
                            data['yclose'] = yclose if yclose else 0
                            data['open'] = topen if topen else 0
                            data['close'] = close if close else 0
                            data['volume'] = volume if volume else 0
                            data['amount'] = amount if amount else 0
                            data['high'] = high if high else 0
                            data['low'] = low if low else 0
                            data['market'] = 'SH'
                            data['abhuj'] = 'A'
                            data['remark'] = ''
                            data['time'] = now
                            # 指定字段addr 说明这是上海交易所
                            data['addr'] = 'SH'
                            
                            if DEBUG:
                                print("[StockDBFUtil] 股票实时行情")
                                print('[股票代码] =', code)
                                print('[股票简称] =', name)
                                print('[tr_date] =', tr_date)
                                print('[tr_time] =', name)
                                print('[股票昨日收盘价] =', yclose)
                                print('[股票开盘价] =', topen)
                                print('[股票收盘价] =', close)
                                print('[股票成交量] =', volume)
                                print('[股票成交金额] =', amount)
                                print('[股票最高价] =', high)
                                print('[股票最低价] =', low)
                                print('[股票时间] =', now)
                                print('___________________________________________________________')
                            else:
                                if topen <= 0:
                                    continue
                                result_list.append(data)
                                # 一次最多插入500条数据
                                if len(result_list) >= 500:
                                    row = inster_mongo_data(result_list)
                                    # 打印程序运行时间
                                    print('[StockDBFUtil] SH inster_mongo_data row = %d' % len(result_list))
                                    del result_list[:]
                    
                    if result_list:
                        row = inster_mongo_data(result_list)
                        print('[StockDBFUtil] SH inster_mongo_data row = %d' % len(result_list))
                
                # 代表股市已闭市
                if cur == '1' and times == NOW_TIME:
                    print('[StockDBFUtil] get_stock_sh_kline_data is finish')
                    break
        
        # 打印程序运行时间
        end = time.clock()
        print('[StockDBFUtil] get_stock_sh_kline_data run time:', end - start)
        
        time.sleep(1)


if __name__ == '__main__':
    
    print('--------------------------------------------------------------------------------------start delete db', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # 清空MongoDB，删除所有数据
    remove_all_data()
    print('-------------------------------------------------------------------------------------delete db finish', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # 解析沪市实时行情数据，添加入mongoDB
    p = Process(target=get_stock_sh_kline_data)
    # 解析深市实时行情数据，添加入mongoDB
    f = Process(target=get_stock_sz_kline_data)
    
    p.start()
    f.start()
    
    p.join()
    f.join()

