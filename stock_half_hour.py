#!/usr/bin/python
# -*- coding: utf-8 -*-


import MySQLdb as mysql
import matplotlib
import pymongo
import time


from MySQLdb import cursors
from threading import Thread


config = {
    'HOST': '123.56.72.57',
    'PORT': 3306,
    'USER': 'yoquant',
    'PASSWD': 'yunliangkeji888',
    'DB':'test_stock',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}

# 股市半小时间隔
HALF_HOUR_TIME = ['100000', '103000', '110000', '113000', '133000', '140000', '143000', '150000']
# 对应的时间区间
HALF_HOUR_TIME_DICT = {'103000':['100000','103000' ], 
                       '110000':['103000','110000' ],
                       '113000':['110000','113000' ],
                       '133000':['130000','133000' ],
                       '140000':['133000','140000' ],
                       '143000':['140000','143000' ]
                       }


class Half_Hour_Klines(Thread):
    '''股票行情分时计算'''
    
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, daemon=None):
        Thread.__init__(self, group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        # 链接MySQL数据库
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                                user=config['USER'], passwd=config['PASSWD'],
                                db=config['DB'], charset=config['CHARSET'],
                                cursorclass=config.get('CURSOR', cursors.DictCursor))
        # 链接mongo数据库
        self.client = pymongo.MongoClient('123.56.72.57', 27017)
        self.db = self.client['stock']
        self.col = self.db['stock_kline']
        # 今天日期 年月日
        self.date = str(time.strftime("%Y%m%d", time.localtime()))
#         self.date = "20180209"
        # 用来存放一分钟K线的上次交易价格
        self.y_close = None
        self.y_amount = None
        self.y_volume = None
        
        self.result = []
    
    
    def get_all_code(self, addr):
        '''获取所有的个股代码
        @param addr: 交易所 sh/sz
        '''
        table = 'stock2.stock_code'
        cursor = self.conn.cursor()
        columns = ['`code`']
        sql = "select %(columns)s from %(table)s where type = '%(type)s' and code regexp '%(addr)s'" % {"table": table, "columns": ", ".join(columns), "type": 'GG', "addr":addr}
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        return data
    
    
    def get_data_by_code(self, data):
        '''获取mongo中个股分时数据
         @param data : 查询条件 {}
         '''
        result_data = []
        try:
            result_data = self.col.find(data, {"_id": 0}).sort([("tr_time",1)])
        except Exception as e:
            print(e)
        if result_data:
            return list(result_data)
        else:
            return []
    
    
    def match_minter_kline(self, code, rest_data):
        '''计算一分钟K线
        @param code: 股票代码
        @param rest_data: 分时数据
        '''
        if rest_data:
            result_data = []
            data_dict = {}
            # 提取出所有的分钟，然后分组
            for data in rest_data:
                # 分钟数据
                time_data = data.get('tr_time', '')[8:12]
                if time_data:
                    data_dict.setdefault(time_data, [])
                    data_dict[time_data].append(data)
            
            # 将每分钟的数据，生成K线
            for key, val in data_dict.items():
                item = self.make_kline(data_list=val, date_type='FS', minter=key)
                if item:
                    result_data.append(item)
                    # 缓存100条数据就添加入MySQL
                    if len(result_data) >= 100:
                        row = self.insert_minter_kline_data(result_data)
                        print("insert_minter_kline_data code = %s, row = %s" % (code, str(row)))
                        del result_data [:]
            # 将数据添加入MySQL
            if result_data:
                row = self.insert_minter_kline_data(result_data)
                print("insert_minter_kline_data code = %s, row = %s" % (code, str(row)))     
    
    
    def make_kline(self,data_list, date_type, minter=None, t_time=None):
        '''生成K线数据
        @param data_list: 分时数据
        @param date_type: k线时间类型
        @param minter: 一分钟时间
        @param date_type: 30分钟时间
        '''
        if data_list:
            date_type = date_type  # K线时间类型，FS：分时， 30:30分钟线
            ex_type = 'N'          # 复权类型
            code = data_list[0].get('code','')  # 股票代码
            tr_date = data_list[0].get('tr_date', '')  # 日期
            topen = float(data_list[0].get('close', 0)) if data_list[0].get('open', 0) else 0   # 开盘价
            close = float(data_list[-1].get('close', 0)) if data_list[-1].get('close', 0) else 0  # 收盘价
            high = max([float(data.get('close', 0)) if data.get('close', 0) else 0 for data in data_list])  # 最高价
            low = min([float(data.get('close', 0)) if data.get('close', 0) else 0 for data in data_list])  # 最低价
            
            # 时间是分钟线
            if minter:
                tr_time = tr_date + minter + '00'
                
                # 上次交易价格
                if not self.y_close:
                    
                    # 查找当天最新的一条分钟K线收盘价
                    yclose_data = self.get_last_close(code, tr_date, date_type)
                    
                    if yclose_data:
                        yclose = yclose_data.get("close", "")
                        l_volume = yclose_data.get("volume", "")
                        l_amount = yclose_data.get("amount", "")
                        volume = float(data_list[-1].get('volume', 0)) - float(l_volume) if data_list[-1].get('volume', 0) else 0  # 成交量
                        amount = float(data_list[-1].get('amount', 0)) - float(l_amount) if data_list[-1].get('amount', 0) else 0  # 成交额
                        
                    else:
                        # 如果没有，说明是早上第一分钟，直接取第一条分时数据的yclose
                        yclose = data_list[0].get("yclose", "")
                        volume = float(data_list[-1].get('volume', 0)) if data_list[-1].get('volume', 0) else 0  # 成交量
                        amount = float(data_list[-1].get('amount', 0)) if data_list[-1].get('volume', 0) else 0  # 成交额
                    
                    # 将本次分钟线的收盘价存入self.y_close中，待下一条分钟K线使用
                    self.y_close = close
                    self.y_amount = amount
                    self.y_volume = volume
                    
                else:
                    # 如果有上次交易价格，直接使用
                    yclose = self.y_close
                    volume = float(data_list[-1].get('volume', 0)) - float(self.y_volume) if data_list[-1].get('volume', 0) else 0
                    amount = float(data_list[-1].get('amount', 0)) - float(self.y_amount) if data_list[-1].get('amount', 0) else 0
                    # 将本次计算结果存入属性中，待下条K线使用
                    self.y_close = close
                    self.y_amount = amount
                    self.y_volume = volume
                
            # 时间是30分钟线   
            elif t_time:
                tr_time = t_time
                # 查找当天最新的一条30分钟K线收盘价   
                yclose_data = self.get_last_close(code, tr_date, date_type)
                
                if yclose_data:
                    yclose = yclose_data.get("close", "")
                    l_volume = yclose_data.get("volume", "")
                    l_amount = yclose_data.get("amount", "")
                    volume = float(data_list[-1].get('volume', 0)) - float(l_volume) if data_list[-1].get('volume', 0) else 0  # 成交量
                    amount = float(data_list[-1].get('amount', 0)) - float(l_amount) if data_list[-1].get('amount', 0) else 0  # 成交额
                
                else:
                    # 如果没有，说明是早上第一个30分钟线
                    yclose = data_list[0].get("yclose", "")
                    volume = float(data_list[-1].get('volume', 0)) if data_list[-1].get('volume', 0) else 0  # 成交量
                    amount = float(data_list[-1].get('amount', 0)) if data_list[-1].get('volume', 0) else 0  # 成交额
            
            cur = float(close) - float(topen)
            # K线类型，阳线/阴线/十字
            if cur > 0:
                k_type = 1
            elif cur < 0:
                k_type = -1
            elif cur == 0:
                k_type =0
            wave_cnt = float(close) - float(yclose) 
            wave_range = round((wave_cnt/float(yclose))*100, 4) if yclose else 0  # 涨跌量
            vibrate_range = round(((high - low)/float(yclose))*100, 4) if yclose else 0  # 振幅
            
            item = [code, tr_date, tr_time, date_type, ex_type, k_type, topen, high, low, close, yclose, volume, amount, wave_cnt, wave_range, vibrate_range]
            
            return item
        return None
    
    
    def run(self):
        addr_list = ['SH', 'SZ']
        for addr in addr_list:
            # 获取交易所的个股code
            code_data_list = self.get_all_code(addr)
            print("code_data_list %s" % addr)
            for code_data in code_data_list:
                # 每次循环新的code，将上次交易价格清空
                self.y_close = None
                self.y_amount = None
                self.y_volume = None
                
                code = code_data.get('code', '')
                # 遍历所有的半个小时,如果该时间有K线，跳过，没有，取出，break
                t_time = ''
                emu = ''
                
                for date in HALF_HOUR_TIME:
                    tr_time = self.date + date
                    #  查询是否有该时间的K线
                    rest = self.check_code_data_by_time(code, tr_time)
                    if rest:
                        continue
                    else:
                        t_time = tr_time
                        emu = date
                        break
                
                if t_time:
                    r = {}
                    # 判断从mongo中获取数据的条件
                    if t_time == self.date + '100000':
                        r = {'code':code, 'tr_time':{'$lte':t_time}} # 小于10点
                    elif t_time == self.date + '150000':
                        r = {'code':code, 'tr_time':{'$gt':self.date + '143000'}} # 大于2点30
                    else:
                        temp = HALF_HOUR_TIME_DICT.get(emu, '')
                        if temp:
                            r = {'code':code, 'tr_time':{'$gt':self.date + temp[0],'$lte':self.date + temp[1]}}
                    
                    if r:
                        # 获取mongo中的分时数据
                        rest_data = self.get_data_by_code(r)
                        print("get_data_by_code for code :: ", code)
                        if rest_data:
                            # 计算30分钟K线
                            half_hour_kline = self.make_kline(data_list=rest_data, date_type='30', t_time=t_time)
                            del rest_data[:], rest_data
                            
                            if half_hour_kline:
                                self.result.append(half_hour_kline)
                                # 将30分钟线添加入MySQL
                                if len(self.result) >= 500:
                                    row = self.insert_minter_kline_data(self.result)
                                    print('insert half hour kilne row = %s' % str(row))
                                    del self.result[:]
                            
                            # 删除mongo中本次计算的数据
                            id_list = [data.get('_id', '') for data in rest_data]
                            self.remove_tick_data_in_mongo(id_list)
                else:
                    print("the code every time have kline code = %s" % code)
        
        if self.result:
            row = self.insert_minter_kline_data(self.result)
            print('insert half hour kilne  row = %s' % str(row))
        
        self.conn.close()
        self.client.close()
    
    
    def remove_tick_data_in_mongo(self, id_list):
        '''删除本次执行的分时数据
        @param id_list: 数据id列表
        '''
        try:
            rest = self.col.remove({'_id':{'$in':id_list}})
            if rest:
                print('[Minter_Klines :: remove_tick_data_in_mongo ok')
            else:
                print('[Minter_Klines :: remove_tick_data_in_mongo  wrong')
        except Exception as e:
            
            print("[Minter_Klines :: remove_tick_data_in_mongo  error: %s" % e)
    
    
    def check_code_data_by_time(self, code, tr_time):
        '''查询当前时间是否已有K线
            @param code :股票代码
            @param tr_time： 当前时间
        '''
        cursor = self.conn.cursor()
        table = 'test_stock.stock_klineV2'
        sql = "select * from %(table)s where code = '%(code)s' and tr_time = '%(tr_time)s' and date_type = '30'" % {"table": table, "code": code, "tr_time":tr_time}
        cursor.execute(sql)
        
        data = cursor.fetchone()
        cursor.close()
        return data
    
    
    def get_last_close(self,code, tr_date, date_type):
        ''' 获取上一根30/1分钟K线数据
            @param code:股票代码
            @param tr_date： 日期
            @param date_type： 周期类型
        '''
        cursor = self.conn.cursor()
        table = 'test_stock.stock_klineV2'
        sql = "select close, amount, volume from %(table)s where code = '%(code)s' and tr_date = '%(tr_date)s' and date_type = '%(date_type)s' order by tr_time desc limit 1" % {"table": table, "code": code, "tr_date":tr_date, "date_type":date_type}
        cursor.execute(sql)
        
        data = cursor.fetchone()
        cursor.close()
        return data
    
    
    def insert_minter_kline_data(self, data):
        ''' 添加K线数据入MySQL
        @param data: 计算后的K线数据
        '''
        table = 'test_stock.stock_klineV2'
        
        cursor = self.conn.cursor()
        
        rows = 0
        # 插入字段
        insert_columns = ['`code`','`tr_date`','`tr_time`','`date_type`','`ex_type`','`k_type`','`open`','`high`','`low`','`close`','`yclose`','`volume`','`amount`','`wave_cnt`','`wave_range`','`vibrate_range`']
        
        # SQL 语句
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}
        
        sql = sql_insert
        
        params = data
        
        try:
            if sql and params:
                rows = cursor.executemany(sql, params)
                self.conn.commit()
        except Exception as e:
            print(e)
        del params[:], params
        
        cursor.close()
        
        return rows
    
    
if __name__ == '__main__':
    p = Half_Hour_Klines()
    p.run()


