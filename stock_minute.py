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



class Minter_Klines(Thread):
    '''股票分时计算'''
    
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, daemon=None):
        Thread.__init__(self, group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                                user=config['USER'], passwd=config['PASSWD'],
                                db=config['DB'], charset=config['CHARSET'],
                                cursorclass=config.get('CURSOR', cursors.DictCursor))
        
        self.client = pymongo.MongoClient('123.56.72.57', 27017)
        self.db = self.client['stock']
        self.col = self.db['stock_kline']
        # 今天日期 年月日
        self.last_time = ''
        # 用来存放一分钟K线的上次交易价格
        self.y_close = None
        self.y_amount = None
        self.y_volume = None
        self.result_data = []
        self.date = str(time.strftime("%Y%m%d", time.localtime()))
        
        # code列表
        self.code_list = self.get_all_code()
    
    
    def __del__(self):
        print('match minter klines in finish...')
    
    
    def get_all_code(self):
        '''获取所有的个股代码
        @param addr: 交易所 sh/sz
        '''
        result_list = []
        addr_list = ['SH', 'SZ']
        for addr in addr_list:
            table = 'stock2.stock_code'
            cursor = self.conn.cursor()
            columns = ['`code`']
            sql = "select %(columns)s from %(table)s where type = '%(type)s' and code regexp '%(addr)s'" % {"table": table, "columns": ", ".join(columns), "type": 'GG', "addr":addr}
            cursor.execute(sql)
            data = cursor.fetchall()
            cursor.close()
            result_list += data
        return result_list
    
    
    def get_data_by_code(self, data):
        '''获取mongo中个股分时数据
         @param data : 查询条件
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
    
    
    def match_minter_kline(self, rest_data):
        '''计算一分钟K线
        @param rest_data: 分时数据
        '''
        if rest_data:
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
                    self.result_data.append(item)
    
    
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
                        # 取出最新分钟K线的收盘价，成交量，成交金额
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
                    volume = float(data_list[-1].get('volume', 0)) - float(self.y_volume) if data_list[-1].get('volume', 0) else 0  # 成交量
                    amount = float(data_list[-1].get('amount', 0)) - float(self.y_amount) if data_list[-1].get('amount', 0) else 0  # 成交额
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
            
            cur = close - topen
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
        while True:
            end_date = str(time.strftime("%H%M", time.localtime())) + "00"
            date = self.date + end_date
            if 1145 < int(end_date) < 1245:
                time.sleep(3700)
            if end_date > '1700':
                break
            # 获取交易所的个股code
            for code_data in self.code_list:
                # 每次循环新的code，将上次交易价格清空
                self.y_close = None
                self.y_amount = None
                self.y_volume = None
                code = code_data.get('code', '')
                # 如果没有上次更新时间，说明是程序刚启动第一条数据
                if not self.last_time:
                    # 判断从mongo中获取数据的条件
                    r = {'code':code, 'tr_time':{'$lt':date}} # 第一次获取数据
                else:
                    r = {'code':code, 'tr_time':{'$gte':self.last_time,'$lt':date}}
                    # 获取mongo中的分时数据
                rest_data = self.get_data_by_code(r)
                print("get_data_by_code for code :: ", code)
                if rest_data:
                    # 计算一分钟K线
                    self.match_minter_kline(rest_data)
                    # 最大缓存数500
                    if len(self.result_data) >= 500:
                        row = self.insert_minter_kline_data(self.result_data)
                        print("insert_minter_kline_data row = %s" % str(row))
                        del self.result_data[:]
            if self.result_data:
                row = self.insert_minter_kline_data(self.result_data)
                print("insert_minter_kline_data row = %s" % str(row))
                del self.result_data[:]
            self.last_time = date
            time.sleep(5)
        
        self.conn.close()
        self.client.close()
    
    
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
        sql_insert = "insert ignore into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}
        
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
    p = Minter_Klines()
    p.run()


