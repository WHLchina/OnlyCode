#!/usr/bin/python
# -*- coding: utf-8 -*-


import MySQLdb as mysql
import matplotlib
from MySQLdb import cursors

config = {
    'HOST': '123.56.72.57',
    'PORT': 3306,
    'USER': 'python',
    'PASSWD': 'yoquantpython.',
    'DB':'stock',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}

class Minter_Klines(object):
    def __init__(self, code, num):
        '''
        @param code: 股票代码，
        @param num: 时间间隔：如 6s => 6 
        '''
        # 取出前30分钟或一个小时的K线
        # 包含字段要求 high，low， tr_date, code, yclose, avgclose, avgclose, volume, amount
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                                user=config['USER'], passwd=config['PASSWD'],
                                db=config['DB'], charset=config['CHARSET'],
                                cursorclass=config.get('CURSOR', cursors.DictCursor))
        self.cursor = self.conn.cursor()
        limit = 1800 // num
        self.klines = self.get_data(code, limit)
        self.code = code
    
    
    def get_data(self, code, limit):
        '''获取数据
         @param code : 股票代码
         @param limit : 30m or 60m'''
#         table1 = 'stock.stock_index'
#         table2 = 'stock_feature'
#         sql = "select f.`feature`, k.`tr_time`, k.`open`, k.`high`, k.`low`, k.`close` from %(table1)s as k left join %(table2)s as f on k.`code`=f.`code` and k.`tr_date`=f.`tr_date` and k.`date_type`=f.`date_type` where k.code = '%(code)s' and k.date_type=%(date_type)s order by tr_time desc limit 220" % {"table1":table1, "table2":table2, "code":code, "date_type":date_type}
        sql = "select tr_time, tr_date, close, volume, amount from stock.stock_kline where code = '%(code)s' and date_type = -1 order by tr_time desc limit %(limit)s" % {"code":code, "limit":limit}
        self.cursor.execute(sql)
        klines = self.cursor.fetchall()
        return klines
    
    
    def minter_kline(self):
        if self.klines:
            data = {}
            close_list = [temp.get('close', 0) for temp in self.klines]
            data['open'] = close_list[-1]
            data['close'] = close_list[0]
            data['high'] = max(close_list)
            data['low'] = min(close_list)
            data['tr_date'] = self.klines[0].get('tr_date', '')
            data['yclose'] = self.klines[0].get('yclose', '')
            data['tr_time'] = self.klines[0].get('tr_time', '')
            data['volume'] = sum([temp.get('volume', 0) for temp in self.klines])
            data['amount'] = sum([temp.get('amount', 0) for temp in self.klines])
            
            # 存入数据库
            self.insert_data(data)
    
    
    def insert_data(self, data):
        '''
        @param data: 30m 行情数据
        '''
        table = 'stock.stock_index'
        
        rows = ''
        # 插入字段
        insert_columns = ['`code`', '`open`', '`close`', '`high`', '`low`', '`tr_date`', '`yclose`', '`tr_time`', '`volume`', '`amount`', ]
        
        # SQL 语句
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}
        
        sql = sql_insert
        
        params = [self.code, data.get('open', ''), data.get('close', 0), data.get('high', 0), data.get('low', 0), data.get('tr_date', ''), data.get('yclose', ''), data.get('tr_time', ''), data.get('volume', ''), data.get('amount', ''), ]
          
        try:
            if sql and params:
                rows = self.cursor.execute(sql, params)
        except Exception as e:
            print e
        del params[:], params
        
        return rows
    
    