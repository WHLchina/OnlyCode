#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mysql
from MySQLdb import cursors
from sshtunnel import SSHTunnelForwarder  

'''
准确率：变盘点和拐点同时出现的个数/变盘点的个数
覆盖率: 变盘点和拐点同时出现的个数/拐点的个数
计算方法：

读出一个股票所有k线，依次查找是否有变盘信号，如果有变盘点+1，并在该K线为中心，向两边各找5根k线，如果存在拐点，
则准确的个数+1.直到窗口右边界到达当前k线。准确率=准确的个数/变盘点个数。
    
读出一个股票所有k线，依次查找是否有拐点，如果有拐点+1，并在该K线为中心，向两边各找5根k线，
如果存在变盘信号，则准确的个数+1.直到窗口右边界到达当前k线。覆盖率=准确的个数/拐点个数。
'''

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


class Rate(object):
    '''准确率/覆盖率'''
    def __init__(self, code, date_type):
        self.code = code
        self.date_type = date_type
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                                user=config['USER'], passwd=config['PASSWD'],
                                db=config['DB'], charset=config['CHARSET'],
                                cursorclass=config.get('CURSOR', cursors.DictCursor))
        self.cursor = self.conn.cursor()
        self.klines = self.get_data()   
        # 根据type取出所有K线 从左侧往右移， 字段要求有 feature
        
        self.__length = len(self.klines)
        
    
    def get_data(self):
        sql = "select feature from stock.stock_feature where code = '%(code)s' and date_type=%(date_type)s and ex_type=0 order by tr_date" % {"code":self.code, "date_type":self.date_type}
        self.cursor.execute(sql)
        klines = self.cursor.fetchall()
        return klines
    
    
    def precision_coverage(self):
        # 准确的个数
        buy_precise = 0
        # 变盘点
        buy_change = 0
        # 拐点个数
        buy_guai = 0
        # 准确的个数
        buy_precise_guai = 0
        
        # 准确的个数
        sell_precise = 0
        # 变盘点
        sell_change = 0
        # 拐点个数
        sell_guai = 0
        # 准确的个数
        sell_precise_guai = 0
        
        if self.klines:
            # 带下标遍历所有的K线
            for index, temp in enumerate(self.klines):
                feature = temp.get('feature', '')
                # 判断是否是变盘点  buy
                if feature.count('S106') or feature.count('S406'):
                    buy_change += 1
                    # 以此为中心，左右各找5根K线
                    if index <= 5:
                        left = self.klines[:index]
                        if index == 0:
                            left = [{'feature':''}]
                    else:
                        left = self.klines[index-5:index]
                    if index >= self.__length - 5:
                        right = self.klines[index:self.__length]
                        if index == self.__length:
                            right = [{'feature':''}]
                    else:
                        right = self.klines[index:index+5]
                    # 判断是否有拐点
                    features = ','.join([temp.get('feature', '') for temp in left]) + ',' + ','.join([temp.get('feature', '') for temp in right])
                    if features.count('S131') or features.count('S133') or features.count('S135') or features.count('S137') or features.count('S139'):
                        buy_precise += 1
                    
                # 判断是否是变盘点  sell    
                elif feature.count('S107') or feature.count('S407'):
                    sell_change += 1
                    # 以此为中心，左右各找5根K线
                    if index <= 5:
                        left = self.klines[:index]
                        if index == 0:
                            left = [{'feature':''}]
                    else:
                        left = self.klines[index-5:index]
                    if index >= self.__length - 5:
                        right = self.klines[index:self.__length]
                        if index == self.__length:
                            right = [{'feature':''}]
                    else:
                        right = self.klines[index:index+5]
                    # 判断是否有拐点
                    features = ','.join([temp.get('feature', '') for temp in left]) + ',' + ','.join([temp.get('feature', '') for temp in right])
                    if features.count('S130') or features.count('S132') or features.count('S134') or features.count('S136') or features.count('S138'):
                        sell_precise += 1
                        
                # 判断是否是拐点
                if feature.count('S131') or feature.count('S133') or feature.count('S135') or feature.count('S137') or feature.count('S139'):
                    buy_guai += 1
                    # 以此为中心，左右各找5跟K线
                    if index <= 5:
                        left = self.klines[:index]
                        if index == 0:
                            left = [{'feature':''}]
                    else:
                        left = self.klines[index-5:index]
                    if index >= self.__length - 5:
                        if index == self.__length:
                            right = [{'feature':''}]
                        right = self.klines[index:self.__length]
                    else:
                        right = self.klines[index:index+5]
                    features = ','.join([temp.get('feature', '') for temp in left]) + ',' + ','.join([temp.get('feature', '') for temp in right])
                    # 判断是否是变盘点
                    if features.count('S106') or features.count('S406'):
                        buy_precise_guai += 1
                elif feature.count('S130') or feature.count('S132') or feature.count('S134') or feature.count('S136') or feature.count('S138'):
                    sell_guai += 1
                    # 以此为中心，左右各找5跟K线
                    if index <= 5:
                        left = self.klines[:index]
                        if index == 0:
                            left = [{'feature':''}]
                    else:
                        left = self.klines[index-5:index]
                    if index >= self.__length - 5:
                        if index == self.__length:
                            right = [{'feature':''}]
                        right = self.klines[index:self.__length]
                    else:
                        right = self.klines[index:index+5]
                    features = ','.join([temp.get('feature', '') for temp in left]) + ',' + ','.join([temp.get('feature', '') for temp in right])
                    # 判断是否是变盘点
                    if features.count('S107') or features.count('S407'):
                        sell_precise_guai += 1
            
            item = {}
            # 计算覆盖率        
            buy_coverage_rate = float(buy_precise) / float(buy_guai) if buy_precise else 0     
            # 计算准确率 
            buy_precision_rate = float(buy_precise_guai) / float(buy_change) if buy_precise_guai else 0     
            
            item['buy_precision_rate'] = buy_precision_rate
            item['buy_coverage_rate'] = buy_coverage_rate
            
            # 计算覆盖率        
            sell_coverage_rate = float(sell_precise) / float(sell_guai) if sell_precise else 0     
            # 计算准确率 
            sell_precision_rate = float(sell_precise_guai) / float(sell_change) if sell_precise_guai else 0     
            
            item['sell_precision_rate'] = sell_precision_rate
            item['sell_coverage_rate'] = sell_coverage_rate
            # 后期需将这8个变量保存入数据库。 不直接返回计算结果。
             
            return item
    
    
if __name__ == '__main__':
    r = Rate('000001',0)
    temp = r.precision_coverage()
    print(temp)


