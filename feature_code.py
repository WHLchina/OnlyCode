
# coding: utf-8
import MySQLdb as mysql
from MySQLdb import cursors
import numpy as np

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

class FEATURE_CODE(object):

    def __init__(self, code):
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                               user=config['USER'], passwd=config['PASSWD'],
                               db=config['DB'], charset=config['CHARSET'],
                               cursorclass=config.get('CURSOR', cursors.DictCursor))
        self.code = code
        
        self.num = 0   # 保存数据为正的个数
        self.feature_code = []
        # 从数据库中拿出220条日K线
        self.day_kline_data_list = self.get_klines(0)
        self.dl = len(self.day_kline_data_list)
        # 从数据库中拿出220条周K线
        self.week_kline_data_list = self.get_klines(1)
        self.wl = len(self.week_kline_data_list)
        # 从数据库中拿出220条月K线
        self.month_kline_data_list = self.get_klines(2)
        self.ml = len(self.month_kline_data_list)
        
        # 从数据库中拿出日K线3条识别码数据
        self.day_feature_data_list = self.get_features(0)
        self.dfl = len(self.day_feature_data_list)
        # 从数据库中拿出周K线3条识别码数据
        self.week_feature_data_list = self.get_features(1)
        self.wfl = len(self.week_feature_data_list)
        # 从数据库中拿出月K线3条识别码数据
        self.month_feature_data_list = self.get_features(2)
        self.mfl = len(self.month_feature_data_list)
           
        # 从数据库中拿出日K线沪深数据  
        self.stock_hushen_data_day = self.get_hushen(0, 10)
        # 从数据库中拿出周K线沪深数据  
        self.stock_hushen_data_week = self.get_hushen(1, 10)
        # 从数据库中拿出月K线沪深数据  
        self.stock_hushen_data_month = self.get_hushen(2, 10)
        
    
    def get_klines(self, date_type):
        ''' 获取K线数据
        @param date_type :日期类型
        '''
        cursor = self.conn.cursor()
        table = 'stock.stock_kline'
        sql = "select * from %(table)s where code = '%(code)s' and date_type = '%(date_type)s' order by tr_date desc limit 220" % {"table": table, "code": self.code, "date_type":date_type}
        cursor.execute(sql)
        
        data = cursor.fetchall()
        cursor.close()
        return data
        
        
    def get_features(self, date_type):
        ''' 获取识别码数据
        @param date_type :日期类型
        '''
        cursor = self.conn.cursor()
        table = 'stock.stock_feature'
        sql = "select * from %(table)s where code = '%(code)s' and date_type = '%(date_type)s' order by tr_date desc limit 3" % {"table": table, "code": self.code, "date_type":date_type}
        cursor.execute(sql)
        
        data = cursor.fetchall()
        cursor.close()
        return data
    
       
    def get_hushen(self, date_type, limit):
        ''' 获取沪深指数数据
        @param date_type :日期类型
        @param limit :获取数量
        '''
        cursor = self.conn.cursor()
        table = 'stock.stock_kline'
        sql = "select * from %(table)s where code = '%(code)s' and date_type = '%(date_type)s' order by tr_date desc limit %(limit)s" % {"table": table, "code": "000300", "date_type":date_type, "limit": limit}
        cursor.execute(sql)
        
        data = cursor.fetchall()
        cursor.close()
        return data
    
    
    def S101_S102(self, cycle):
        """计算黄昏星与启明星识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        
        if cycle == 0:
            res = self.day_kline_data_list[:3] if self.dl > 3 else 0
        elif cycle == 1:
            res = self.week_kline_data_list[:3] if self.wl > 3 else 0
        elif cycle == 2:
            res = self.month_kline_data_list[:3] if self.ml > 3 else 0
        if not res:
            return
        # 本期最高价高于上期最高价，本期最低价高于上期最低价，上期最高价低于上上期最高价，上期最低价低于上上期最低价。同时，本期为阳线，上上期为阴线
        if (res[0].get('close', 0) - res[0].get('open', 0)) > 0 and (res[2].get('close', 0) - res[2].get('open', 0)) < 0:
            if res[0].get('high', 0) > res[1].get('high', 0) < res[2].get('high', 0) and res[0].get('low', 0) > res[1].get('low', 0) < res[2].get('low', 0):
                self.feature_code.append('S101')
        # 本期最高价低于上期最高价，本期最低价低于上期最低价，上期最高价高于上上期最高价，上期最低价高于上上期最低价。同时，本期为阴线，上上期为阳线
        if (res[0].get('close', 0) - res[0].get('open', 0)) < 0 and (res[2].get('close', 0) - res[2].get('open', 0)) > 0:
            if res[0].get('high', 0) < res[1].get('high', 0) > res[2].get('high', 0) and res[0].get('low', 0) < res[1].get('low', 0) > res[2].get('low', 0):
                self.feature_code.append('S102')
    
    
    def S103(self, cycle):
        """计算多空分歧识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_kline_data_list[:2] if self.dl > 2 else 0
        elif cycle == 1:
            res = self.week_kline_data_list[:2] if self.wl > 2 else 0
        elif cycle == 2:
            res = self.month_kline_data_list[:2] if self.ml > 2 else 0
        if not res:
            return
        
        high = res[0].get('high', 0) - res[1].get('high', 0)
        low = res[0].get('low', 0) - res[1].get('low', 0)
        # 一个为+，一个为-，或者，一个为+一个为0，或者，一个为0，一个为-
        if (high > 0 and low < 0) or (high > 0 and low == 0) or (high == 0 and low < 0):
            self.feature_code.append('S103')
     
    
    def S104_S105(self, cycle):
        """
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_kline_data_list[:11] if self.dl > 11 else 0
        elif cycle == 1:
            res = self.week_kline_data_list[:11] if self.wl > 11 else 0
        elif cycle == 2:
            res = self.month_kline_data_list[:11] if self.ml > 11 else 0
        if not res:
            return
        close_list = [temp.get('close',0) for temp in res]
        midle_list = [(temp.get('high', 0) + temp.get('low', 0))/2 for temp in res]
        close_three = sum(close_list[0:3])/3 - sum(close_list[1:4])/3
        if close_three > 0:
            self.num += 1
        close_five = sum(close_list[0:5])/5 - sum(close_list[1:6])/5
        if close_five > 0:
            self.num += 1
        close_ten = sum(close_list[0:10])/10 - sum(close_list[1:11])/10
        if close_ten > 0:
            self.num += 1
        middle_three = sum(midle_list[0:3])/3 - sum(midle_list[1:4])/3
        if middle_three > 0:
            self.num += 1
        middle_five = sum(midle_list[0:5])/3 - sum(midle_list[1:6])/3
        if middle_five > 0:
            self.num += 1
        middle_ten = sum(midle_list[0:10])/3 - sum(midle_list[1:11])/3
        if middle_ten > 0:
            self.num += 1
        # 至少中有三个以上为+。同时，本期收盘价高于上期收盘价，上期收盘价高于上上期
        if self.num > 3:
            if res[0].get('close', 0) > res[1].get('close', 0) > res[2].get('close', 0) or midle_list[0] > midle_list[1] > midle_list[2]:
                self.feature_code.append('S104')
        # 至少中有三个以上为+。本期中间价高于上期中间价，上期中间价高于上上期
        if self.num < 3:
            if res[0].get('close', 0) < res[1].get('close', 0) < res[2].get('close', 0) or midle_list[0] < midle_list[1] < midle_list[2]:
                self.feature_code.append('S105')
            
    
    def S106(self, cycle):
        """计算轻度向上变盘识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_feature_data_list[:2] if self.dfl > 2 else 0
            rest = self.day_kline_data_list[:2] if self.dl > 2 else 0
        if cycle == 1:
            res = self.week_feature_data_list[:2] if self.dfl > 2 else 0
            rest = self.week_kline_data_list[:2] if self.dl > 2 else 0
        if cycle == 2:
            res = self.month_feature_data_list[:2] if self.dfl > 2 else 0
            rest = self.month_kline_data_list[:2] if self.dl > 2 else 0
        if not res or not rest:
            return
        # 上期含105，本期含103。且本期不含111或112识别码
        if res[0].get('feature', '').count('S105') > 0:
            if 'S103' in self.feature_code and ('S111' not in self.feature_code or 'S112' not in self.feature_code):
                self.feature_code.append('S106')
        # 上上期含105，本期含101。且本期不含111或112识别码，而且跌幅>-9
        elif res[1].get('feature', '').count('S105') > 0:
            if 'S101' in self.feature_code and ('S111' not in self.feature_code or 'S112' not in self.feature_code):
                item = (rest[0].get('close', 0) - rest[1].get('close', 0))/rest[1].get('close', 0)
                if item > -9:
                    self.feature_code.append('S106')
    
    
    def S107(self, cycle):
        """计算轻度向下变盘识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_feature_data_list[:2] if self.dfl > 2 else 0
        if cycle == 1:
            res = self.week_feature_data_list[:2] if self.dfl > 2 else 0
        if cycle == 2:
            res = self.month_feature_data_list[:2] if self.dfl > 2 else 0
        if not res:
            return
        # 上期含104，本期含103。且本期不含114或113识别码
        if res[0].get('feature', '').count('S104') > 0:
            if 'S103' in self.feature_code and ('S114' not in self.feature_code or 'S113' not in self.feature_code):
                self.feature_code.append('S107')
        # 上上期含104，本期含102。且本期不含114或113识别码
        elif len(res) == 2:
            if res[1].get('feature', '').count('S105') > 0 and 'S102' in self.feature_code and ('S114' not in self.feature_code or 'S113' not in self.feature_code):
                self.feature_code.append('S107')
    
    
    def S111_S113(self, cycle):
        """计算包孕线识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_kline_data_list[:2] if self.dl > 2 else 0
        elif cycle == 1:
            res = self.week_kline_data_list[:2] if self.wl > 2 else 0
        elif cycle == 2:
            res = self.month_kline_data_list[:2] if self.ml > 2 else 0
        if not res:
            return
        # 前一根K线是阳线，后一根K线是阴线。后一根K线的开盘价高于前一根K线的收盘价，后一根K线的收盘价低于前一个K线的开盘价
        if res[0].get('close', 0) - res[0].get('open', 0) < 0 and res[1].get('close', 0) - res[1].get('open', 0) > 0:
            if res[0].get('open', 0) > res[1].get('close', 0) and res[0].get('close', 0) < res[1].get('open', 0):
                self.feature_code.append('S111')
        # 前一根K线是阴线，后一根K线是阳线。后一根K线的收盘价高于前一根K线的开盘价，后一根K线的开盘价低于前一个K线的收盘价
        elif res[0].get('close', 0) - res[0].get('open', 0) > 0 and res[1].get('close', 0) - res[1].get('open', 0) < 0:
            if res[0].get('close', 0) > res[1].get('open', 0) and res[0].get('open', 0) < res[1].get('close', 0):
                self.feature_code.append('S113')
    
    
    def S112_S114(self, cycle):
        """计算长影线识别码
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_kline_data_list[0] if self.day_kline_data_list else 0
        elif cycle == 1:
            res = self.week_kline_data_list[0] if self.week_kline_data_list else 0
        elif cycle == 2:
            res = self.month_kline_data_list[0] if self.month_kline_data_list else 0
        if not res:
            return
        # 影线长度占K线长度的66%-100%
        if res.get('open', 0) - res.get('close', 0) > 0:
            # K线长度
            body = abs(res.get('open', 0) - res.get('close', 0))
            # 上影线长度
            up_ying = res.get('high', 0) - res.get('close', 0)
            # 下影线长度
            down_ying = res.get('open', 0) - res.get('low', 0)
            if 0.66 < float(up_ying/body) < 1.0:
                self.feature_code.append('S112')
            if 0.66 < float(down_ying/body) < 1.0:
                self.feature_code.append('S114')
    
    
    def S115_S116_S117_S118(self, cycle):
        """（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        if cycle == 0:
            res = self.day_feature_data_list[0] if self.day_feature_data_list else 0
            rest = self.day_kline_data_list[0] if self.day_kline_data_list else 0
        if cycle == 1:
            res = self.week_feature_data_list[0] if self.week_feature_data_list else 0
            rest = self.week_kline_data_list[0] if self.week_kline_data_list else 0
        if cycle == 2:
            res = self.month_feature_data_list[0] if self.month_feature_data_list else 0
            rest = self.month_kline_data_list[0] if self.month_kline_data_list else 0
        if not res or not rest:
            return
        # 判断本期是阳线
        if rest.get('close', 0) - rest.get('open', 0) > 0:
            # 上期含106
            if res.get('feature', '').count('S106') > 0:
                self.feature_code.append('S115')
            # 上期含108
            if res.get('feature', '').count('S108') > 0:
                self.feature_code.append('S117')
        # 判断本期是阴线
        elif rest.get('close', 0) - rest.get('open', 0) < 0:
            # 上期含107
            if res.get('feature', '').count('S107') > 0:
                self.feature_code.append('S116')
            # 上期含109
            if res.get('feature', '').count('S109') > 0:
                self.feature_code.append('S118')
    
    
    def S119_S120_S121_S122_S127(self, cycle):
        ''' 需要即时计算index技术指标
            计算均线金叉（5x10），均量线金叉，kdj金叉，macd金叉，必涨金股，5*20 金叉，5*30 金叉，40x120金叉 ，macd死叉
        @param cycle: 周期类型'''
        
        table = 'stock.stock_index'
        columns = ['`ma5`', '`ma10`', '`ma20`', '`ma30`', '`ma40`', '`ma120`','`volma5`', '`volma10`', 'k', 'd', '`diff`', '`dea`']
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit 2" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle": cycle}
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if len(res) < 2:
            return
        
        # 今日5日均线高于10日均线，昨日5日均线低于10日均线
        if res[0].get('ma5', 0) > res[0].get('ma10', 0) and res[1].get('ma5', 0) < res[1].get('ma10', 0):
            self.feature_code.append('S119')
        # 今日成交量5日均线高于10日均线，昨日成交量5日均线低于成交量10日均线
        if res[0].get('volma5', 0) > res[0].get('volma10', 0) and res[1].get('volma5', 0) < res[1].get('volma10', 0):
            self.feature_code.append('S120')
        # 今日kdj的k值高于d值，昨日k值低于d值
        if res[0].get('k', 0) > res[0].get('d', 0) and res[1].get('k', 0) < res[1].get('d', 0):
            self.feature_code.append('S121')
        # 今日macd的diff高于dea，昨日diff低于dea
        if res[0].get('diff', 0) > res[0].get('dea', 0) and res[1].get('diff', 0) < res[1].get('dea', 0):
            self.feature_code.append('S122')
        # 今日5日均线高于20日均线，昨日5日均线低于20日均线
        if res[0].get('ma5', 0) > res[0].get('ma20', 0) and res[1].get('ma5', 0) < res[1].get('ma20', 0):
            self.feature_code.append('S124')
        # 今日5日均线高于30日均线，昨日5日均线低于30日均线
        if res[0].get('ma5', 0) > res[0].get('ma30', 0) and res[1].get('ma5', 0) < res[1].get('ma30', 0):
            self.feature_code.append('S125')
        # 今日40日均线高于120日均线，昨日40日均线低于120日均线
        if res[0].get('ma40', 0) > res[0].get('ma120', 0) and res[1].get('ma40', 0) < res[1].get('ma120', 0):
            self.feature_code.append('S126')
        # 今日macd的diff低于dea，昨日diff高于dea
        if res[0].get('diff', 0) < res[0].get('dea', 0) and res[1].get('diff', 0) > res[1].get('dea', 0):
            self.feature_code.append('S127')
    
    
    def S123(self, cycle):
        ''' 需要即时计算index技术指标
        @param cycle: 周期类型
        '''
        
        if cycle == 0:
            res = self.day_feature_data_list[:2] if self.dfl > 2 else 0
        if cycle == 1:
            res = self.week_feature_data_list[:2] if self.dfl > 2 else 0
        if cycle == 2:
            res = self.month_feature_data_list[:2] if self.dfl > 2 else 0
        
        table2 = 'stock.stock_index'
        columns2 = ['`dea`']
        sql2 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit 1" % {"columns": ", ".join(columns2), "table": table2, "code":self.code}
        self.cursor.execute(sql2)
        rest = self.cursor.fetchone()
        
        if not res or not rest:
            return
        
        str_con = ''
        # 如果包含今日，之前4个交易日出现过119、120、121、122、106，并且今日macd的dea数值低于0
        for temp in res:
            # 合并之前的feature
            str_con += temp.get('feature')
        str_con = str_con + (',').join(self.feature_code)
        if rest.get('dea', 0) < 0 and str_con.count('S119') > 0 and str_con.count('S106') > 0 and str_con.count('S120') > 0 and str_con.count('S121') > 0 and str_con.count('S122') > 0:
            self.feature_code.append('S123')
    
    
    def S128(self, cycle):
        """
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        """
        if cycle == 0:
            res = self.day_feature_data_list[0] if self.day_feature_data_list else 0
            rest = self.day_kline_data_list[:2] if self.dl > 2 else 0
        if cycle == 1:
            res = self.week_feature_data_list[0] if self.week_feature_data_list else 0
            rest = self.week_kline_data_list[:2] if self.wl > 2 else 0
        if cycle == 2:
            res = self.month_feature_data_list[0] if self.month_feature_data_list else 0
            rest = self.month_kline_data_list[:2] if self.ml > 2 else 0
        if not res or not rest:
            return
        # 昨日收阴线。昨日包含105。
        if res.get('feature', '').count('S105') > 0 and (rest[1].get('close', 0) - rest[1].get('open',0)) < 0:
            # 今日最高价低于昨日最高价，今日最低价高于昨日最低价
            if rest[0].get('high', 0) < rest[1].get('high', 0) and rest[0].get('low', 0) > rest[1].get('low', 0):
                # 今日收阴线。今日开盘价低于昨日开盘价，今日收盘价高于昨日收盘价。
                if (rest[0].get('close', 0) - rest[0].get('open',0)) < 0 and rest[0].get('open', 0) < rest[1].get('open', 0) and rest[0].get('close', 0) > rest[1].get('close', 0):
                    self.feature_code.append('S128')
                # 今日收阳线或十字星。今日收盘价低于昨日开盘价，今日开盘价高于昨日收盘价
                elif (rest[0].get('close', 0) - rest[0].get('open',0)) >= 0 and rest[0].get('close', 0) < rest[1].get('open', 0) and rest[0].get('open', 0) > rest[1].get('close', 0):
                    self.feature_code.append('S128')
            # 今日最高价低于或等于昨日收盘价，今日最低价高于或等于昨日最低价。昨日收阴线，今日收盘价和开盘价均低于昨日收盘价
            elif rest[0].get('high', 0) <= rest[1].get('close', 0) and rest[0].get('low', 0) >= rest[1].get('low', 0) and rest[0].get('close', 0) < rest[1].get('close', 0) and rest[0].get('open', 0) < rest[1].get('close', 0):
                self.feature_code.append('S128')
    
    
    def SR108_109(self):
        '''日线算法, 需要计算技术指标'''
        table = 'stock.stock_index'
        columns = ['`k`', ]
        columns2 = ['`dea`', '`macd`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code}
        sql1 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 1 order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code}
        sql2 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit 1" % {"columns": ", ".join(columns2), "table": table, "code":self.code}
        self.cursor.execute(sql)
        day_k = self.cursor.fetchall()
        self.cursor.execute(sql1)
        week_k = self.cursor.fetchall()
        self.cursor.execute(sql2)
        rest = self.cursor.fetchone()
        if len(day_k) < 4 or len(week_k) < 3 or not rest:
            return
        # 获取日线K值
        day_k_list = [temp.get('k', 0) for temp in day_k]
        # 获取周线K值
        week_k_list = [temp.get('k', 0) for temp in week_k]
        # 计算均值
        day_avg = float(sum(day_k_list) / len(day_k_list))
        week_avg = float(sum(week_k_list) / len(week_k_list))
        # 计算标准差
        day_avr = np.std(day_k_list)
        week_avr = np.std(week_k_list)
        # 计算K值下限
        day_gap = day_avg - day_avr
        week_gap = week_avg - week_avr
        # 计算K值上限
        day_sum = day_avg + day_avr
        week_sum = week_avg -+ week_avr
        # 本期日线不含111或112识别码
        if 'S111' not in self.feature_code or 'S112' not in self.feature_code:
            # 本期日线含106，并且上期和上上期日线K值都在日线K值下限之下。同时，本期日线DEA值和日线MACD值都<0。上周周线K值在周线K值下限之下
            if day_k[1].get('k', 0) < day_gap and day_k[2].get('k', 0) < day_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0 and week_k[1].get('k', 0) < week_gap and 'S106' in self.feature_code:
                self.feature_code.append('S108')
            # 本期日线含106和101，并且上上期和上上上期日线K值都在日线K值下限之下。同时，本期日线DEA值和日线MACD值都<0。上周周线K值在周线K值下限之下
            elif day_k[3].get('k', 0) < day_gap and day_k[2].get('k', 0) < day_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0 and week_k[2].get('k', 0) < week_gap and 'S106' in self.feature_code and 'S101' in self.feature_code: 
                self.feature_code.append('S108')
        # 本期日线不含114或113识别码
        if 'S114' not in self.feature_code or 'S113' not in self.feature_code:
            # 本期日线含107，并且上期和上上期日线K值都在日线K值上限之上。同时，本期日线DEA值和日线MACD值都>0。上周周线K值在周线K值上限之上
            if day_k[1].get('k', 0) > day_sum and day_k[2].get('k', 0) > day_sum and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and week_k[1].get('k', 0) > week_sum and 'S107' in self.feature_code:
                self.feature_code.append('S109')
            # 本期日线含107和102，并且上上期和上上上期日线K值都在日线K值下限之下。同时，本期日线DEA值和日线MACD值都>0。上周周线K值在周线K值下限之上
            elif day_k[3].get('k', 0) < day_gap and day_k[2].get('k', 0) < day_gap and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and week_k[2].get('k', 0) > week_gap and 'S107' in self.feature_code and 'S102' in self.feature_code: 
                self.feature_code.append('S109')
    
    
    def SW108_109(self):
        '''周线算法, 需要计算技术指标'''
        table = 'stock.stock_index'
        columns = ['`k`', ]
        columns2 = ['`dea`', '`macd`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 1 order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code}
        sql1 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 2 order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code}
        sql2 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 1 order by tr_date desc limit 1" % {"columns": ", ".join(columns2), "table": table, "code":self.code}
        self.cursor.execute(sql)
        week_k = self.cursor.fetchall()
        self.cursor.execute(sql1)
        mon_k = self.cursor.fetchall()
        self.cursor.execute(sql2)
        rest = self.cursor.fetchone()
        if len(week_k) < 4 or len(mon_k) < 3 or not rest:
            return
        # 获取周线K值
        week_k_list = [temp.get('k', 0) for temp in week_k]
        # 获取月线K值
        mon_k_list = [temp.get('k', 0) for temp in mon_k]
        # 计算均值
        week_avg = float(sum(week_k_list) / len(week_k_list))
        mon_avg = float(sum(mon_k_list) / len(mon_k_list))
        # 计算标准差
        week_avr = np.std(week_k_list)
        mon_avr = np.std(mon_k_list)
        # 计算下限
        week_gap = week_avg - week_avr
        mon_gap = mon_avg - mon_avr
        # 计算上限
        week_sum = week_avg + week_avr
        mon_sum = mon_avg + mon_avr
        # 本期周线不含111或112识别码
        if 'S111' not in self.feature_code or 'S112' not in self.feature_code:
            # 本期周线含106，并且上期和上上期周线K值都在周线K值下限之下。同时，本期周线DEA值和周线MACD值都<0。上月月线K值在月线K值下限之下
            if week_k[1].get('k', 0) < week_gap and week_k[2].get('k', 0) < week_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0 and mon_k[1].get('k', 0) < mon_gap and 'S106' in self.feature_code:
                self.feature_code.append('S108')
            # 本期周线含106和101，并且上上期和上上上期周线K值都在周线K值下限之下。同时，本期周线DEA值和周线MACD值都<0。上月月线K值在月线K值下限之下
            elif week_k[3].get('k', 0) < week_gap and week_k[2].get('k', 0) < week_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0 and mon_k[2].get('k', 0) < mon_gap and 'S106' in self.feature_code and 'S101' in self.feature_code: 
                self.feature_code.append('S108')   
        # 本期周线不含114或113识别码
        if 'S114' not in self.feature_code or 'S113' not in self.feature_code:
            #  本期周线含107，并且上期和上上期周线K值都在日线K值上限之上。同时，本期周线DEA值和周线MACD值都>0。上月月线K值在月线K值上限之上
            if week_k[1].get('k', 0) > week_sum and week_k[2].get('k', 0) > week_sum and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and mon_k[1].get('k', 0) > mon_sum and 'S107' in self.feature_code:
                self.feature_code.append('S109')
            # 本期周线含107和102，并且上上期和上上上期周线K值都在周线K值下限之下。同时，本期周线DEA值和周线MACD值都>0。上月月线K值在月线K值下限之上
            elif week_k[3].get('k', 0) < week_gap and week_k[2].get('k', 0) < week_gap and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and mon_k[2].get('k', 0) > mon_gap and 'S107' in self.feature_code and 'S102' in self.feature_code: 
                self.feature_code.append('S109')
    
    
    def SM108_109(self):
        '''月线算法, 需要计算技术指标'''
        table = 'stock.stock_index'
        columns = ['`k`', ]
        columns2 = ['`dea`', '`macd`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 2 order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code}
        sql2 = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 2 order by tr_date desc limit 1" % {"columns": ", ".join(columns2), "table": table, "code":self.code}
        self.cursor.execute(sql)
        mon_k = self.cursor.fetchall()
        self.cursor.execute(sql2)
        rest = self.cursor.fetchone()
        if len(mon_k) < 4 or not rest:
            return
        # 获取月线k值
        mon_k_list = [temp.get('k', 0) for temp in mon_k]
        # 计算均值
        mon_avg = float(sum(mon_k_list) / len(mon_k_list))
        # 计算标准差
        mon_avr = np.std(mon_k_list)
        # 计算下限
        mon_gap = mon_avg - mon_avr
        # 计算上限
        mon_sum = mon_avg + mon_avr
        # 本期月线不含111或112识别码
        if 'S111' not in self.feature_code or 'S112' not in self.feature_code:
            # 本期月线含106，并且上期和上上期月线K值都在月线K值下限之下。同时，本期月线DEA值和月线MACD值都<0
            if mon_k[1].get('k', 0) < mon_gap and mon_k[2].get('k', 0) < mon_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0  and 'S106' in self.feature_code:
                self.feature_code.append('S108')
            # 本期月线含106和101，并且上上期和上上上期月线K值都在月线K值下限之下。同时，本期月线DEA值和月线MACD值都<0
            elif mon_k[3].get('k', 0) < mon_gap and mon_k[2].get('k', 0) < mon_gap and (rest.get('dea', 0) + rest.get('macd', 0)) < 0 and 'S106' in self.feature_code and 'S101' in self.feature_code: 
                self.feature_code.append('S108')   
        # 本期月线不含114或113识别码
        if 'S114' not in self.feature_code or 'S113' not in self.feature_code:
            # 本期月线含107，并且上期和上上期月线K值都在月线K值上限之上。同时，本期月线DEA值和日线MACD值都>0
            if mon_k[1].get('k', 0) > mon_sum and mon_k[2].get('k', 0) > mon_sum and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and 'S107' in self.feature_code:
                self.feature_code.append('S109')
            # 本期月线含107和102，并且上上期和上上上期月线K值都在月线K值下限之下。同时，本期月线DEA值和月线MACD值都>0
            elif mon_k[3].get('k', 0) < mon_gap and mon_k[2].get('k', 0) < mon_gap and rest.get('dea', 0) > 0 and rest.get('macd', 0) > 0 and 'S107' in self.feature_code and 'S102' in self.feature_code: 
                self.feature_code.append('S109')
    
    
    def shares_guai(self, cycle):
        '''拐点计算'''
        for day in [5,7,10,20,30]:
            table = 'stock.stock_kline'
            limit = 2*day -1
            # 计算向上拐点
            if type == 'up':
                columns = ['`tr_date`', '`high`']
                sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit %(limit)s" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle, "limit":limit}
                self.cursor.execute(sql)
                rest = self.cursor.fetchall()
                high_list = [temp.get('high', 0) for temp in rest]
                max_high = max(high_list)
                # 找出最大值，如果最大值与前day根K线最大值相等，就对这跟K线添加识别码
                if high_list[day-1] == max_high:
                    if day == 5:
                        status = 'S131'
                    elif day == 7:
                        status = 'S135'
                    elif day == 10:
                        status = 'S133'
                    elif day == 20:
                        status = 'S137'
                    elif day == 30:
                        status = 'S139'
            if type == 'down':
                columns = ['`tr_date`', '`low`']
                sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit %(limit)s" % {"columns": ", ".join(columns), "table": table, "code":self.code, "limit":limit}
                self.cursor.execute(sql)
                rest = self.cursor.fetchall()
                low_list = [temp.get('low', 0) for temp in rest]
                min_low = min(low_list)
                # 找出最小值，如果最小值与前day根K线最小值相等，就对这跟K线添加识别码
                if low_list[day-1] == min_low:
                    if day == 5:
                        status = 'S130'
                    elif day == 7:
                        status = 'S134'
                    elif day == 10:
                        status = 'S132'
                    elif day == 20:
                        status = 'S136'
                    elif day == 30:
                        status = 'S138'
            data = rest[day-1].get('tr_date', 0)
            self.update(code=self.code, data=data, date_type=type, status=status)
        
    
    def turnover_guai(self, cycle):
        '''成交量拐点计算'''
        for day in [5,7,10,20,30]:
            table = 'stock.stock_kline'
            limit = 2*day -1
            columns = ['`tr_date`', '`volume`']
            sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit %(limit)s" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle, "limit":limit}
            self.cursor.execute(sql)
            rest = self.cursor.fetchall()
            volume_list = [temp.get('volume', 0) for temp in rest]
            if type == 'up':
                max_volume = max(volume_list)
                # 找出最大值，如果最大值与前day根K线最大值相等，就对这跟K线添加识别码
                if volume_list[day-1] == max_volume:
                    if day == 5:
                        status = 'S231'
                    elif day == 7:
                        status = 'S235'
                    elif day == 10:
                        status = 'S233'
                    elif day == 20:
                        status = 'S237'
                    elif day == 30:
                        status = 'S239'
            if type == 'down':
                min_low = min(volume_list)
                # 找出最小值，如果最小值与前day根K线最小值相等，就对这跟K线添加识别码
                if volume_list[day-1] == min_low:
                    if day == 5:
                        status = 'S230'
                    elif day == 7:
                        status = 'S234'
                    elif day == 10:
                        status = 'S232'
                    elif day == 20:
                        status = 'S236'
                    elif day == 30:
                        status = 'S238'
                        
            data = rest[day-1].get('tr_date', 0)
            self.update(code=self.code, data=data, status=status, date_type=type)
    
    
    def update(self, code, data, status, date_type=type):
        '''修改拐点的feature'''
        table = 'stock.stock_feature'
        sql = "select `feature` from %(table)s where code = '%(code)s' and date_type = %(date_type)s and tr_date = %(data)" % {"table": table, "code":code, "date_type":date_type, "data":data}
        self.cursor.execute(sql)
        rest = self.cursor.fetchone()
        if rest:
            feature = rest.get('feature', '')
            # 将新识别码添加到之前的feature
            feature += ',' + status
            sql_update = "update %(table)s set `feature`='%(feature)s' where code = %(code)s and date_type = %(date_type)s and tr_date = %(data)s" % {"table": table, "code":code, "data":data, "date_type":date_type, "feature":feature}
            self.cursor.execute(sql_update)
            self.conn.commit()
        else:
            return
 
    
    def S222_S223(self, cycle):
        '''需要计算技术指标 （日周月）'''
        table = 'stock.stock_index'
        columns = ['`dea`']
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit 1" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle}
        self.cursor.execute(sql)
        rest = self.cursor.fetchone()
        if rest:
            # 判断包含'S122',并且dea大于0
            if 'S122' in self.feature_code and rest.get('dea', 0) < 0:
                self.feature_code.append('S222')
                # 判断是否包含'S123','S222'
                if 'S123' in self.feature_code:
                    self.feature_code.append('S223')
    
    
    def S406(self, cycle):
        """（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        # 判断含有106，且不含有111，
        if 'S106' in self.feature_code and 'S111' not in self.feature_code:
            is_up = False
            is_down = False
            if cycle == 0:
                rest = self.day_kline_data_list[:2] if self.dl > 2 else 0
            elif cycle == 1:
                rest = self.week_kline_data_list[:2] if self.wl > 2 else 0
            elif cycle == 2:
                rest = self.month_kline_data_list[:2] if self.ml > 2 else 0
            if not rest:
                return
            
            if rest[0].get('high', 0) <= rest[1].get('high', 0) and rest[0].get('low', 0) >= rest[1].get('low', 0):
                avg_data = (rest[0].get('close', 0) + rest[1].get('close', 0)) / 2
                if rest[1].get('close', 0) - rest[1].get('open', 0) <= 0 and rest[0].get('opne', 0) >= rest[1].get('open', 0) and avg_data >= rest[1].get('open', 0):
                    is_up = True
                elif rest[1].get('close', 0) - rest[1].get('open', 0) > 0 and rest[0].get('opne', 0) >= rest[1].get('close', 0) and avg_data >= rest[1].get('close', 0):
                    is_up = True
            if rest[0].get('open', 0) >= rest[1].get('close', 0) and rest[0].get('close', 0) <= rest[1].get('open', 0):
                if rest[0].get('close', 0) - rest[0].get('open', 0) < 0 and rest[1].get('close', 0) - rest[1].get('open', 0) >= 0:
                    is_down = True
            # 不含有看跌吞没，不含有上涨尽头
            if not is_up and not is_down:
                self.feature_code.append('S406')
    
    
    def S407(self, cycle):
        """（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        # 当期含有107，且不含有113
        if 'S106' in self.feature_code and 'S111' not in self.feature_code:
            is_up = False
            is_down = False
            if cycle == 0:
                rest = self.day_kline_data_list[:2] if self.dl > 2 else 0
            elif cycle == 1:
                rest = self.week_kline_data_list[:2] if self.wl > 2 else 0
            elif cycle == 2:
                rest = self.month_kline_data_list[:2] if self.ml > 2 else 0
            if not rest:
                return
            if rest[0].get('high', 0) <= rest[1].get('high', 0) and rest[0].get('low', 0) >= rest[1].get('low', 0):
                avg_data = (rest[0].get('close', 0) + rest[1].get('close', 0)) / 2
                if rest[1].get('close', 0) - rest[1].get('open', 0) <= 0 and rest[0].get('opne', 0) <= rest[1].get('open', 0) and avg_data <= rest[1].get('open', 0):
                    is_down = True
                elif rest[1].get('close', 0) - rest[1].get('open', 0) > 0 and rest[0].get('opne', 0) <= rest[1].get('close', 0) and avg_data <= rest[1].get('close', 0):
                    is_down = True
            if rest[0].get('open', 0) <= rest[1].get('close', 0) and rest[0].get('close', 0) >= rest[1].get('open', 0):
                if rest[0].get('close', 0) - rest[0].get('open', 0) > 0 and rest[1].get('close', 0) - rest[1].get('open', 0) < 0:
                    is_up = True
            # 不含有看涨吞没，不含有下跌尽头线
            if not is_up and not is_down:
                self.feature_code.append('S407')
    
    
    def S206_207_306_307(self, cycle):
        '''需要计算技术指标 （日周月）
        @param cycle:计算周期'''
        
        table = 'stock.stock_index'
        columns = ['`k`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle}
        self.cursor.execute(sql)
        rest = self.cursor.fetchall()
        if len(rest) < 3:
            return
        k_list = [temp.get('k', 0) for temp in rest]
        # 计算均值
        avg_k = sum(k_list) / len(k_list)
        # 计算标准差
        std_k = np.std(k_list)
        # 当期含有406
        if 'S406' in self.feature_code:
            # 且当期K值<220期k值均值
            if k_list[0].get('k', 0) < avg_k:
                self.feature_code.append('S206')
                # 当期含有206，且当期k值小于220日k值均值-k值方差
                if k_list[0].get('k', 0) < (avg_k - std_k):
                    self.feature_code.append('S306')
        if 'S407' in self.feature_code:
            # 本期含有407，且当期k值大于220期k值均值
            if k_list[0].get('k', 0) < avg_k:
                self.feature_code.append('S207')
                # 本期含有207，且当期k值阿玉220期k值+方差
                if k_list[0].get('k', 0) < (avg_k - std_k):
                    self.feature_code.append('S307')
    
    
    def S501_502(self, cycle):
        """（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        """
        if cycle == 0:
            rest = self.day_kline_data_list if self.day_kline_data_list else 0
        elif cycle == 1:
            rest = self.week_kline_data_list if self.week_kline_data_list else 0
        elif cycle == 2:
            rest = self.month_kline_data_list if self.month_kline_data_list else 0
        if not rest:
            return
        # 计算标准差
        std_close = np.std([temp.get('close', 0) for temp in rest])
        # k低于标准差
        if rest[0].get('close', 0) < std_close:
            self.feature_code.append('S501')
        # k高于标准差
        elif rest[0].get('close', 0) > std_close:
            self.feature_code.append('S502')
    
          
    def S503_504(self, cycle):
        """（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        """
        if cycle == 0:
            rest = self.week_kline_data_list if self.week_kline_data_list else 0
        elif cycle == 1:
            rest = self.month_kline_data_list if self.month_kline_data_list else 0
        elif cycle == 2:
            rest = self.month_kline_data_list if self.month_kline_data_list else 0
        if not rest:
            return
        # 计算标准差
        std_close = np.std([temp.get('close', 0) for temp in rest])
        # 上级k线低于标准差
        if rest[0].get('close', 0) < std_close:
            self.feature_code.append('S503')
        # 上级k线高于于标准差 
        elif rest[0].get('close', 0) > std_close:
            self.feature_code.append('S504')
      

    def SS609_SS612(self, cycle, stock_price):
        """ 只包含（日）
        # 股票技术指标数据 在stock_index表
        @param cycle: 计算周期  
        @param stock_price: 股票价格，【待定获取路径】"""
        table = 'stock.stock_index'
        columns = ['`boll`', '`bollup`', '`bolldown`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit 1" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle}
        self.cursor.execute(sql)
        rest = self.cursor.fetchone()
        if not rest:
            return
        # 布林带数据 需计算
        boll_data = rest.get('boll', '')
        # 布林带数据上轨
        boll_data_up = rest.get('bollup', '')
        # 布林带数据下轨
        boll_data_down = rest.get('bolldown', '')
        
        # 股价在日线布林带上轨之上运行
        if stock_price > boll_data_up:
            self.feature_code.append('S609')
        # 股价在日线布林带中轨和上轨之间运行
        elif boll_data_up > stock_price > boll_data:
            self.feature_code.append('S610')
        # 股价在日线布林带中轨和下轨之间运行
        elif boll_data > stock_price > boll_data_down:
            self.feature_code.append('S611')
        # 股价在日线布林带上轨之下运行
        elif stock_price > boll_data_down:
            self.feature_code.append('S612')
    
    
    def SS607_SS608(self, cycle):
        """ 包含（日周月）
        # 股票技术指标数据 在stock_index表
        
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟 ；
        """
        
        table = 'stock.stock_index'
        columns = ['`k`',]
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and date_type = %(cycle)s order by tr_date desc limit 220" % {"columns": ", ".join(columns), "table": table, "code":self.code, "cycle":cycle}
        self.cursor.execute(sql)
        rest = self.cursor.fetchall()
        if len(rest) < 3:
            return
        # KDJ的K值(一年)
        k_data_list = [data.get('k', '') for data in rest]
        # KDJ的K值(当天)
        k_data = rest[-1].get('k', '')
        # KDJ的K值的平均值
        k_data_avg = sum(k_data_list) / len(k_data_list)
        # KDJ的K值的标准差
        k_data_std = np.std(k_data_list)
        # KDJ的K值已经高于平均数+标准差区间
        if k_data > (k_data_avg + k_data_std):
            self.feature_code.append('S607')
        # KDJ的K值已经高于平均数-标准差区间
        elif k_data > (k_data_avg - k_data_std):
            self.feature_code.append('S608')
    
    
    def SS616(self, cycle, tr_date=None):
        """ 包含（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        
        # 假定初始平均值
        avg_data = 9999999
        
        if cycle == 0:
            # 计算周期
            tr_date = 3
            # 5日平均
            n_day = 5
            stock_data_list = self.day_kline_data_list[0:7]
        elif cycle == 1:
            tr_date = 2
            n_day = 4
            stock_data_list = self.week_feature_data_list[0:5]            
        elif cycle == 2:
            tr_date = 2
            n_day = 6
            stock_data_list = self.month_feature_data_list[0:7]
        
        for i in range(tr_date):
            stock_high_avg_data = np.average([ float(i.get('wave_range', 0)) for i in stock_data_list[i:n_day + i]])
            if avg_data > stock_high_avg_data:
                avg_data = stock_high_avg_data
            else:
                return
        
        self.feature_code.append('S616')
    
    
    def SS613(self, cycle, tr_date=None):
        """包含（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        
        if cycle == 0:
            # 计算周期
            tr_date = 3
            # 5日平均
            n_day = 5
            stock_data_list = self.day_kline_data_list[0:7]
            stock_hushen_list = self.stock_hushen_data_day[0:7]
        elif cycle == 1:
            tr_date = 2
            n_day = 4
            stock_data_list = self.week_feature_data_list[0:5]
            stock_hushen_list = self.stock_hushen_data_week[0:5]
        elif cycle == 2:
            tr_date = 2
            n_day = 6
            stock_data_list = self.month_feature_data_list[0:7]
            stock_hushen_list = self.stock_hushen_data_month[0:7]
        
        for i in range(tr_date):
            stock_high_avg_data = np.average([ float(i.get('wave_range', 0)) for i in stock_data_list[i:n_day + i]])
            stock_hushen_avg_data = np.average([ float(i.get('wave_range', 0)) for i in stock_hushen_list[i:n_day + i]])
            
            if stock_high_avg_data < stock_hushen_avg_data:
                return
        
        self.feature_code.append('S613')
    
    
    def SS614_SS615(self, cycle):
        """包含（日周月）
        @param cycle: 周期类型，-1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟；
        @param tr_date: 计算周期 """
        # 上期feature
        if cycle == 0:
            last_feature = self.day_feature_data_list[0].get("feature", "") if self.day_feature_data_list else 0
        if cycle == 1:
            last_feature = self.week_feature_data_list[0].get("feature", "") if self.week_feature_data_list else 0
        if cycle == 2:
            last_feature = self.month_feature_data_list[0].get("feature", "") if self.month_feature_data_list else 0
        if not last_feature:
            return
        if last_feature.count('S613') and 'S613' not in self.feature_code:
            self.feature_code.append('S615')
        elif not last_feature.count('S613') and 'S613' in self.feature_code:
            self.feature_code.append('S614')

    
    
if __name__ == '__main__':
    f = FEATURE_CODE('000040')
    f.S501_502(0)
    print(f.feature_code)

