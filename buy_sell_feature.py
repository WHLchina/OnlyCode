#!/usr/bin/python
# -*- coding: utf-8 -*-

from stock_match import STOCK_MATCH


class Buy_Feature(object):
    '''买点计算'''
    def __init__(self, code, close):
        self.code = code
        self.close = close
        # 取出7天的feature
        self.feature_list = []
        
        #今天的feature
        self.featuer_code = []
        
        #周线feature
        self.week_feature = None
        
        # 月线feature
        self.month_feature = None
        
        self.stock_match = STOCK_MATCH(self.close)
    
    
    def get_feature(self):
        '''获取feature'''
        pass
    
    
    def B01(self):
        '''个股在40日均线之上出现406（收盘价在40日均线之上），同时40日均线高于120日均线。'''
        ma40 = self.stock_match.MA(0, 40)
        ma120 = self.stock_match.MA(0, 120)
        if self.close > ma40 and ma40 > ma120 and self.feature_list[0].count('S406'):
            self.featuer_code.append('B01')
    
    
    def B03(self):
        '''个股在7日内出现5日均线按顺序依次连穿10日、20日、30日均线。'''
        features = ','.join(self.feature_list[:7])
        if not features.count('B03'):
            # 取出7天的 5,10,20日均线
            ma5_list =  []
            ma10_list = []
            ma20_list = []
            ma30_list = []
            for i in range(1, len(ma5_list)):
                if ma5_list[i-1] < ma10_list[i-1] and ma5_list[i] > ma10_list[i]:
                    for j in range(i, len(ma20_list[i:])):
                        if ma10_list[j-1] < ma20_list[j-1] and ma10_list[j] > ma20_list[j]:
                            for k in range(j, len(ma30_list[j:])):
                                if ma20_list[k-1] < ma30_list[k-1] and ma20_list[k] > ma30_list[k]:
                                    self.featuer_code.append('B03')
    
    
    def B04(self):
        '''个股在40日均线与120日之间出现406（收盘价在40日和120日均线之间），同时40日均线高于120日均线。'''
        ma40 = self.stock_match.MA(0, 40)
        ma120 = self.stock_match.MA(0, 120)
        if ma40 > self.close > ma120 and self.feature_list[0].count('S406'):
            self.featuer_code.append('B04')
    
    
    def B05(self):
        '''个股在5日内在120日均线之上出现406（收盘价在120日均线之上）和506（MACD从死叉转金），同时120日均线高于220日均线。'''
        features = ','.join(self.feature_list[:5])
        if not features.count('B05'):
            # 取出前6天的值
            close_list = []
            ma120_list = []
            ma220_list = []
            dif_list = []
            dea_list = []
            feature = self.feature_list[:6][::-1]
            
            
            for i in range(1,6):
                if close_list[i] > ma120_list[i] and dif_list[i-1] < dea_list[i-1] and dif_list[i] > dea_list[i]:
                    for j in range(i,6):
                        if feature[j].count('S406') and ma120_list[j] > ma220_list[j]:
                            self.feature_code.append('B05')
                elif close_list[i] > ma120_list[i] and feature[j].count('S406'):
                    for j in range(i, 6):
                        if dif_list[j-1] < dea_list[j-1] and dif_list[j] > dea_list[j] and ma120_list[j] > ma220_list[j]:
                            self.featuer_code.append('B05')
    
    
    def B06(self):
        '''个股在48周均线之上出现406'''
        ma48 = self.stock_match.MA(1, 48)
        if self.close > ma48 and self.week_feature.count('S406'):
            self.featuer_code.append('B06')
    
    
    def B07(self):
        '''个股月线出现406。'''
        if self.month_feature.count('S406'):
            self.featuer_code.append('B07')
    
    
    def B08(self):
        '''10个交易日内依次发生，5日均线上穿10日均线，5日均线上穿20日均线，10日均线上穿20日均线。'''
        # 取出10天的 5,10,20日均线
        ma5_list =  []
        ma10_list = []
        ma20_list = []
        for i in range(1, len(ma5_list)):
            if ma5_list[i-1] < ma10_list[i-1] and ma5_list[i] > ma10_list[i]:
                for j in range(i, len(ma20_list[i:])):
                    if ma5_list[j-1] < ma20_list[j-1] and ma5_list[j] > ma20_list[j]:
                        for k in range(k, len(ma20_list[j:])):
                            if ma10_list[k-1] < ma20_list[k-1] and ma10_list[k] > ma20_list[k]:
                                self.featuer_code.append('B03')
    
    


class Sell_Feature(object):
    '''卖点计算'''
    def __init__(self, code):
        self.code = code
        # 当前价格
        self.close = None
        # 最大价格
        self.max_close = None
        # 买入的价格
        self.buy_close = None
        # 卖点识别码
        self.feature_code = []
        # 最大涨幅
        self.price = (self.max_close - self.buy_close) / self.buy_close
        # 当前涨幅
        self.now_price = (self.close - self.buy_close) / self.buy_close
        
        self.stock_match = STOCK_MATCH(self.code)
    
    
    def S02(self):
        '''该笔买入的最大涨幅超过5%后涨幅回落到2%时'''
        if self.price >= 0.05:
            if self.now_price <= 0.02:
                self.feature_code.append('S02')
    
    
    def S03(self):
        '''该笔买入的最大涨幅超过10%后，回撤了55%时'''
        if self.price >= 0.1:
            if self.now_price <= 0.045:
                self.feature_code.append('S03')
    
    
    def S04(self):
        '''该笔买入的最大涨幅超过20%后，回撤了40%时'''
        if self.price >= 0.2:
            if self.now_price <= 0.12:
                self.feature_code.append('S04')
    
    
    def S05(self):
        '''该笔买入的最大涨幅超过40%，回撤了30%'''
        if self.price >= 0.4:
            if self.now_price <= 0.28:
                self.feature_code.append('S05')
    
    
    def S06(self):
        '''从买入后跌破40日均线10%时'''
        ma40 = self.stock_match.MA(0, 40)
        if (ma40 - self.close) / ma40 >= 0.1:
            self.feature_code.append('S06')
    
    
    def S07(self):
        '''从买入后跌破120日均线10%时''' 
        ma120 = self.stock_match.MA(0, 120)
        if (ma120 - self.close) / ma120 >= 0.1:
            self.feature_code.append('S07')
    
    
    def S08(self):
        '''该笔买入后上涨超过20%后，又从最大涨幅下跌超过50%时'''
        if self.price >= 0.2:
            if self.now_price <= 0.1:
                self.feature_code.append('S08')
    
    
    def S09(self):
        '''该笔买入后上涨超过30%后，又从最大涨幅下跌超过30%时'''
        if self.price >= 0.3:
            if self.now_price <= 0.21:
                self.feature_code.append('S09')
    
    
    def S10(self):
        '''该笔买入后上涨超过40%后，又从最大涨幅下跌超过20%时'''
        if self.price >= 0.4:
            if self.now_price <= 0.32:
                self.feature_code.append('S10')
    
    
    def S11(self):
        '''从买入后下跌N时 写死10%止损'''
        if (self.buy_close - self.close) / self.buy_close >= 0.1:
            self.feature_code.append('S11')
    
    
    def S12(self):
        '''该笔买入后上涨超过10%后，又从最大涨幅下跌超过80%时''' 
        if self.price >= 0.1:
            if self.now_price <= 0.02:
                self.feature_code.append('S12')
    
    
    def S13(self):
        '''从买入后跌破48周均线15%时'''
        ma48_week = self.stock_match.MA(1, 48)
        if (ma48_week - self.close) / ma48_week >= 0.15:
            self.feature_code.append('S13')
    
    
    def S14(self):
        '''从买入后跌破120日均线15%时'''
        ma120 = self.stock_match.MA(0, 120)
        if (ma120 - self.close) / ma120 >= 0.15:
            self.feature_code.append('S14')

