#!/usr/bin/python
# -*- coding: utf-8 -*-


import heapq
import matplotlib.pyplot as plt
import matplotlib.finance as mpf  
from matplotlib.pylab import date2num
from decimal import Decimal as D
import datetime
import MySQLdb as mysql
import matplotlib
from MySQLdb import cursors
from matplotlib.font_manager import *  
from matplotlib.dates import DateFormatter

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


class Support_Resistance(object):
    '''支撑阻力计算'''

    def __init__(self, code, name, date_type):
        '''
        @param code : 股票代码
        @param date_type: -1–分时，0—日，1—周，2—月；5–5分钟，15–15分钟，30—30分钟，60—60分钟，90—90分钟，120—120分钟
        '''
        # 从数据库拿出220条数据, 需要 feature, high, low 字段
#         self.klines = [{'tr_date':736613.0, 'feature':'S130,S111,S166', 'open':55, 'high':60, 'low':52, 'close':58},
#                        {'tr_date':736614.0, 'feature':'S133,S145,S135', 'open':25, 'high':52, 'low':25, 'close':33},
#                        {'tr_date':736615.0, 'feature':'S146,S150,S175', 'open':36, 'high':45, 'low':33, 'close':35},
#                        {'tr_date':736616.0, 'feature':'S136,S188,S108', 'open':47, 'high':61, 'low':40, 'close':44},
#                        {'tr_date':736617.0, 'feature':'S132,S152,S179', 'open':28, 'high':30, 'low':24, 'close':27},
#                        {'tr_date':736618.0, 'feature':'S133,S145,S135', 'open':25, 'high':52, 'low':25, 'close':33},
#                        {'tr_date':736619.0, 'feature':'S146,S150,S175', 'open':36, 'high':45, 'low':33, 'close':35},
#                        {'tr_date':736620.0, 'feature':'S136,S188,S108', 'open':47, 'high':61, 'low':40, 'close':44},
#                        {'tr_date':736621.0, 'feature':'S132,S152,S179', 'open':28, 'high':30, 'low':24, 'close':27},
#                        {'tr_date':736622.0, 'feature':'S133,S145,S135', 'open':25, 'high':52, 'low':25, 'close':33},
#                        {'tr_date':736623.0, 'feature':'S146,S150,S175', 'open':36, 'high':45, 'low':33, 'close':35},
#                        {'tr_date':736624.0, 'feature':'S136,S188,S108', 'open':47, 'high':61, 'low':40, 'close':44},
#                        {'tr_date':736625.0, 'feature':'S132,S152,S179', 'open':28, 'high':30, 'low':24, 'close':27},
#                        {'tr_date':736626.0, 'feature':'S133,S145,S135', 'open':25, 'high':52, 'low':25, 'close':33},
#                        {'tr_date':736627.0, 'feature':'S146,S150,S175', 'open':36, 'high':45, 'low':33, 'close':35},
#                        {'tr_date':736628.0, 'feature':'S136,S188,S108', 'open':47, 'high':61, 'low':40, 'close':44},
#                        {'tr_date':736629.0, 'feature':'S132,S152,S179', 'open':28, 'high':30, 'low':24, 'close':27},
#                        ]
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
           user=config['USER'], passwd=config['PASSWD'],
           db=config['DB'], charset=config['CHARSET'],
           cursorclass=config.get('CURSOR', cursors.DictCursor))
        
        self.type = date_type
        self.cursor = self.conn.cursor()
        self.klines = self.get_data(code, date_type)
        self.__length = len(self.klines) 
        # 股票名字
        self.name = name
        # 股票代码
        self.code = code
        # 支持
        self.support_list = []
        # 阻力
        self.resistance_list = []
        self.close = self.klines[0].get('close', 0) if self.klines else []
        
        # 斜线支持
        self.xie_support_list = []
        # 斜线阻力
        self.xie_resistance_list = []
        
        # 向上变盘
        self.up_change = []
        # 向下变盘
        self.down_change = []
    
    
    def get_data(self, code, date_type):
        '''获取数据'''
#         table1 = 'stock.stock_index'
#         table2 = 'stock_feature'
#         sql = "select f.`feature`, k.`tr_time`, k.`open`, k.`high`, k.`low`, k.`close` from %(table1)s as k left join %(table2)s as f on k.`code`=f.`code` and k.`tr_date`=f.`tr_date` and k.`date_type`=f.`date_type` where k.code = '%(code)s' and k.date_type=%(date_type)s order by tr_time desc limit 220" % {"table1":table1, "table2":table2, "code":code, "date_type":date_type}
        sql = "select f.feature, k.tr_time, k.open, k.high, k.low, k.close from stock.stock_kline as k left join stock.stock_feature as f on k.code=f.code and k.tr_date=f.tr_date and k.date_type=f.date_type and k.ex_type=f.ex_type where k.code = '%(code)s' and k.date_type=%(date_type)s and k.ex_type=0 order by tr_time desc limit 220" % {"code":code, "date_type":date_type}
        
        self.cursor.execute(sql)
        klines = self.cursor.fetchall()
        return klines
    
    
    def match_twnty(self):
        '''获取最近20条K线最高值和最低值'''
        # 判断K线是否有20根以上，如果不足，则有多少拿多少
        datas = self.klines[:20] if self.__length >= 20 else self.klines[:self.__length]
        high = max([temp.get('high', 0) for temp in datas])
        low = min([temp.get('low', 0) for temp in datas])
        # 以元组的形式表示一个点，为水平线， （Y轴,）
        self.support_list.append((low,))
        self.resistance_list.append((high,))
    
    
    def match_one_guai(self):
        '''计算出拐点的支撑阻力位'''
        up_temp = None
        down_temp = None 
        
        if self.klines:
            for index, item in enumerate(self.klines):
                feature = item.get('feature', '')
                # 判断向上拐点
                if feature:
                    if feature.count('S131') or feature.count('S133') or feature.count('S135') or feature.count('S137') or feature.count('S139'):
                        # 判断支持还是阻力， 以元组的形式表示一个点，其中0表示水平线
                        if item.get('high', 0) > self.close:
                            self.resistance_list.append((item.get('high',0),))
                        if item.get('high', 0) < self.close:
                            self.support_list.append((item.get('high', 0),))
                    # 判断向下拐点
                    elif feature.count('S130') or feature.count('S132') or feature.count('S134') or feature.count('S136') or feature.count('S138'):
                        # 判断支撑还是阻力
                        if item.get('low', 0) > self.close:
                            self.support_list.append((item.get('low', 0),))
                        if item.get('low', 0) < self.close:
                            self.resistance_list.append((item.get('low', 0),))
                    
                    num = 100 if self.__length > 100 else self.__length
                    if feature.count('S131') or feature.count('S133') or feature.count('S135') or feature.count('S137') or feature.count('S139'):
                        # 需要两个点确定一条直线，用up_temp保存前一个点的值
                        if not up_temp:
                            up_temp = [item.get('high', 0), index, item.get('tr_time', 0)]
                        if up_temp:
                            # a为斜率
                            a = (item.get('high', 0) - up_temp[0]) / (index - up_temp[1])
                            # data 为斜线在0轴的价格
                            data = item.get('high', 0) - (a * index)
                            # 判断是支撑还是阻力
                            if data > self.close:
                                # 以列表的方式表示一条直线，其中两个元组为两个点，data为0轴价格
                                y = num*a + data
#                                 self.xie_resistance_list.append([(up_temp[0], up_temp[2]), (item.get('high', 0), item.get('tr_time', 0)), (data, self.klines[0].get('tr_time', 0))])
                                self.xie_resistance_list.append([num, (y, num), (data, self.klines[0].get('tr_time', 0))])
                            if data < self.close:
#                                 self.xie_support_list.append([(up_temp[0], up_temp[2]), (item.get('high', 0), item.get('tr_time', 0)), (data, self.klines[0].get('tr_time', 0))])
                                self.xie_support_list.append([num, (up_temp[0], up_temp[2]), (data, self.klines[0].get('tr_time', 0))])
                    
                    # 判断向下拐点        
                    elif feature.count('S130') or feature.count('S132') or feature.count('S134') or feature.count('S136') or feature.count('S138'):
                        if not down_temp:
                            # 用down_temp 保存前一个值
                            down_temp = [item.get('low', 0), index, item.get('tr_time', 0)]
                        if down_temp:
                            # a 为斜率
                            a = (item.get('low', 0) - down_temp[0]) / (index - down_temp[1])
                            # data 为斜线在0轴的价格
                            data = item.get('low', 0) - (a * index)
                            if data > self.close:
#                                 self.xie_resistance_list.append([(up_temp[0], up_temp[2]), (item.get('low', 0), item.get('tr_time', 0)), (data, self.klines[0].get('tr_time', 0))])
                                self.xie_resistance_list.append([index, (up_temp[0], up_temp[2]), (data, self.klines[0].get('tr_time', 0))])
                            if data < self.close:
#                                 self.xie_support_list.append([(up_temp[0], up_temp[2]), (item.get('low', 0), item.get('tr_time', 0)), (data, self.klines[0].get('tr_time', 0))])
                                self.xie_support_list.append([index, (up_temp[0], up_temp[2]), (data, self.klines[0].get('tr_time', 0))])
                    
                    if index < 100:
                        # 因为只显示一百根K线，所以只判断前一百是否有变盘信号
                        if feature.count('S106') or feature.count('S108'):
                            self.up_change.append((index, item.get('high', 0)))
                        elif feature.count('S107') or feature.count('S109'):
                            self.down_change.append((index, item.get('low', 0)))
    
    
    def main(self):
        '''计算出距当前价格最近的三个支撑位，阻力位'''
        self.match_twnty()
        self.match_one_guai()
#         self.match_two_guai()
        
        support_close_list = []
        resistance_close_list = []
        support = {}
        resistance = {}
        
        # 将所有点的价格减去当前价格，算出绝对值保存
        for item in self.support_list:
            support[abs(item[0] - self.close)] = item
            support_close_list.append(abs(item[0] - self.close))
        
        for item in self.xie_support_list:
            support[abs(item[2] - self.close)] = item
            support_close_list.append(abs(item[2] - self.close))
        
        # 以计算出来的绝对值为键，保存支撑阻力原来的值
        for item in self.resistance_list:
            resistance[abs(item[0] - self.close)] = item
            resistance_close_list.append(abs(item[0] - self.close))
        
        for item in self.xie_resistance_list:
            resistance[abs(item[2] - self.close)] = item
            resistance_close_list.append(abs(item[2] - self.close))
        
        item_data = {'support':[], 'resistance':[]}
        min_n = 3
        # 找出距离当前价格最近的三个支撑位 和阻力位
        if support_close_list:
            three_support = list(heapq.nsmallest(min_n, support_close_list))
            
            for temp in three_support:
                item_data['support'].append(support[temp])
        
        if resistance_close_list:
            three_resistance = list(heapq.nsmallest(min_n, resistance_close_list))
            
            for temp in three_resistance:
                item_data['resistance'].append(resistance[temp])
        
        return item_data
    
    def datetime_to_num(self, tr_time):
        '''转换时间格式'''
        date_time = datetime.datetime.strptime(tr_time,'%Y%m%d%H%M%S')
        num_date = date2num(date_time)
        return num_date
    
    
    def run(self):
        '''画图'''
        # 设置字体
        myfont = FontProperties(fname='/usr/share/fonts/wps-office/FZHTK.TTF')  
        temp_data = self.main()
        support = temp_data.get('support', '')
#         support.append([100,(11.400, 20), (11.200, 20)])
        resistance = temp_data.get('resistance', '')
        # 设置历史数据区间  
        high = None
        low = None
        last = 100
        if self.__length < 100:
            last = self.__length
        # 截取最多100根K线用来显示
        kilnes_datas = self.klines[:last]
        max_price = max([temp.get('high', 0) for temp in kilnes_datas])
        min_price = min([temp.get('low', 0) for temp in kilnes_datas])
        quotes = []
        times = []
        #  保存数据开盘价， 最高价， 最低价， 收盘价列表
        for index, item in enumerate(kilnes_datas):
            int_time = item.get('tr_time', 0)
            if int_time:
                # 转换时间格式
                str_time = str(int_time)
                date_time = datetime.datetime.strptime(str_time,'%Y%m%d%H%M%S')
                date_time = str(date_time).split(' ')[0]
                temp = (date_time, item.get('open', 0), item.get('high', 0), item.get('low', 0), item.get('close', 0))
                times.insert(0, date_time)
                quotes.append(temp)
                # 保存所有K线中的最高价，最低价
                if item.get('high', 0) == max_price:
                    high = [index, item.get('high', 0),]
                if item.get('low', 0) == min_price:
                    low = [index, item.get('low', 0)]
        # 将最高价与最低价之间的区域，分成10分    
        highest = max([temp.get('high', 0) for temp in self.klines])       
        lowest = min([temp.get('low', 0) for temp in self.klines])       
        cur = (highest - lowest) / 10
        d = cur/10
        print(highest,lowest)
        cur_list = [[lowest, lowest+cur],
                    [lowest+cur, lowest+2*cur],
                    [lowest+2*cur, lowest+3*cur],
                    [lowest+3*cur, lowest+4*cur],
                    [lowest+4*cur, lowest+5*cur],
                    [lowest+5*cur, lowest+6*cur],
                    [lowest+6*cur, lowest+7*cur],
                    [lowest+7*cur, lowest+8*cur],
                    [lowest+8*cur, lowest+9*cur],
                    [lowest+9*cur, lowest+10*cur],
                    ]  
        num_s_list = [0,0,0,0,0,0,0,0,0,0]
        num_r_list = [0,0,0,0,0,0,0,0,0,0]
        # 计算密集支撑区间
        support_price = [item[0] for item in self.support_list] + [item[2][0] for item in self.xie_support_list]
        for price in support_price:
            for index, temp in enumerate(cur_list):
                if temp[0] <= price <= temp[1]:
                    num_s_list[index] +=1
                    break
                
        rest_s = None   
        max_s_num = max(num_s_list)
        for index, i in enumerate(num_s_list):
            if i == max_s_num:
                rest_s = cur_list[index]
                break
            
        # 计算密集阻力区间   
        resistance_price = [item[0] for item in self.resistance_list] + [item[2][0] for item in self.xie_resistance_list]
        for price in resistance_price:
            for index, temp in enumerate(cur_list):
                if temp[0] <= price <= temp[1]:
                    num_r_list[index] +=1
                    break
                
        rest_r = None   
        max_s_num = max(num_r_list)
        for index, i in enumerate(num_r_list):
            if i == max_s_num:
                rest_r = cur_list[index]
                break
        # 创建一个子图   
        fig, ax = plt.subplots(figsize=(18,6), dpi=80, facecolor=('white'))  
#         fig.subplots_adjust(bottom=0.5)        
        #画出变盘信号
        if self.up_change:
            ax.plot([100 - temp[0] for temp in self.up_change], [temp[1] + D(d) for temp in self.up_change], 'vr')
        if self.down_change:
            ax.plot([100 - temp[0] for temp in self.down_change], [temp[1] - D(d) for temp in self.down_change], '^g')
        
        # 画出密集支撑阻力区域
        ax.axhspan(
            rest_s[0],
            rest_s[1],
            facecolor='r', # 颜色
            alpha=0.3, # 透明度
            label = u'密集支撑'
        )
        ax.axhspan(
            rest_r[0],
            rest_r[1],
            facecolor='g', # 颜色
            alpha=0.3, # 透明度
            label = u'密集阻力'
        )
        
        # 画出支撑阻力线
        for item in support:
            if len(item) == 1:
                ax.hlines(
                    y=item[0], # y轴坐标
                    linewidth=1, # 线宽
                    xmin=0, # 
                    xmax=100, # xmin,xmax表示从左开始到右线条的长度比例 0无 1最长 0.5一半
                    color='r', # 颜色
                    label=u'支撑位'
                )
            elif len(item) == 3:
#                 ax.plot([self.datetime_to_num(item[0][0]), self.datetime_to_num(item[1][0]), self.datetime_to_num(item[2][0])],[item[0][0], item[1][0], item[2][0]], color='r',linewidth=1,linestyle='-')
                ax.plot([last - item[0], last],[item[1][0], item[2][0]], color='r',linewidth=1,linestyle='-')
        for item in resistance:
            if len(item) == 1:
                ax.hlines(
                    y=item[0], # y轴坐标
                    linewidth=1, # 线宽
                    xmin=0, # 
                    xmax=100, # xmin,xmax表示从左开始到右我线条的长度比例 0无 1最长 0.5一半
                    color='g', # 颜色
                    label=u'阻力位'
                )     
            elif len(item) == 3:
#                 ax.plot([self.datetime_to_num(item[0][0]), self.datetime_to_num(item[1][0]), self.datetime_to_num(item[2][0])],[item[0][0], item[1][0], item[2][0]], color='g',linewidth=1,linestyle='-')
                ax.plot([last - item[0], last],[item[1][0], item[2][0]], color='g',linewidth=1,linestyle='-')
        
        # 箭头表示最高和最低价格
        plt.annotate(
        high[1], #显示字符串
        fontproperties='SimHei', # 中文字体
        xy=(last-high[0]-1,high[1]), # 箭头位置
        xytext=(last-high[0]-1,high[1]), # 文本位置
        arrowprops=dict(facecolor='red',shrink=1,width=1, headlength=0) # facecolor:箭头颜色；shrink:箭头的起始和结束位置两侧的空白大小；width:箭头宽度
        )
        
        plt.annotate(
        low[1], #显示字符串
        fontproperties='SimHei', # 中文字体
        xy=(last-1-low[0],low[1]), # 箭头位置
        xytext=(last-low[0]-1,low[1]-D(2*d)), # 文本位置
        arrowprops=dict(facecolor='green',shrink=1,width=1,headlength=0) # facecolor:箭头颜色；shrink:箭头的起始和结束位置两侧的空白大小；width:箭头宽度
        )
        
        # X轴刻度文字倾斜45度  
        ax.yaxis.tick_right()
        # 设置X,Y轴备注
        plt.title(u"%(name)s(%(code)s) 支撑阻力" % {"name":self.name, "code":self.code}, fontproperties=myfont, fontsize='24')  
        plt.xlabel(u"日期", fontproperties=myfont, fontsize='18')  
        plt.ylabel(u"价格(万元)", fontproperties=myfont, fontsize='18') 
        # 画出K线图
#         mpf.candlestick2_ochl(ax, opens, closes, highs, lows, width=0.6, colorup='red', colordown='green', alpha=1)
        '''1 开, 2 高, 3 低, 4 收'''
        for index, data in enumerate(quotes[::-1], 1):
            if data[4] < data[1]:
                col = 'g'
            elif data[4] == data[1]:
                col = 'black'
            elif data[4] > data[1]:
                col = 'r'
            # 画出影线  
            plt.vlines(
                x=index, # y轴坐标
                linewidth=1, # 线宽
                ymin=data[3], # 
                ymax=data[2], # xmin,xmax表示从左开始到右线条的长度比例 0无 1最长 0.5一半
                color=col # 颜色
            )
            # 画出K线主体
            data_min = data[1]
            data_max = data[4]
            if data[4] == data[1]:
                data_min = data[1] - D(0.003)
                data_max = data[4] + D(0.003)
            plt.vlines(
                x=index, 
                ymin=data_min,
                ymax=data_max, 
                linewidth=6, 
                color=col)
        # 设置X轴刻度
        ax.set_xticks([0,last+1])
#         ax.set_yticks(price)
        ax.set_xticklabels([times[0],times[-1]])
#         ax.set_yticklabels(price)
        
        # label 显示的位置设置 
        plt.legend(loc = 'upper left', prop=myfont, fancybox=False,shadow=False)
        plt.grid(True)  # 显示网格
#         plt.savefig('test',dpi=600)  #  保存图片
        plt.show()  # 显示图片
        
if __name__ == '__main__':
    p = Support_Resistance(name='中国平安', code='000001', date_type=0)
    p.run()
    
    