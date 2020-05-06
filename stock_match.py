# coding: utf-8
import MySQLdb as mysql
from MySQLdb import cursors
from datetime import timedelta, datetime  
import numpy as np
import time
from fileinput import close

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



class STOCK_MATCH(object):

    TYPE_DATE_DAY = 0
    TYPE_EX_FORWARD = 1
    DAY_LIMIT_BEGIN = 0
    DAY_LIMIT_END = 99999999
    STOCK_KLINE_WITH_REALTIME = True
    DIRECT_KLINE_RIGHT = 1
    
    
    def __init__(self, code):
        self.conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                               user=config['USER'], passwd=config['PASSWD'],
                               db=config['DB'], charset=config['CHARSET'],
                               cursorclass=config.get('CURSOR', cursors.DictCursor))
        self.cursor = self.conn.cursor()
        self.table = 'stock.stock_kline'
        self.code = code
        self.close = self.CLOSE()
        
    

    def __get_stock_move_kline_with_index__(self, date_type=TYPE_DATE_DAY, ex_type=TYPE_EX_FORWARD, start_date=DAY_LIMIT_BEGIN, limit=DAY_LIMIT_END, equal=STOCK_KLINE_WITH_REALTIME, direct=-DIRECT_KLINE_RIGHT):
        """从某一起始点，获取移动的K线数据(带数据索引序数：1/2/3...N)"""
        """Columns : index, id, code, tr_date, date_type, ex_type, k_type, open, close, high, low, yclose, volume, amount, wave_cnt, wave_range, vibrate_range, hl_range"""
        
        # ------------------------------
        # MySQL Feature
        timestamp = int(time.time() * 1000)
        sql = "set @x%s = 0" % timestamp
        self.cursor.execute(sql)
        rownum = "@x%s:=ifnull(@x%s,0)+1 as rownum" % (timestamp, timestamp)
        # ------------------------------
        
        if direct > 0:
            order = 'asc'
            than = equal and '>=' or '>'
        else:
            order = 'desc'
            than = equal and '<=' or '<'
        sql = """select %(rownum)s, vt.* from (
                     select `id`, `code`, `tr_date`, `date_type`, `ex_type`, `k_type`, `open`, `close`, `high`, `low`, `yclose`, `volume`, `amount`, `wave_cnt`, `wave_range`, `vibrate_range`, `hl_range` 
                     from %(table)s 
                     where `code` = %(param)s and `tr_date` %(than)s %(param)s and `date_type` = %(param)s and `ex_type` = %(param)s 
                     order by `tr_date` %(order)s limit 0, %(limit)s) vt order by vt.`tr_date`
                 """ % {'table': self.table, 'rownum': rownum, 'limit': limit, 'than': than, 'order': order, 'param': '%s'}
        params = [self.code, start_date, date_type, ex_type]
        self.cursor.execute(sql, params)
        res = self.cursor.fetchall()
        del params[:], params
        return res
    
    
    
    def CLOSE(self, data=None):
        if not data:
            data =  time.strftime("%Y%m%d", time.localtime()) 
        sql = "select `close` from %(table)s where code = '%(code)s' and date_type = 0 and tr_date = %(data)s" % {"table": self.table, "code":self.code, "data":data}
        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        return res.get('close', 0)      
    
    
    def get_db_data(self, type, page=0, limit=0):
        sql = "select %(type)s from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit %(page),%(limit)s" % {"type":type, "table": self.table, "code":self.code, "page":page, "limit":limit}
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return res 
        
#      
#     def EMA(self, price_today, N, COFF):
#         if COFF == 0:
#             a = 2/(N+1)
#         else:
#             a = COFF/N
#         yesterday_EMA = None
#         ema = a*(price_today - yesterday_EMA) + yesterday_EMA
#         return ema
    
    
    def EMA(self, L, N, COEF, all=None):
        if COEF == 0:
            alpha = 2/(N+1)
        else:
            alpha = COEF/N
        ema_data = []
        if not alpha:
            alpha = 1/(len(L)+1.25) # defaults
        if (alpha<0) or (alpha>1):
            raise ValueError("0 < smoothing factor <= 1")
        alpha_bar = float(1-alpha)
        
        """ generate [x(0)], [x(1),x(0)], [x(2),x(1),x(0)],.... """
        num_terms_list = [sorted(L[:i],reverse=True) for i in range(1,len(L)+1)]
        #print num_terms_list
        #return 
        for nterms in num_terms_list:
            # calculate 1st~(t-1)-th terms corresponding exponential factor
            pre_exp_factor = [float(alpha_bar**(i-1)) for i in range(1,len(nterms))]
            # calculate the ema at the next time periods
            ema_data.append(alpha*float(sum(float(a)*float(b) for a,b in zip(tuple(pre_exp_factor), tuple(nterms[:-1])))) + \
                             (alpha_bar**(len(nterms)-1))*float(nterms[-1]))
        if all:
            return ema_data
        return ema_data[-1]


    
    def MA(self, type, n):
        res = self.get_db_data(type, limit=n)
        close_data = [temp.get('close', 0) for temp in res]
        return float(sum(close_data)) / len(close_data)
    
    
    def XS(self):
        
        UPPERL = self.MA('high', 30) * 1.15
        UPPERS = self.MA('high', 3) * 1.03
        LOWERS = self.MA('low', 3) * 0.97
        LOWERL = self.MA('low', 30) * 0.85
        
        return UPPERL,UPPERS,LOWERS,LOWERL
    
    
    def BOLL(self, n):
        boll = self.MA('close',n)
        rest = self.get_db_data('close', limit=n)
        close_data = [temp.get('close', 0) for temp in rest]
        ub = boll + 2 * np.std(close_data)
        lb = boll - 2 * np.std(close_data)
        return ub,boll,lb
    
    
    def RSI(self, a, b, c):
        # 获取a + 1收盘价格列表
        data_a = [temp.get('close', 0) for temp in self.get_db_data('close', limit=a+1)]
        close_cmp_a = [max(data_a[i] - data_a[i-1], 0) for i in range(data_a-1)]
        close_abs_a = [abs(data_a[i] - data_a[i-1]) for i in range(data_a-1)]
        data_b = [temp.get('close', 0) for temp in self.get_db_data('close', limit=b+1)]
        close_cmp_b = [max(data_b[i] - data_b[i-1], 0) for i in range(data_b-1)]
        close_abs_b = [abs(data_b[i] - data_b[i-1]) for i in range(data_b-1)]
        data_c = [temp[0].get('close', 0) for temp in self.get_db_data('close', limit=c+1)]
        close_cmp_c = [max(data_c[i] - data_c[i-1], 0) for i in range(data_c-1)]
        close_abs_c = [abs(data_c[i] - data_c[i-1]) for i in range(data_c-1)]
        
        RSI1 = self.EMA(close_cmp_a[::-1],a,1) / self.EMA(close_abs_a[::-1],a,1)*100;
        RSI2 = self.EMA(close_cmp_b[::-1],b,1) / self.EMA(close_abs_b[::-1],b,1)*100;
        RSI3 = self.EMA(close_cmp_c[::-1],c,1) / self.EMA(close_abs_c[::-1],c,1)*100;
        
        return RSI1,RSI2,RSI3    
    
    
    def BIAS(self, a, b, c):
        ma1 = self.MA('close',a)
        ma2 = self.MA('close',b)
        ma3 = self.MA('close',c)
        bias_1 = (self.close - ma1) / ma1 * 100
        bias_2 = (self.close - ma2) / ma2 * 100
        bias_3 = (self.close - ma3) / ma3 * 100
        
        return bias_1, bias_2, bias_3
    
    
    def MACD(self, a, b, c,):
        diff_list = []
        for i in range(c):
            close_a = [temp.get('close', 0) for temp in self.get_db_data('close', page=i, limit=a)]
            close_b = [temp.get('close', 0) for temp in self.get_db_data('close', page=i, limit=b)]
            DIFF1 = self.EMA(close_a[::-1],a,0) - self.EMA(close_b[::-1],b,0)
            diff_list.append(DIFF1)
        DEA1 = self.EMA(diff_list[::-1],c,0)
        macd = 2 * (diff_list[0]-DEA1)
#         close_a = self.get_db_data('close', a)
#         close_b = self.get_db_data('close', b)
#         DIFF1 = self.EMA(close_a[::-1],a,0) - self.EMA(close_b[::-1],b,0)
#         DEA1 = self.EMA(DIFF1,c,0)
#         macd = 2 * (DIFF1-DEA1) 
        return diff_list[0], DEA1, macd
    
    
    def KDJ(self, a, b, c):
        rsv = ''
        K1_list = []
        # 获取前C天的K1值K1_list
        for j in range(c):
            RSV1_LIST = []
            for i in range(j, b+j):
                # 获取前b天的RSV1值RSV1_LIST
                close = self.get_db_data('close', i, 1)
                low_list = [temp.get('low', 0) for temp in self.get_db_data('low', i, a)]
                low = min(low_list)
                high_list = [temp.get('high', 0) for temp in self.get_db_data('high', i, a)]
                high = max(high_list)
                RSV1 = (close - low) / (high - low) * 100
                if rsv == '':
                    rsv = RSV1
                RSV1_LIST.append(RSV1)
            K1 = self.EMA(RSV1_LIST,b,1)
            K1_list.append(K1)
        
        D1 = self.EMA(K1_list,c,1)
        J1 = 3 * K1_list[0] - 2 * D1
        
        return K1_list[0], D1, J1
                             
    
#     def LLV(self, a):
#         # a日内最低价
#         sql = "select `low` from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit %(a)s" % {"table": self.table, "code":self.code, "a":a}
#         self.cursor.execute(sql)
#         res = self.cursor.fetchall()
#         return min(res)[0]
#     
#     
#     def HHV(self, a):
#         # a日内最高价
#         sql = "select `high` from %(table)s where code = '%(code)s' and date_type = 0 order by tr_date desc limit %(a)s" % {"table": self.table, "code":self.code, "a":a}
#         self.cursor.execute(sql)
#         res = self.cursor.fetchall()
#         return max(res)[0]
#     
    
    def turn_off(self):
        self.cursor.close()


if __name__ == '__main__':
    def ema(L, N, COEF):
        if COEF == 0:
            alpha = float(2)/float((N+1))
        else:
            alpha = float(COEF)/float(N)
        ema_data = []
        if not alpha:
            alpha = 1/(len(L)+1.25) # defaults
        if (alpha<0) or (alpha>1):
            raise ValueError("0 < smoothing factor <= 1")
        alpha_bar = float(1-alpha)
        """ generate [x(0)], [x(1),x(0)], [x(2),x(1),x(0)],.... """
        num_terms_list = [sorted(L[:i],reverse=True) for i in range(1,len(L)+1)]
        #print num_terms_list
        #return 
        for nterms in num_terms_list:
            # calculate 1st~(t-1)-th terms corresponding exponential factor
            pre_exp_factor = [float(alpha_bar**(i-1)) for i in range(1,len(nterms))]
            # calculate the ema at the next time periods
            ema_data.append(alpha*float(sum(float(a)*float(b) for a,b in zip(tuple(pre_exp_factor), tuple(nterms[:-1])))) + \
                             (alpha_bar**(len(nterms)-1))*float(nterms[-1]))
        return ema_data
    
    sp = [333.53,334.3,340.98,343.55,338.55,343.51,347.64,352.15,354.87,348,353.54,356.71,]

    
# float* EMA(float* close,int n,int len,int coef)
# {
#     float k;
#     if(coef==0)
#         k=(float)2/(len+1);
#     else
#         k=(float)coef/len;
# 
#     float* EMAvalue=NULL;
#     EMAvalue=new float[n];
# 
#     int i;
#     if(n<len)
#     {
#         for(i=0;i<n;i++)
#         {
#             EMAvalue[i]=close[i];
#         }
#     }else
#     {
#         for(i=0;i<len-1;i++)
#         {
#             EMAvalue[i]=close[i];
#         }
#     }
#     for(i=len-1;i<n;i++)
#     {
#         EMAvalue[i]=k*(close[i]-EMAvalue[i-1])+EMAvalue[i-1];
#     }
# // KDJarray中RSV传入数组前面有0,防止计算D递归出错,仍然以0填充RSV的0的个数
#     int count=0;
#     while(close[count]==0)
#         count++;
#     for(i=0;i<count;i++)
#         EMAvalue[i]=0;
# 
#     return EMAvalue;
# }
