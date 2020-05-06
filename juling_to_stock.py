
import MySQLdb as mysql
from MySQLdb import cursors
import re
import time
from sshtunnel import SSHTunnelForwarder  
from _ast import Pass


config = {
    'HOST': '123.56.72.57',
    'PORT': 3306,
    'USER': 'python',
    'PASSWD': 'yoquantpython.',
    'DB':'test',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}


server = SSHTunnelForwarder(  
         ('101.201.155.139', 22),    #B机器的配置  
         ssh_password="Yoquant130612",  
         ssh_username="root",  
         remote_bind_address=('rds47r30r2052hg03je8.mysql.rds.aliyuncs.com', 3306)) #A机器的配置  

server.start()
  
att_conn = mysql.connect(host='127.0.0.1',              #此处必须是是127.0.0.1  
                       port=server.local_bind_port,  
                       user='asttac',  
                       passwd='Asttac123',  
                       db='julingdata',
                       charset='utf8',
                       cursorclass=config.get('CURSOR', cursors.DictCursor))  

conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                           user=config['USER'], passwd=config['PASSWD'],
                           db=config['DB'], charset=config['CHARSET'],
                           cursorclass=config.get('CURSOR', cursors.DictCursor))

class Stock_Finance(object):
    """P240801: 每股收益 ， P150101：净利润， P110100：营业收入， P110200:营业成本， TOTAL：总股本， C100000：经营活动产生的现金流量净额
    B311101：归属母公司所有者权益，  B310201：资本公积， B310701：未分配利润， B111501：存货净额， B111511：存货原值，
    B200000：负债合计， B100000：资产总计
    """
    mins = '-01-01 00:00:00'
    first = '-03-31 00:00:00'
    second = '-06-30 00:00:00'
    third = '-09-30 00:00:00'
    four = '-12-31 00:00:00'
    
    def __init__(self, year, code, inner_code):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        # 当前年份 如 2017 int
        self.year = year
        self.code = code
        self._comcode = None
        self.jzc = None
        self.zgb = None
        self.yysr = None
        self._inner_code = inner_code
        self.avg_price = None
    
    
    def get_finance_datas(self):
        key_list = [self.first, self.second, self.third, self.four]
        table5 = 'stk_code'
        sql7 = "select COMCODE, INNER_CODE from %(table)s where STOCKCODE = '%(code)s'" % {"table": table5, "code":self.code}
        self.cursor_att.execute(sql7)
        comcode = self.cursor_att.fetchone()
        self._comcode = comcode.get('COMCODE', 0)
        self._inner_code = comcode.get('INNER_CODE', 0)
        
        table2 = 'stk_bala_gen'             
        ss_end = str(self.year - 1) + self.four
        sql10 = "select `B111501`, `B311101` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and RPT_SRC = '年报' and ENDDATE = '%(end)s'" % {"table": table2, "code": self.code, "end":ss_end}
        self.cursor_att.execute(sql10)
        s_data = self.cursor_att.fetchone()
        
#         sql8 = "select `B311101` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and ENDDATE = '%(year)s'" % {"table": table2, "code": self.code, "year":str(self.year-1) + self.four}
#         self.cursor_att.execute(sql8)
#         last_mother = self.cursor_att.fetchone()
        
        data_list = []    
        
        for key in key_list:
            
            table = 'stk_income_gen'
            insert_columns = ['`P160101`', '`P150101`', '`P110100`', '`P240801`', '`P110202`']
            start = str(self.year) + self.mins
            print(start)
            end = str(self.year) + key
            print(end)
            sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code, "start":start, "end":end}
            print(sql)
            self.cursor_att.execute(sql)
            res = self.cursor_att.fetchone()
            if res:
                columns = ['`B311101`', '`B310201`', '`B310701`', '`B111501`', '`B111511`', '`B200000`', '`B100000`']
                
                sql2 = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and ENDDATE = '%(end)s'" % {"table": table2, "columns": ", ".join(columns), "code": self.code, "end":end}
                self.cursor_att.execute(sql2)
                data = self.cursor_att.fetchone()
                print(data)
                print('jingzichang:', data.get('B311101', 0))
                self.jzc = data.get('B311101', 0) if data.get('B311101', 0) else 0
                
                table3 = 'ana_stk_hold_dtl'
                sql3 = "select TOT_SHARE from %(table)s where INNER_CODE = '%(inner_code)s' and TOT_SHARE is not null and ENDDATE <= '%(end)s' order by ENDDATE desc" % {"table": table3, "inner_code": comcode.get('INNER_CODE', ''), "end":end}
                print(sql3)
                self.cursor_att.execute(sql3)
                total = self.cursor_att.fetchone()
                print('total', total)
                
                table4 = 'stk_cash_gen'
                sql4 = "select `C100000` from %(table)s where A_STOCKCODE = '%(code)s' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table4, "code": self.code, "start":start, "end":end}
                self.cursor_att.execute(sql4)
                gen = self.cursor_att.fetchone()
                
                s_start = str(self.year-1) + self.mins
                s_end = str(self.year-1) + key
                s_sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code, "start":s_start, "end":s_end}
                self.cursor_att.execute(s_sql)
                s_rest = self.cursor_att.fetchone()
                
                sql5 = "select `B311101` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and ENDDATE = '%(end)s'" % {"table": table2, "code": self.code, "end":s_end}
                self.cursor_att.execute(sql5)
                s_mother = self.cursor_att.fetchone()
                
                sql6 = "select TOT_SHARE from %(table)s where INNER_CODE = '%(inner_code)s'  and ENDDATE = '%(year)s'" % {"table": table3, "inner_code": comcode.get('INNER_CODE', ''), "year":str(self.year-1) + self.four}
                print(sql6)
                self.cursor_att.execute(sql6)
                last_total = self.cursor_att.fetchone()
                
                sql9 = "select TOT_SHARE from %(table)s where INNER_CODE = '%(inner_code)s'  and ENDDATE <= '%(year)s' order by ENDDATE desc" % {"table": table3, "inner_code": comcode.get('INNER_CODE', ''), "year":str(self.year-1) + key}
                self.cursor_att.execute(sql9)
                s_total = self.cursor_att.fetchone()
                
                # 每股收益
                income_per_stock = round(res.get('P240801', 0), 2)
                # 净利润
                net_profit = res.get('P160101', 0)
                # 营业收入
                income = res.get('P110100', 0)
                # 营业成本
                cb_income = res.get('P110202', 0)
                # 净利润增长率
                net_profit_growth_rate = round(net_profit/s_rest.get('P160101', 0) -1, 4)
                # 营业收入增长率
                income_growth_rate = round(income/s_rest.get('P110100', 0) -1, 4)
                #每股净资产
                if total.get('TOT_SHARE', 0):
                    net_assets_per_stock = round(data.get('B311101', 0)/total.get('TOT_SHARE', 0), 2)
                    # 每股资本公积金
                    capital_surpluses_per_stock = round(data.get('B310201', 0)/ total.get('TOT_SHARE', 0), 2)
                    #每股未分配利润
                    profit_unallocated_per_stock = round(data.get('B310701', 0)/ total.get('TOT_SHARE', 0), 2)
                    # 每股经营现金流
                    operating_cash_flow_per_stock = round(gen.get('C100000', 0)/ total.get('TOT_SHARE', 0), 2)
                else:
                    net_assets_per_stock = 0
                    # 每股资本公积金
                    capital_surpluses_per_stock = 0
                    #每股未分配利润
                    profit_unallocated_per_stock = 0
                    # 每股经营现金流
                    operating_cash_flow_per_stock = 0
                    
                if s_total.get('TOT_SHARE', 0):
                    # 上期每股净资产
                    last_net_assets_per_stock = round(s_mother.get('B311101', 0)/s_total.get('TOT_SHARE', 0), 2)
                else:
                    last_net_assets_per_stock = 0
                # 每股净资产同比增幅
                net_assets_per_stock_growth_rate = round(net_assets_per_stock/last_net_assets_per_stock -1, 4)
                if last_total.get('TOT_SHARE', 0):
                    # 期初每股净资产
                    start_net_assets_per_stock = round(s_data.get('B311101', 0)/last_total.get('TOT_SHARE', 0), 2)
                else:
                    start_net_assets_per_stock = 0  
                print(start_net_assets_per_stock, '期初')
                # 净资产收益率 2*每股收益/（期初每股净资产+期末每股净资产）
                net_assets_income_rate = 2 * income_per_stock / (start_net_assets_per_stock + net_assets_per_stock)
                # 净资产收益率摊薄
                if net_assets_per_stock:
                    net_assets_income_rate_dilute = round(net_profit / data.get('B311101', 0), 4)
                else:
                    net_assets_income_rate_dilute = 0
    
                # 存货周转率
                if data.get('B111501', 0):
                    store_turnover_rate = round(2*cb_income / (data.get('B111501', 0) + s_data.get('B111501', 0)), 2)
                else:
                    store_turnover_rate = 0
                # 销售毛利率，
                gross_profit_rate = round((income - cb_income) / income, 4)
                # 资产负债比率
                assets_debt_ratio = round(data.get('B200000', 0) / data.get('B100000', 0), 4)
                # 日期
                tr_date = ''.join(end.split('-'))
                
                print('每股收益 :', income_per_stock)
                print('净利润 :', net_profit)
                print('营业收入 :', income)
                print('净利润增长率 :', net_profit_growth_rate)
                print('营业收入增长率 :', income_growth_rate)
                print('每股净资产', net_assets_per_stock)
                print('每股净资产同比增幅 :', net_assets_per_stock_growth_rate)
                print('净资产收益率 :', net_assets_income_rate)
                print('净资产收益率摊薄 :', net_assets_income_rate_dilute)
                print('每股资本公积金 :', capital_surpluses_per_stock)
                print('每股未分配利润 :', profit_unallocated_per_stock)
                print('存货周转率 :', store_turnover_rate)
                print('每股经营现金流 :', operating_cash_flow_per_stock)
                print('销售毛利率 :', gross_profit_rate)
                print('资产负债比率 :', assets_debt_ratio)
                print('tr_date :', tr_date)
                print('cb_income', cb_income)
                
                test_mgsx = 2 * net_profit / (s_data.get('B311101', 0) + data.get('B311101', 0))
                print('期初净资产：', s_data.get('B311101', 0))
                print('期末净资产：', data.get('B311101', 0))
                print('净资产收益率V2', test_mgsx)
                i = {"income_per_stock":income_per_stock ,"net_profit": net_profit, "income": income,"net_profit_growth_rate":net_profit_growth_rate ,"income_growth_rate": income_growth_rate,
                     "net_assets_per_stock": net_assets_per_stock,"net_assets_per_stock_growth_rate": net_assets_per_stock_growth_rate,"net_assets_income_rate": net_assets_income_rate,
                     "net_assets_income_rate_dilute": net_assets_income_rate_dilute,"capital_surpluses_per_stock": capital_surpluses_per_stock,"profit_unallocated_per_stock": profit_unallocated_per_stock,
                     "store_turnover_rate": store_turnover_rate,"operating_cash_flow_per_stock": operating_cash_flow_per_stock,"gross_profit_rate": gross_profit_rate,"assets_debt_ratio": assets_debt_ratio,
                     "tr_date": tr_date,"B311101": data.get('B311101', 0), "zgb":total.get('TOT_SHARE', 0)}
                data_list.append(i)
            else:
                print('nothing is happend')
                
        self.jzc = data_list[0].get('B311101', 0)
        self.yysr = data_list[0].get('income', 0)
        self.gzb = data_list[0].get('zgb', 0)
        self.avg_price = data_list[0].get('net_assets_per_stock', 0)
        self.cursor.close()
        self.cursor_att.close()
             
     
    def get_finance_basic_data(self):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        table = 'stk_com_profile'
        insert_columns = ['`PRI_BIZ`', '`SW_INDU_CODE_2`']
        sql = "select %(columns)s from %(table)s where COMCODE = '%(code)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self._comcode}
        self.cursor_att.execute(sql)
        com_info = self.cursor_att.fetchone()
        print('主营业务：',com_info.get('PRI_BIZ', ''))
        print('行业：', com_info.get('SW_INDU_CODE_2', ''))

        table3 = 'stk_shr_stru'
        columns = ['`TOTAL`', '`FL_SHR`', '`FL_ASHR`', '`TOT_LTDFL`']
        sql3 = "SELECT %(columns)s FROM %(table)s where A_STOCKCODE = '%(code)s' order by CHANGEDATE desc limit 1" % {"table": table3, "columns": ", ".join(columns), "code": self.code}
        self.cursor_att.execute(sql3)
        total_data = self.cursor_att.fetchone()
        print('总股本：', total_data.get('TOTAL', 0))
        print('流通股：', total_data.get('FL_SHR', 0))
        self.ltg = total_data.get('FL_SHR', 0)
        print('流通A股：', total_data.get('FL_ASHR', 0))
        print('A股总股本：', total_data.get('FL_ASHR', 0))
        print('限售A股：', total_data.get('TOT_LTDFL', 0))
        
        table4 = 'stk_mkt'
        sql4 = "SELECT TCLOSE, TVOLUME FROM %(table)s where SECCODE = '%(code)s' order by TRADEDATE desc limit 1" % {"table":table4, "code": self.code}
        self.cursor_att.execute(sql4)
        kline = self.cursor_att.fetchone()
        
        print('收盘价', kline.get('TCLOSE', 0))
        print('成交量', kline.get('TVOLUME', 0))
        a = total_data.get('FL_ASHR', 0) * kline.get('TCLOSE', 0)
        
        print('净资产:', self.jzc)
        print('营业收入:', self.yysr)
        print('市净率：',a / self.jzc)
        print('市销率：',a / self.yysr)
        print('换手率：', kline.get('TVOLUME', 0) / total_data.get('FL_SHR', 0))
        print('总市值：', total_data.get('TOTAL', 0) * kline.get('TCLOSE', 0))
        print('流通市值：',total_data.get('FL_SHR', 0) * kline.get('TCLOSE', 0))
        
        table5 = 'stk_income_gen'    
        end = str(self.year) + self.four  
        ss_end = str(self.year - 1) + self.four
        start = str(self.year) + self.mins
        sql5 = "select `P240801` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and RPT_SRC = '年报' and ENDDATE = '%(end)s'" % {"table": table5, "code": self.code, "end":end}
        self.cursor_att.execute(sql5)
        s_data = self.cursor_att.fetchone()
        if not s_data:
            sql5 = "select `P240801` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and RPT_SRC = '年报' and ENDDATE = '%(end)s'" % {"table": table5, "code": self.code, "end":ss_end}
            self.cursor_att.execute(sql5)
            s_data = self.cursor_att.fetchone()
        
        print('静态市盈率：', kline.get('TCLOSE', 0)/ s_data.get('P240801', 0))
        now = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        sql6 = "select `P240801`, `ENDDATE` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and STARTDATE = '%(start)s' and ENDDATE <= '%(end)s' order by ENDDATE desc limit 1" % {"table": table5, "code": self.code, "start": start, "end":now}
        print(sql6)
        self.cursor_att.execute(sql6)
        now_data = self.cursor_att.fetchone()
        time_date = now_data.get('ENDDATE', 0)
        month = str(time_date).split('-')[1]
        print(month)
        if month == '03':
            money =  float(now_data.get('P240801', 0)) * 4
        elif month == '06':
            money =  float(now_data.get('P240801', 0)) * 2
        elif month == '09':
            money =  float(now_data.get('P240801', 0)) * (4 / 3)
        elif month == '12':
            money =  now_data.get('P240801', 0)
        else:
            money = 1
        print('动态市盈率：', float(kline.get('TCLOSE', 0))/ money)
        self.cursor.close()
        self.cursor_att.close()
    
    
    def stock_holder_top_ten(self):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        table = 'stk_cir_holder'
        insert_columns = ['`TOT_HOLD_NUM`', '`TOT_SHR_PCT`', '`RANK`', '`NAME`', '`HOLD_PCT`', '`HOLD_NUM`', '`ENDDATE`']
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' order by ENDDATE desc limit 10" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code}
        self.cursor_att.execute(sql)
        holder_data = self.cursor_att.fetchall()
        top10_dict = {1:[], 2:[], 3:[], 4:[], 5:[], 6:[], 7:[], 8:[], 9:[], 10:[]}
        # 获取十大股东 存入数据库
        for data in holder_data:
            holder = data.get('NAME', '')
            cnt = data.get('HOLD_NUM', '')
            ratio = data.get('HOLD_PCT', '')
            remark = data.get('ENDDATE', '')
            rank = int(data.get('RANK', ''))
            top10_dict[rank] = [holder, cnt, ratio, remark, self.code]
            
            print('股东名称', holder )
            print('持股数', cnt )
            print('持股比例', ratio )
            print('时间', remark )
        top10_all_num = holder_data[0].get('TOT_HOLD_NUM', 0)
        top10_all_ratio = holder_data[0].get('TOT_SHR_PCT', 0)
#         last_all_num = holder_data[10].get('TOT_HOLD_NUM', 0)

        sql2 = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' order by ENDDATE desc limit 10, 1" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code}
        self.cursor_att.execute(sql2)
        data_num = self.cursor_att.fetchone()
        last_all_num = data_num.get('TOT_HOLD_NUM', 0)
        change_num = (top10_all_num - last_all_num) / last_all_num
        print('前十大流通股东累计持股: ', top10_all_num)
        print('前十大流通股东累计占流通股比: ', top10_all_ratio)
        print('前十大流通股东累计持股环比变化: ', change_num)
        
        lt_top10_1 = top10_dict[1][0]
        lt_top10_1_hold = top10_dict[1][1]
        lt_top10_1_hold_ratio = top10_dict[1][2]
        lt_top10_2 = top10_dict[2][0]
        lt_top10_2_hold = top10_dict[2][1]
        lt_top10_2_hold_ratio = top10_dict[2][2]
        lt_top10_3 = top10_dict[3][0]
        lt_top10_3_hold = top10_dict[3][1]
        lt_top10_3_hold_ratio = top10_dict[3][2]
        lt_top10_4 = top10_dict[4][0]
        lt_top10_4_hold = top10_dict[4][1]
        lt_top10_4_hold_ratio = top10_dict[4][2]
        lt_top10_5 = top10_dict[5][0]
        lt_top10_5_hold = top10_dict[5][1]
        lt_top10_5_hold_ratio = top10_dict[5][2]
        lt_top10_6 = top10_dict[6][0]
        lt_top10_6_hold = top10_dict[6][1]
        lt_top10_6_hold_ratio = top10_dict[6][2]
        lt_top10_7 = top10_dict[7][0]
        lt_top10_7_hold = top10_dict[7][1]
        lt_top10_7_hold_ratio = top10_dict[7][2]
        lt_top10_8 = top10_dict[8][0]
        lt_top10_8_hold = top10_dict[8][1]
        lt_top10_8_hold_ratio = top10_dict[8][2]
        lt_top10_9 = top10_dict[9][0]
        lt_top10_9_hold = top10_dict[9][1]
        lt_top10_9_hold_ratio = top10_dict[9][2]
        lt_top10_10 = top10_dict[10][0]
        lt_top10_10_hold = top10_dict[10][1]
        lt_top10_10_hold_ratio = top10_dict[10][2]
        
        print('第一大流通股东股东名称', lt_top10_1 )
        print('第一大流通股东持股数', lt_top10_1_hold )
        print('第一大流通股东持股比例', lt_top10_1_hold_ratio )
        print('第二大流通股东股东名称', lt_top10_2 )
        print('第二大流通股东持股数', lt_top10_2_hold )
        print('第二大流通股东持股比例', lt_top10_2_hold_ratio )
        print('第三大流通股东股东名称', lt_top10_3 )
        print('第三大流通股东持股数', lt_top10_3_hold )
        print('第三大流通股东持股比例', lt_top10_3_hold_ratio )
        print('第四大流通股东股东名称', lt_top10_4 )
        print('第四大流通股东持股数', lt_top10_4_hold )
        print('第四大流通股东持股比例', lt_top10_4_hold_ratio )
        print('第五大流通股东股东名称', lt_top10_5 )
        print('第五大流通股东持股数', lt_top10_5_hold )
        print('第五大流通股东持股比例', lt_top10_5_hold_ratio )
        print('第六大流通股东股东名称', lt_top10_6 )
        print('第六大流通股东持股数', lt_top10_6_hold )
        print('第六大流通股东持股比例', lt_top10_6_hold_ratio )
        print('第七大流通股东股东名称', lt_top10_7 )
        print('第七大流通股东持股数', lt_top10_7_hold )
        print('第七大流通股东持股比例', lt_top10_7_hold_ratio )
        print('第八大流通股东股东名称', lt_top10_8 )
        print('第八大流通股东持股数', lt_top10_8_hold )
        print('第八大流通股东持股比例', lt_top10_8_hold_ratio )
        print('第九大流通股东股东名称', lt_top10_9 )
        print('第九大流通股东持股数', lt_top10_9_hold )
        print('第九大流通股东持股比例', lt_top10_9_hold_ratio )
        print('第十大流通股东股东名称', lt_top10_10 )
        print('第十大流通股东持股数', lt_top10_10_hold )
        print('第十大流通股东持股比例', lt_top10_10_hold_ratio )
        
        table2 = 'stk_holder_num'
        sql2 = "SELECT TOT_HLD, FL_HLD FROM %(table)s where A_STOCKCODE = '%(code)s' order by DECLAREDATE desc limit 2" % {"table" : table2, "code": self.code}
        self.cursor_att.execute(sql2)
        holder_num = self.cursor_att.fetchall()
        if holder_num:
            fl1 = holder_num[0].get('FL_HLD', 0)
            fl2 = holder_num[1].get('FL_HLD', 0)
            fl_num = (fl1 - fl2) / fl2
        else:
            # 流通股东户数
            fl1 = 0
            # 流通股东户数环比增长
            fl_num = 0
            
        print('流通股东数: ', fl1)
        print('流通股东户数环比增长:', fl_num )
        table3 = 'ana_stk_hold_dtl'
        columns = ['`HLD_TOT_NUM`', '`HLD_MKTCAP`', '`HLD_FL_PCT`', '`TOT_FL_NUM`']
        sql3 = "select %(columns)s from %(table)s where INNER_CODE = '%(code)s' and ENDDATE = (select max(ENDDATE) from %(table)s where INNER_CODE = '%(code)s')" % {"table": table3, "columns": ", ".join(columns), "code": self._inner_code, "end": str(self.year) + self.third}
        self.cursor_att.execute(sql3)
        holder_num = self.cursor_att.fetchall()
        zltg = holder_num[0].get('TOT_FL_NUM', 0)
        avg_stk = zltg / fl1
        print('流通股东人均持股：', avg_stk)
        print('持股机构数量：', len(holder_num))
        print('持股机构数量：', len(holder_num))
        print('持股机构累计持股数量：', sum([data.get('HLD_TOT_NUM', 0) for data in holder_num]))
        print('持股机构持股累计市值：', sum([data.get('HLD_MKTCAP', 0) for data in holder_num]))
        print('持股机构持股累计比例：', sum([data.get('HLD_FL_PCT', 0) for data in holder_num]))
        self.cursor.close()
        self.cursor_att.close()
    
    
    def get_stock_holder_jg(self):
        item_list = []
        table3 = 'ana_stk_hold_dtl'
        columns = ['`HLD_TOT_NUM`', '`HLD_MKTCAP`', '`HLD_FL_PCT`', '`ORGCODE`', '`ENDDATE`']
        sql3 = "select %(columns)s from %(table)s where INNER_CODE = '%(code)s'" % {"table": table3, "columns": ", ".join(columns), "code": self._inner_code, "end": str(self.year) + self.third}
        self.cursor_att.execute(sql3)
        data_list = self.cursor_att.fetchall()
        if data_list:
            for data in data_list[:20]:
                orgcode = data.get('ORGCODE', 0)
                table = 'org_profile'
                columns2 = ['`CNAME`', '`TYPE_NAME`']
                sql = "select %(columns)s from %(table)s where ORGCODE = '%(orgcode)s'" % {"table": table, "columns": ", ".join(columns2), "orgcode": orgcode}
                self.cursor_att.execute(sql)
                org_data = self.cursor_att.fetchone()
                cnt = data.get('HLD_TOT_NUM', 0)
                value = data.get('HLD_MKTCAP', 0)
                ratio = data.get('HLD_FL_PCT', 0)
                remark = data.get('ENDDATE', 0)
                if org_data:
                    holder = org_data.get('CNAME', '')
                    type = org_data.get('TYPE_NAME', '')
                else:
                    holder = ''
                    type = ''
                print('机构名称：', holder)
                print('机构类型：', type)
                print('持股数量：', cnt)
                print('持股市值：', value)
                print('占流通股比例：', ratio)
                print('时间：', remark)
                item_list.append([holder, type, cnt, value, ratio]) 
        self.cursor.close()
        self.cursor_att.close()   
        
    
    def get_stock_holder_name(self):
        pass   
        table = 'stk_cir_holder'
        columns = ['`NAME`', '`HOLD_NUM`', '`HOLD_PCT`', '`ENDDATE`']
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' order by ENDDATE desc limit 10" % {"table": table, "columns": ", ".join(columns), "code": self.code}
        self.cursor_att.execute(sql)
        holder_data = self.cursor_att.fetchall()
        for data in holder_data:
            holder = data.get('NAME', '')
            cnt = data.get('HOLD_NUM', '')
            ratio = data.get('HOLD_PCT', '')
            remark = data.get('ENDDATE', '')
            
            print('股东名称', holder )
            print('持股数', cnt )
            print('持股比例', ratio )
            print('时间', remark )
        self.cursor.close()
        self.cursor_att.close()
    
    
if __name__ == "__main__":
    p = Stock_Finance(2017, '002252', '101000001')
    p.get_finance_datas()
    p.get_finance_basic_data()
    p.stock_holder_top_ten()
    server.close()
    print("111111111111") 
