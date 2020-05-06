
import MySQLdb as mysql
from MySQLdb import cursors
import time
from sshtunnel import SSHTunnelForwarder  

# 财务数据导入程序

config = {
    'HOST': '127.0.0.1',
    'PORT': 3306,
    'USER': 'root',
    'PASSWD': 'yoquant',
    'DB':'stock',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}


server = SSHTunnelForwarder(  
         ('101.200.83.229', 22),    #B机器的配置  
         ssh_password="Yoquant130612",  
         ssh_username="root",  
         remote_bind_address=('rds47r30r2052hg03je8.mysql.rds.aliyuncs.com', 3306)) #A机器的配置  

server.start()
  
att_conn = mysql.connect(host='127.0.0.1',              #此处必须是是127.0.0.1  
                       port=server.local_bind_port,  
                       user='yoquant',  
                       passwd='t19861012',  
                       db='julingdata',
                       charset='utf8',
                       cursorclass=config.get('CURSOR', cursors.DictCursor))  

conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                           user=config['USER'], passwd=config['PASSWD'],
                           db=config['DB'], charset=config['CHARSET'],
                           cursorclass=config.get('CURSOR', cursors.DictCursor))



HOLDER_NUM = []

STOCK_FINANCE = []

SROCK_FINANCE_BASIC = []

TOP_TEN = []



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
    
    def __init__(self, year, code):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        # 当前年份 如 2017 int
        self.year = year
        self.code = code
        self.name = None
        self._comcode = None
        self.jzc = self.get_jin_zi_chan()
        self.zgb = self.get_zong_gu_ben()
        self.yysr = self.get_yin_ye_shou_ry()
        self._inner_code = None
    
    
    def get_zong_gu_ben(self):
        '''获取总股本'''
        table = 'stk_shr_stru'
        sql = "select TOTAL from %(table)s where A_STOCKCODE = '%(code)s' and TOTAL is not null order by CHANGEDATE desc limit 1" % {"table": table, "code": self.code}
        self.cursor_att.execute(sql)
        total = self.cursor_att.fetchone()
        return total.get('TOTAL', 0)
    
    
    def get_jin_zi_chan(self):
        '''获取净资产'''
        table = 'stk_bala_gen'
        columns = ['`B311101`']
        # 获取当期B311101：归属母公司所有者权益， B310201：资本公积金， B310701：未分配利润，B111501：存货净额，B111511：存货原值， B200000：负债合计， B100000：资产总计
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' order by  ENDDATE desc limit 1" % {"table": table, "columns": ", ".join(columns), "code": self.code}
        self.cursor_att.execute(sql)
        data = self.cursor_att.fetchone()
        return data.get('B311101', 0) if data else 0
    
    
    def get_yin_ye_shou_ry(self):
        '''获取营业收入'''
        table = 'stk_income_gen'
        insert_columns = ['`P110101`']
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' order by ENDDATE desc limit 1" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code}
        self.cursor_att.execute(sql)
        res = self.cursor_att.fetchone()
        return res.get('P110101', 0)
    
    
    def get_stock_name_info(self):
        # 获取该股票的公司代码，和巨灵内部代码，和股票名字
        table = 'stk_code'
        sql = "select COMCODE, INNER_CODE, STOCKSNAME from %(table)s where STOCKCODE = '%(code)s'" % {"table": table, "code":self.code}
        self.cursor_att.execute(sql)
        comcode = self.cursor_att.fetchone()
        self._comcode = comcode.get('COMCODE', 0)
        self._inner_code = comcode.get('INNER_CODE', 0)
        self.name = comcode.get('STOCKSNAME', '')
    
    
    def get_finance_datas(self):
        '''计算财务数据指标'''
        key_list = [self.four, self.third, self.second, self.first]
        
        # B111501：存货净额  B311101：归属母公司所有者权益
        table2 = 'stk_bala_gen'             
        ss_end = str(self.year - 1) + self.four
        sql10 = "select `B111501`, `B311101` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and RPT_SRC = '年报' and ENDDATE = '%(end)s'" % {"table": table2, "code": self.code, "end":ss_end}
        self.cursor_att.execute(sql10)
        s_data = self.cursor_att.fetchone()
        tables = 'stock_finance'
        sqls = "select max(`tr_date`) tr_date from %(tables)s where code = '%(code)s' and type = '2' and tr_date regexp '%(year)s'" % {"tables": tables, "code": self.code, "year": self.year}
        self.cursor.execute(sqls)
        tr_date = self.cursor.fetchone()
        max_time = tr_date.get('tr_date', '0') if tr_date.get('tr_date', '0') else '0'
        season = 4
        type = 2
        for key in key_list:
            start = str(self.year) + self.mins
            end = str(self.year) + key
            tr_end = ''.join(end.split(' ')[0].split('-'))
            
            # 判断时间是否大于当前最大时间，若大于则证明数据有更新
            if tr_end > str(max_time):
            # 获取股票季度 P160101：净利润， P110101：营业收入， P240801：每股收益， P110202：营业成本
                table = 'stk_income_gen'
                insert_columns = ['`P160101`', '`P150101`', '`P110101`', '`P240801`', '`P110202`']
                sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code, "start":start, "end":end}
                self.cursor_att.execute(sql)
                res = self.cursor_att.fetchone()
                if res:
                    columns = ['`B311101`', '`B310201`', '`B310701`', '`B111501`', '`B111511`', '`B200000`', '`B100000`']
                    # 获取当期B311101：归属母公司所有者权益， B310201：资本公积金， B310701：未分配利润，B111501：存货净额，B111511：存货原值， B200000：负债合计， B100000：资产总计
                    sql2 = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and ENDDATE = '%(end)s'" % {"table": table2, "columns": ", ".join(columns), "code": self.code, "end":end}
                    self.cursor_att.execute(sql2)
                    data = self.cursor_att.fetchone()
                    
                    # 获取总股本
                    table3 = 'stk_shr_stru'
                    sql3 = "select TOTAL from %(table)s where A_STOCKCODE = '%(code)s' and TOTAL is not null and CHANGEDATE <= '%(end)s' order by CHANGEDATE desc" % {"table": table3, "code": self.code, "end":end}
                    self.cursor_att.execute(sql3)
                    total = self.cursor_att.fetchone()
                    # C100000:经营活动产生的现金流量净额
                    table4 = 'stk_cash_gen'
                    sql4 = "select `C100000` from %(table)s where A_STOCKCODE = '%(code)s' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table4, "code": self.code, "start":start, "end":end}
                    self.cursor_att.execute(sql4)
                    gen = self.cursor_att.fetchone()
                    
                    # 获取上年同期数据
                    s_start = str(self.year-1) + self.mins
                    s_end = str(self.year-1) + key
                    s_sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and STARTDATE = '%(start)s' and ENDDATE = '%(end)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code, "start":s_start, "end":s_end}
                    self.cursor_att.execute(s_sql)
                    s_rest = self.cursor_att.fetchone()
                    
                    # 获取上年同期归属母公司所有者权益
                    sql5 = "select `B311101` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and ENDDATE = '%(end)s'" % {"table": table2, "code": self.code, "end":s_end}
                    self.cursor_att.execute(sql5)
                    s_mother = self.cursor_att.fetchone()
                    # 获取上年同期的总股本
                    sql9 = "select TOTAL from %(table)s where A_STOCKCODE = '%(code)s'  and CHANGEDATE <= '%(year)s' order by CHANGEDATE desc" % {"table": table3, "code": self.code, "year":str(self.year-1) + key}
                    self.cursor_att.execute(sql9)
                    s_total = self.cursor_att.fetchone()
                    
                    if res:
                        # 每股收益
                        income_per_stock = round(res.get('P240801', 0), 4) if res.get('P240801', 0) else 0
                        # 净利润
                        net_profit = round(res.get('P160101', 0)/10000, 4) if res.get('P160101', 0) else 0
                        # 营业收入
                        income = round(res.get('P110101', 0)/10000, 4) if res.get('P110101', 0) else 0
                        # 营业成本
                        cb_income = round(res.get('P110202', 0)/10000, 4) if res.get('P110202', 0) else 0
                    else:
                        # 每股收益
                        income_per_stock = 0
                        # 净利润
                        net_profit = 0
                        # 营业收入
                        income = 0
                        # 营业成本
                        cb_income = 0
                    
                    if s_rest:
                        # 净利润增长率
                        net_profit_growth_rate = round(net_profit/round(s_rest.get('P160101', 0)/10000, 4) -1, 4) if s_rest.get('P160101', 0) else 0
                        # 营业收入增长率
                        income_growth_rate = round(income/round(s_rest.get('P110101', 0)/10000, 4) -1, 4) if s_rest.get('P110101', 0) else 0
                    else:
                        net_profit_growth_rate = 0
                        income_growth_rate = 0
                    
                    #每股净资产
                    net_assets_per_stock = 0
                    # 每股资本公积金
                    capital_surpluses_per_stock = 0
                    #每股未分配利润
                    profit_unallocated_per_stock = 0
                    # 每股经营现金流
                    operating_cash_flow_per_stock = 0
                    # 上期每股净资产
                    last_net_assets_per_stock = 0
                    
                    if total and data:
                        if total.get('TOTAL', 0):
                            #每股净资产
                            net_assets_per_stock = round(data.get('B311101', 0)/total.get('TOTAL', 0), 4) if data.get('B311101', 0) else 0
                            # 每股资本公积金
                            capital_surpluses_per_stock = round(data.get('B310201', 0)/ total.get('TOTAL', 0), 4) if data.get('B310201', 0) else 0
                            #每股未分配利润
                            profit_unallocated_per_stock = round(data.get('B310701', 0)/ total.get('TOTAL', 0), 4) if data.get('B310701', 0) else 0
                            if gen:
                                # 每股经营现金流
                                operating_cash_flow_per_stock = round(gen.get('C100000', 0)/ total.get('TOTAL', 0), 4) if gen.get('C100000', 0) else 0
                            else:
                                operating_cash_flow_per_stock = 0
                    
                    if s_total and s_mother:
                        if s_total.get('TOTAL', 0):
                            # 上期每股净资产
                            last_net_assets_per_stock = round(s_mother.get('B311101', 0)/s_total.get('TOTAL', 0), 4) if s_mother.get('B311101', 0) else 0
                    
                    net_assets_per_stock_growth_rate = 0
                    if last_net_assets_per_stock:
                        # 每股净资产同比增幅
                        net_assets_per_stock_growth_rate = round(net_assets_per_stock/last_net_assets_per_stock -1, 4)
                    
                    # 净资产收益率摊薄
                    if net_assets_per_stock and net_profit:
                        net_assets_income_rate_dilute = round(net_profit / round(data.get('B311101', 0)/10000, 4), 4)
                    else:
                        net_assets_income_rate_dilute = 0
                    store_turnover_rate = 0
                    net_assets_income_rate = 0
                    if data and s_data :
                        if data.get('B311101', 0) and s_data.get('B311101', 0):
                            # 净资产收益率 2*每股收益/（期初每股净资产+期末每股净资产）
                            net_assets_income_rate = round(2 * net_profit / (round(s_data.get('B311101', 0)/10000, 4) + round(data.get('B311101', 0)/10000, 4)), 4)
                        
                    # 存货周转率
                        if data.get('B111501', 0) and s_data.get('B111501', 0):
                            store_turnover_rate = round(2*cb_income / (round(data.get('B111501', 0)/10000, 4) + round(s_data.get('B111501', 0)/10000, 4)), 2)
                    # 销售毛利率，
                    gross_profit_rate = round((income - cb_income) / income, 4) if income else 0
                    assets_debt_ratio = 0
                    if data:
                        if data.get('B100000', 0):
                    # 资产负债比率
                            assets_debt_ratio = round(data.get('B200000', 0) / data.get('B100000', 0), 4) if data.get('B200000', 0) else 0
                    
                    # 日期
                    tr_date = ''.join(end.split('-'))
                    
                    item = [self.code, tr_date, type, season, income_per_stock, net_profit, round(net_profit_growth_rate*100, 4), income, round(income_growth_rate*100, 4), net_assets_per_stock, round(net_assets_per_stock_growth_rate*100, 4),
                            round(net_assets_income_rate*100, 4), round(net_assets_income_rate_dilute*100, 4), capital_surpluses_per_stock, profit_unallocated_per_stock, operating_cash_flow_per_stock, store_turnover_rate, gross_profit_rate, assets_debt_ratio]
                    STOCK_FINANCE.append(item)
                else:
                    print('    get_finance_datas  nothing is happend time in %s' %end)
            else:
                print('    get_finance_datas the date is existed  time in %s' % end)
            season -= 1   
        self.cursor.close()
        self.cursor_att.close()
    
    
    def inster_stock_finace_data(self, data_list):
        '''添加财务数据w'''
        table = 'stock_finance'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`code`', '`tr_date`', '`type`', '`season`', '`income_per_stock`', '`net_profit`', '`net_profit_growth_rate`', '`income`', '`income_growth_rate`', '`net_assets_per_stock`', '`net_assets_per_stock_growth_rate`',
                            '`net_assets_income_rate`', '`net_assets_income_rate_dilute`', '`capital_surpluses_per_stock`', '`profit_unallocated_per_stock`', '`operating_cash_flow_per_stock`', '`store_turnover_rate`', '`gross_profit_rate`', '`assets_debt_ratio`']

        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = data_list
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print(' inster_stock_finance_data is error %s ' % e)
        else:
            print(' inster_stock_finance_data is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
    def check_basic_tr_date(self, tr_date):
        table = 'stock_finance_basic'
        self.cursor = conn.cursor()
        sql = "select * from %(table)s where code = '%(code)s' and tr_date='%(tr_date)s'" % {"table": table, "code": self.code, "tr_date":tr_date}
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        return data
    
    
    def get_finance_basic_data(self):
        '''获取财务基础数据
        PRI_BIZ：主营业务, SW_INDU_CODE_2：行业名称, TOTAL：总股本， FL_SHR：流通股， FL_ASHR：流通A股， FL_ASHR：A股总股本， TOT_LTDFL：限售A股,TCLOSE:今日收盘价, 
        TVOLUME： 今日成交量, TRADEDATE： 今日日期，P240801：每股收益，hsl:换手率
        '''
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        # PRI_BIZ：主营业务  SW_INDU_CODE_2：行业名称
        table = 'stk_com_profile'
        insert_columns = ['`PRI_BIZ`', '`SW_INDU_CODE_2`']
        sql = "select %(columns)s from %(table)s where COMCODE = '%(code)s'" % {"table": table, "columns": ", ".join(insert_columns), "code": self._comcode}
        self.cursor_att.execute(sql)
        com_info = self.cursor_att.fetchone()
        if com_info:
            mian_business = com_info.get('PRI_BIZ', '')
            hy = com_info.get('SW_INDU_CODE_2', '')
        else:
            mian_business = ''
            hy = ''
        
        # TOTAL：总股本， FL_SHR：流通股， FL_ASHR：流通A股， FL_ASHR：A股总股本， TOT_LTDFL：限售A股
        table3 = 'stk_shr_stru'
        columns = ['`TOTAL`', '`FL_SHR`', '`FL_ASHR`', '`TOT_LTDFL`']
        sql3 = "SELECT %(columns)s FROM %(table)s where A_STOCKCODE = '%(code)s' order by CHANGEDATE desc limit 1" % {"table": table3, "columns": ", ".join(columns), "code": self.code}
        self.cursor_att.execute(sql3)
        total_data = self.cursor_att.fetchone()
        if total_data:
            general_capital = total_data.get('TOTAL', 0) if total_data.get('TOTAL', 0) else 0
            
            lt_capital_a = total_data.get('FL_ASHR', 0) if total_data.get('FL_ASHR', 0) else 0
            general_capital_a = total_data.get('FL_ASHR', 0) if total_data.get('FL_ASHR', 0) else 0
            xs_capital_a = total_data.get('TOT_LTDFL', 0) if total_data.get('TOT_LTDFL', 0) else 0
        else: 
            general_capital = 0
            
            lt_capital_a = 0
            general_capital_a = 0
            xs_capital_a = 0
        # 获取当天股票K线数据
        table4 = 'stk_mkt'
        sql4 = "SELECT TCLOSE, TVOLUME, TRADEDATE FROM %(table)s where SECCODE = '%(code)s' order by TRADEDATE desc limit 1" % {"table":table4, "code": self.code}
        self.cursor_att.execute(sql4)
        kline = self.cursor_att.fetchone()
        if kline:
            tr_date = ''.join(str(kline.get('TRADEDATE', 0)).split(' ')[0].split('-'))
            check_data = self.check_basic_tr_date(tr_date)
            if not check_data:
                fl_ashr = total_data.get('FL_ASHR', 0) if total_data.get('FL_ASHR', 0) else 0
                tclose = kline.get('TCLOSE', 0) if kline.get('TCLOSE', 0) else 0
                a = fl_ashr * tclose
                
                if self.jzc:
                    pb = round(a / self.jzc, 4)
                else:
                    pb = 0
                if self.yysr:
                    ps = round(a / self.yysr, 4)
                else:
                    ps = 0
                # hsl 换手率
                hsl = round(kline.get('TVOLUME', 0) / total_data.get('FL_SHR', 0), 4) if total_data.get('FL_SHR', 0) else 0
                total_value = general_capital * tclose
                # lt_value 流通股市值
                if total_data:
                    lt_value = total_data.get('FL_SHR', 0) * tclose if total_data.get('FL_SHR', 0) else 0
                else:
                    lt_value = 0
                
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
                if s_data:
                    pe_static = round(tclose/ s_data.get('P240801', 0), 4) if s_data.get('P240801', 0) else 0
                else:
                    pe_static = 0
                # 最近的每股收益
                now = time.strftime('%Y-%m-%d',time.localtime(time.time()))
                sql6 = "select `P240801`, `ENDDATE` from %(table)s where A_STOCKCODE = '%(code)s' and RPT_TYPE = '合并' and STARTDATE = '%(start)s' and ENDDATE <= '%(end)s' order by ENDDATE desc limit 1" % {"table": table5, "code": self.code, "start": start, "end":now}
                self.cursor_att.execute(sql6)
                now_data = self.cursor_att.fetchone()
                time_date = now_data.get('ENDDATE', 0) if now_data else 0
                month = str(time_date).split('-')[1] if time_date else 0
                if month == '03':
                    money =  float(now_data.get('P240801', 0)) * 4 if now_data.get('P240801', 0) else 0
                elif month == '06':
                    money =  float(now_data.get('P240801', 0)) * 2 if now_data.get('P240801', 0) else 0
                elif month == '09':
                    money =  float(now_data.get('P240801', 0)) * (4 / 3) if now_data.get('P240801', 0) else 0
                elif month == '12':
                    money =  now_data.get('P240801', 0) if now_data.get('P240801', 0) else 0
                else:
                    money = 1
                # 动态市盈率
                pe_dynamic = round(float(tclose)/ float(money), 4) if money else 0
                self.cursor.close()
                self.cursor_att.close()
                
                data = [self.code, tr_date, mian_business, hy, round(general_capital/10000, 4), round(general_capital_a/10000, 4), round(lt_capital_a/10000, 4), round(xs_capital_a/10000, 4), pb, ps, pe_static, pe_dynamic, round(total_value/100000000, 4), round(lt_value/100000000, 4), hsl]
                SROCK_FINANCE_BASIC.append(data)
    
    
    def insert_stock_finance_basic_data(self, data):
        table = 'stock_finance_basic'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`code`', '`tr_date`', '`mian_business`', '`hy`', '`general_capital`', '`general_capital_a`', '`lt_capital_a`', '`xs_capital_a`', '`pb`', '`ps`', '`pe_static`', '`pe_dynamic`', '`total_value`', '`lt_value`', '`hsl`']

        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = data
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print(' insert_stock_finance_basic_data is error %s ' % e)
        else:
            print(' insert_stock_finance_basic_data is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
    def get_org_name_data(self, orgcode):
        '''获取机构名称'''
        table = 'org_profile'
        columns2 = ['`CNAME`']
        sql = "select %(columns)s from %(table)s where ORGCODE = '%(orgcode)s'" % {"table": table, "columns": ", ".join(columns2), "orgcode": orgcode}
        self.cursor_att.execute(sql)
        org_data = self.cursor_att.fetchone()
        if org_data:
            return org_data.get('CNAME', '')
        else:
            return ''
    
    
    def stock_holder_top_ten(self):
        '''股东信息
        TOT_HOLD_NUM：十大股东总持股数， TOT_SHR_PCT：十大股东总持股比例， RANK：股东排名， NAME：股东名称， HOLD_PCT：股东持股比例， HOLD_NUM：股东持股数量，ENDDATE：最后日期，
        TOT_HLD：总股东户数， HLD_ORG_NUM：持股机构数量, HLD_TOT_NUM_END：期末持股数量, HLD_TOT_PCT_END：期末持股比例, HLD_TCAP_END： 期末持股市值
        '''
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        table = 'stk_main_holder'
        insert_columns = ['`TOT_HOLD_NUM`', '`TOT_SHR_PCT`', '`RANK`', '`NAME`', '`HOLD_PCT`', '`HOLD_NUM`', '`ENDDATE`']
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and RANK <= 10 and ENDDATE = (select max(ENDDATE) from %(table)s where A_STOCKCODE = '%(code)s') order by ENDDATE desc limit 10" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code}
        self.cursor_att.execute(sql)
        holder_data = self.cursor_att.fetchall()
        if holder_data:
            # 获取本期时间
            last_time = holder_data[0].get('ENDDATE', 0) if holder_data[0] else 0
            tr_date = ''.join(str(last_time).split(' ')[0].split('-')) if last_time else '0'
            
            table10 = 'stock_holder'
            sql10 = "select max(tr_date) tr_date from %(table)s where code = '%(code)s'" % {"table":table10, "code":self.code}
            self.cursor.execute(sql10)
            data_time = self.cursor.fetchone()
            last_max_time = data_time.get('tr_date', 0) if data_time else 0
            if tr_date > str(last_max_time):
                
                top10_dict = {1:[], 2:[], 3:[], 4:[], 5:[], 6:[], 7:[], 8:[], 9:[], 10:[]}
    #             获取十大股东 
                for data in holder_data:
                    holder = data.get('NAME', '')
                    cnt = data.get('HOLD_NUM', '')
                    ratio = data.get('HOLD_PCT', '')
                    remark = data.get('ENDDATE', '')
                    rank = int(data.get('RANK', ''))
                    top10_dict[rank] = [holder, cnt, ratio, remark, self.code]
                    # 添加持股人名称
                    HOLDER_NUM.append(holder)
                
                if holder_data[0]:
                    # 十大股东总持股数和总持股比例
                    lt_top10_total_hold = round(holder_data[0].get('TOT_HOLD_NUM', 0)/10000, 4) if holder_data[0].get('TOT_HOLD_NUM', 0) else 0
                    lt_top10_total_hold_ratio = holder_data[0].get('TOT_SHR_PCT', 0) if holder_data[0].get('TOT_SHR_PCT', 0) else 0
                else:
                    lt_top10_total_hold = 0
                    lt_top10_total_hold_ratio = 0
                # 获取本期时间，取上期数据中十大股东总持股数
                
                sql2 = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' and ENDDATE = (select max(ENDDATE) from %(table)s where A_STOCKCODE = '%(code)s' and ENDDATE < '%(last_time)s') order by ENDDATE desc limit 1" % {"table": table, "columns": ", ".join(insert_columns), "code": self.code, "last_time":last_time}
                self.cursor_att.execute(sql2)
                data_num = self.cursor_att.fetchone()
                # last_all_num 上期十大股东总持股数
                last_all_num = data_num.get('TOT_HOLD_NUM', 0) if data_num else 0
                
                if not lt_top10_total_hold:
                    lt_top10_total_hold = sum([data.get('HOLD_NUM') for data in holder_data if data])
                if not lt_top10_total_hold_ratio:
                    lt_top10_total_hold_ratio = sum([data.get('HOLD_PCT') for data in holder_data if data])
                
                # lt_top10_total_hold_change_hb 十大股东持股数环比增长
                lt_top10_total_hold_change_hb = (lt_top10_total_hold - last_all_num) / last_all_num if last_all_num else 0
                
                # 十大股东信息
                lt_top10_1 = top10_dict[1][0] if top10_dict[1] else ''
                lt_top10_1_hold = top10_dict[1][1] if top10_dict[1] else ''
                lt_top10_1_hold_ratio = top10_dict[1][2] if top10_dict[1] else ''
                lt_top10_2 = top10_dict[2][0] if top10_dict[2] else ''
                lt_top10_2_hold = top10_dict[2][1] if top10_dict[2] else ''
                lt_top10_2_hold_ratio = top10_dict[2][2] if top10_dict[2] else ''
                lt_top10_3 = top10_dict[3][0] if top10_dict[3] else ''
                lt_top10_3_hold = top10_dict[3][1] if top10_dict[3] else ''
                lt_top10_3_hold_ratio = top10_dict[3][2] if top10_dict[3] else ''
                lt_top10_4 = top10_dict[4][0] if top10_dict[4] else ''
                lt_top10_4_hold = top10_dict[4][1] if top10_dict[4] else ''
                lt_top10_4_hold_ratio = top10_dict[4][2] if top10_dict[4] else ''
                lt_top10_5 = top10_dict[5][0] if top10_dict[5] else ''
                lt_top10_5_hold = top10_dict[5][1] if top10_dict[5] else ''
                lt_top10_5_hold_ratio = top10_dict[5][2] if top10_dict[5] else ''
                lt_top10_6 = top10_dict[6][0] if top10_dict[6] else ''
                lt_top10_6_hold = top10_dict[6][1] if top10_dict[6] else ''
                lt_top10_6_hold_ratio = top10_dict[6][2] if top10_dict[6] else ''
                lt_top10_7 = top10_dict[7][0] if top10_dict[7] else ''
                lt_top10_7_hold = top10_dict[7][1] if top10_dict[7] else ''
                lt_top10_7_hold_ratio = top10_dict[7][2] if top10_dict[7] else ''
                lt_top10_8 = top10_dict[8][0] if top10_dict[8] else ''
                lt_top10_8_hold = top10_dict[8][1] if top10_dict[8] else ''
                lt_top10_8_hold_ratio = top10_dict[8][2] if top10_dict[8] else ''
                lt_top10_9 = top10_dict[9][0] if top10_dict[9] else ''
                lt_top10_9_hold = top10_dict[9][1] if top10_dict[9] else ''
                lt_top10_9_hold_ratio = top10_dict[9][2] if top10_dict[9] else ''
                lt_top10_10 = top10_dict[10][0] if top10_dict[10] else ''
                lt_top10_10_hold = top10_dict[10][1] if top10_dict[10] else ''
                lt_top10_10_hold_ratio = top10_dict[10][2] if top10_dict[10] else ''
                
                # 获取股东户数信息
                table2 = 'stk_holder_num'
                sql2 = "SELECT TOT_HLD, FL_HLD FROM %(table)s where A_STOCKCODE = '%(code)s' and `TOT_HLD` is not null order by DECLAREDATE desc limit 2" % {"table" : table2, "code": self.code}
                self.cursor_att.execute(sql2)
                holder_num = self.cursor_att.fetchall()
                if holder_num:
                    lt_count = holder_num[0].get('TOT_HLD', 0) if holder_num[0] else 0
                    fl2 = holder_num[1].get('TOT_HLD', 0) if len(holder_num) == 2 else 0
                    lt_growth_rate_hb = (lt_count - fl2) / fl2 if fl2 else 0
                else:
                    # 流通股东户数
                    lt_count = 0
                    # 流通股东户数环比增长
                    lt_growth_rate_hb = 0
                    # 上期股东数
                    fl2 = 0
                
                # 获取主力持仓信息
                table3 = 'ana_stk_org_hold'
                columns = ['`HLD_ORG_NUM`', '`HLD_TOT_NUM_END`', '`HLD_TOT_PCT_END`', '`HLD_TCAP_END`', '`ENDDATE`']
                sql3 = "select %(columns)s from %(table)s where INNER_CODE = '%(code)s' and ORG_TYPE in (098, 590, 701, 702, 703, 704, 705, 710, 711, 712, 713, 714,715) and ENDDATE = (select max(ENDDATE) from %(table)s where INNER_CODE = '%(code)s')" % {"table": table3, "columns": ", ".join(columns), "code": self._inner_code, "end": str(self.year) + self.third}
                self.cursor_att.execute(sql3)
                holder_num = self.cursor_att.fetchall()
                if holder_num:
                    # 流通股东人均持股数
                    lt_everyone_hold = self.zgb / lt_count
                    l_time = holder_num[0].get('ENDDATE', 0)
                    sql5 = "select %(columns)s from %(table)s where INNER_CODE = '%(code)s' and ORG_TYPE in (098, 590, 701, 702, 703, 704, 705, 710, 711, 712, 713, 714,715) and ENDDATE = (select max(ENDDATE) from %(table)s where INNER_CODE = '%(code)s' and ENDDATE < '%(l_time)s')" % {"table": table3, "columns": ", ".join(columns), "code": self._inner_code, "l_time": l_time}
                    self.cursor_att.execute(sql5)
                    last_holder_num = self.cursor_att.fetchall()
                    # 上期机构持股数量
                    last_sum = sum([data.get('HLD_TOT_NUM_END', '') for data in last_holder_num if data.get('HLD_TOT_NUM_END', '')])
                    
                    # 获取上期总股本
                    table4 = 'stk_shr_stru'
                    sql4 = "select TOTAL from %(table)s where A_STOCKCODE = '%(code)s' and TOTAL is not null order by CHANGEDATE desc limit 1, 1" % {"table": table4, "code": self.code}
                    self.cursor_att.execute(sql4)
                    last_total = self.cursor_att.fetchone()
                    last_zgb = last_total.get('TOTAL', 0) if last_total else 0
                    last_avg_stk = last_zgb / fl2 if fl2 else 0
                    lt_everyone_hold_change = (lt_everyone_hold - last_avg_stk) / last_avg_stk if last_avg_stk else 0 
                    jg_count = sum([data.get('HLD_ORG_NUM', '') for data in holder_num if data.get('HLD_ORG_NUM', '')])
                    jg_total_hold_change_hb = (jg_count - last_sum) / last_sum if last_sum else 0
                    jg_total_hold = sum([data.get('HLD_TOT_NUM_END', '') for data in holder_num if data.get('HLD_TOT_NUM_END', '')])
                    jg_total_hold_value = sum([data.get('HLD_TCAP_END', '') for data in holder_num if data.get('HLD_TCAP_END', '')])
                    jg_total_hold_ratio = sum([data.get('HLD_TOT_PCT_END', '') for data in holder_num if data.get('HLD_TOT_PCT_END', '')])
                else:
                    lt_everyone_hold = 0
                    lt_everyone_hold_change = 0
                    jg_count = 0
                    jg_total_hold_change_hb = 0
                    jg_total_hold = 0 
                    jg_total_hold_value = 0
                    jg_total_hold_ratio =0
                
                i = [self.code, tr_date, round(lt_count/10000, 4), lt_growth_rate_hb, lt_everyone_hold, round(lt_everyone_hold_change/10000, 4), lt_top10_total_hold, lt_top10_total_hold_ratio, lt_top10_total_hold_change_hb,
                 jg_count, round(jg_total_hold/10000, 4), round(jg_total_hold_value/100000000, 4), jg_total_hold_ratio, jg_total_hold_change_hb,lt_top10_1, round(lt_top10_1_hold/10000, 4), lt_top10_1_hold_ratio,lt_top10_2,round(lt_top10_2_hold/10000, 4),
                 lt_top10_2_hold_ratio,lt_top10_3,round(lt_top10_3_hold/10000, 4),lt_top10_3_hold_ratio,lt_top10_4,round(lt_top10_4_hold/10000, 4),lt_top10_4_hold_ratio,lt_top10_5,round(lt_top10_5_hold/10000, 4),lt_top10_5_hold_ratio,
                 lt_top10_6,round(lt_top10_6_hold/10000, 4),lt_top10_6_hold_ratio,lt_top10_7,round(lt_top10_7_hold/10000, 4),lt_top10_7_hold_ratio,lt_top10_8,round(lt_top10_8_hold/10000, 4),lt_top10_8_hold_ratio,lt_top10_9,
                 round(lt_top10_9_hold/10000, 4),lt_top10_9_hold_ratio,lt_top10_10,round(lt_top10_10_hold/10000, 4),lt_top10_10_hold_ratio]
                TOP_TEN.append(i)
        
        self.cursor.close()
        self.cursor_att.close()
    
    
    def insert_top_ten_info(self, data_list):
        table = 'stock_holder'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`code`','`tr_date`','`lt_count`','`lt_growth_rate_hb`','`lt_everyone_hold`','`lt_everyone_hold_change`','`lt_top10_total_hold`','`lt_top10_total_hold_ratio`','`lt_top10_total_hold_change_hb`',
             '`jg_count`','`jg_total_hold`','`jg_total_hold_value`','`jg_total_hold_ratio`','`jg_total_hold_change_hb`','`lt_top10_1`','`lt_top10_1_hold`','`lt_top10_1_hold_ratio`','`lt_top10_2`','`lt_top10_2_hold`',
             '`lt_top10_2_hold_ratio`','`lt_top10_3`','`lt_top10_3_hold`','`lt_top10_3_hold_ratio`','`lt_top10_4`','`lt_top10_4_hold`','`lt_top10_4_hold_ratio`','`lt_top10_5`','`lt_top10_5_hold`','`lt_top10_5_hold_ratio`',
             '`lt_top10_6`','`lt_top10_6_hold`','`lt_top10_6_hold_ratio`','`lt_top10_7`','`lt_top10_7_hold`','`lt_top10_7_hold_ratio`','`lt_top10_8`','`lt_top10_8_hold`','`lt_top10_8_hold_ratio`','`lt_top10_9`',
             '`lt_top10_9_hold`','`lt_top10_9_hold_ratio`','`lt_top10_10`','`lt_top10_10_hold`','`lt_top10_10_hold_ratio`']
        
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = data_list
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print(' insert_top_ten_info is error %s ' % e)
        else:
            print(' insert_top_ten_info is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
    def get_stock_holder_jg(self):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        item_list = []
        update_list = []
        holders = []
        table3 = 'ana_stk_hold_dtl'
        columns = ['`HLD_TOT_NUM`', '`HLD_MKTCAP`', '`HLD_FL_PCT`', '`ORGCODE`', '`ENDDATE`']
        sql3 = "select %(columns)s from %(table)s where INNER_CODE = '%(code)s' and `ENDDATE` = (select max(ENDDATE) from %(table)s where INNER_CODE = '%(code)s')" % {"table": table3, "columns": ", ".join(columns), "code": self._inner_code, "end": str(self.year) + self.third, "table": table3, "code": self._inner_code}
        self.cursor_att.execute(sql3)
        data_list = self.cursor_att.fetchall()
        if data_list:
            for data in data_list:
                if data:
                    orgcode = data.get('ORGCODE', 0)
                    table = 'org_profile'
                    columns2 = ['`CNAME`', '`TYPE_NAME`']
                    sql = "select %(columns)s from %(table)s where ORGCODE = '%(orgcode)s'" % {"table": table, "columns": ", ".join(columns2), "orgcode": orgcode}
                    self.cursor_att.execute(sql)
                    org_data = self.cursor_att.fetchone()
                    cnt = data.get('HLD_TOT_NUM', 0) if data.get('HLD_TOT_NUM', 0) else 0
                    value = data.get('HLD_MKTCAP', 0) if data.get('HLD_MKTCAP', 0) else 0
                    ratio = data.get('HLD_FL_PCT', 0) if data.get('HLD_FL_PCT', 0) else 0
                    remark = ''.join(str(data.get('ENDDATE', 0)).split(' ')[0].split('-'))
                    if org_data:
                        holder = org_data.get('CNAME', '')
                        type = org_data.get('TYPE_NAME', '')
                    else:
                        holder = ''
                        type = ''
                    last_data = self.check_stock_holder_jd(holder)
                    if last_data:
                        last_time = last_data.get('remark', '0')
                        last_cnt = last_data.get('cnt', 0)
                        if remark > last_time:
                            change = float(last_cnt) - float(cnt)
                            if change == 0:
                                change = '不变'
                            update_list.append([self.code, holder, cnt, value, ratio, change, remark])
                        else:
                            continue 
                    else:
                        if holder not in holders:
                            if type != '自然人':
                                change = '新进'
                                item_list.append([self.code, self.name, holder, type, cnt, value, ratio, change, remark])
                    holders.append(holder)
        self.cursor.close()
        self.cursor_att.close()  
        if item_list:
            row = self.insert_stock_holder_jg_data(item_list)
            print('     insert_stock_holder_jg_data row = %s' % str(row))
        if update_list:
            rows = self.update_stock_holder_jg_data(update_list)
            print('     update_stock_holder_jg_data rows = %s' % str(rows))
    
    
    def update_stock_holder_jg_data(self, update_list):
        table = 'stock_holder_jg'
        self.cursor = conn.cursor()
        rows = 0
        for data in update_list:
            sql = "update %(table)s set `cnt` = '%(cnt)s', `value` = '%(value)s', `ratio` = '%(ratio)s', `change` = '%(change)s', `remark` = '%(remark)s' where `code` = '%(code)s' and `holder` = '%(holder)s'" % {"table": table, "cnt": data[2], "value": data[3], "ratio": data[4], "change": data[5], "remark": data[6], "code": data[0], "holder": data[1]}
            try:
                row = self.cursor.execute(sql)
                rows += row
            except Exception as e:
                print('     update_stock_holder_jg_data is error %s ' % e)
            else:
                print('     update_stock_holder_jg_data is ok ')
                conn.commit()
        self.cursor.close()
        return rows
    
    
    def check_stock_holder_jd(self, holder):
        table = 'stock_holder_jg'
        self.cursor = conn.cursor()
        columns = ['`cnt`', '`remark`']
        sql = "select %(columns)s from %(table)s where code = '%(code)s' and holder='%(holder)s'" % {"table": table, "columns": ", ".join(columns), "code": self.code, "holder":holder}
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        return data
    
    
    def insert_stock_holder_jg_data(self, item_list):
        table = 'stock_holder_jg'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`code`', '`name`', '`holder`', '`type`', '`cnt`', '`value`', '`ratio`', '`change`', '`remark`']
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = item_list
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print('     insert_stock_holder_jg_data is error %s ' % e)
        else:
            print('     insert_stock_holder_jg_data is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
    def get_stock_holder_info(self):
        self.cursor = conn.cursor()
        self.cursor_att = att_conn.cursor()
        data_list = []   
        table = 'stk_cir_holder'
        columns = ['`NAME`', '`HOLD_NUM`', '`HOLD_PCT`', '`ENDDATE`']
        sql = "select %(columns)s from %(table)s where A_STOCKCODE = '%(code)s' order by ENDDATE desc limit 10" % {"table": table, "columns": ", ".join(columns), "code": self.code}
        self.cursor_att.execute(sql)
        holder_data = self.cursor_att.fetchall()
        last_time = self.get_stock_holder_last_time()
        if holder_data:
            enddate = holder_data[0].get('ENDDATE', 0) if holder_data[0] else 0
            end_time = ''.join(str(enddate).split(' ')[0].split('-'))
            if end_time > last_time:
                for data in holder_data:
                    holder = data.get('NAME', '')
                    cnt = data.get('HOLD_NUM', '')
                    ratio = data.get('HOLD_PCT', '')
                    remark = ''.join(str(data.get('ENDDATE', '')).split(' ')[0].split('-'))
                    data_list.append([self.code, self.name, holder, cnt, ratio, remark])
                row = self.delete_stock_holder_info()
                print('    delete_stock_holder_info for %s row = %s' %(self.code, str(row)))
                ins_row = self.insert_stock_holder_info(data_list)
                print('    insert_stock_holder_info for %s row = %s' %(self.code, str(ins_row)))
        self.cursor.close()
        self.cursor_att.close()
    
    
    def get_stock_holder_last_time(self):
        '''获取股东最后修改时间'''
        self.cursor = conn.cursor()
        table = 'stock_holder_info'
        sql = "select `remark` from %(table)s where code = '%(code)s' limit 1" % {"table": table, "code": self.code}
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        if data:
            last_time = data.get('remark', '')
        else:
            last_time = ''
        self.cursor.close()
        return last_time
    
    
    def delete_stock_holder_info(self):
        self.cursor = conn.cursor()
        row = 0
        table = 'stock_holder_info'
        sql = "delete from %(table)s where code = '%(code)s'" % {"table": table, "code": self.code}
        row = self.cursor.execute(sql)
        self.cursor.close()
        return row  
    
    
    def insert_stock_holder_info(self, data_list):
        table = 'stock_holder_info'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`code`', '`name`', '`holder`', '`cnt`', '`ratio`', '`remark`']
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = data_list
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print('     insert_stock_holder_info is error %s ' % e)
        else:
            print('     insert_stock_holder_info is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
    def get_holder_name_data(self):
        table = 'stock_holder_name'
        self.cursor = conn.cursor()
        sql = "select name from %(table)s"  % {"table": table}
        self.cursor.execute(sql)
        name_data = self.cursor.fetchall()
        if name_data:
            name_list = [data.get('name', '') for data in name_data]
        else:
            name_list = []
        return name_list
    
    
    def insert_stock_holder_name(self, data_list):
        '''添加股东名称'''
        table = 'stock_holder_name'
        self.cursor = conn.cursor()
        row = 0
        insert_columns = ['`name`',]
        sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
        params = data_list
        try:
            row = self.cursor.executemany(sql_insert, params)
        except Exception as e:
            print('     insert_stock_holder_info is error %s ' % e)
        else:
            print('     insert_stock_holder_info is ok ')
            conn.commit()
        self.cursor.close()    
        return row
    
    
def get_code_list():
    table = 'stk_code'
    cur = att_conn.cursor()
    sql = "select STOCKCODE from %(table)s where STK_TYPE = 'A股' and STATUS_TYPE_REF = '1' and STOCKCODE > '00000' order by STOCKCODE" % {"table": table}
    cur.execute(sql)
    code_data = cur.fetchall()
    if code_data:
        code_list = [data.get('STOCKCODE', '') for data in code_data]
    else:
        code_list = []
    cur.close()
    return code_list
    
    
if __name__ == "__main__":
    code_list = get_code_list()
    all_num = len(code_list)
    num = 1
    for code in code_list:
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print("======================================%s==================================" % now)
        print("============================  begin to %s code info  %d/%d==========================" % (code, num, all_num))
        print("===========================================================================================")
        
        p = Stock_Finance(2017, code)
        p.get_stock_name_info()
        print(" [ Stock_Finance ::::   start to get_finance_datas]")
        p.get_finance_datas()
        print(" [ Stock_Finance ::::   start to get_finance_basic_data ]")
        p.get_finance_basic_data()
        print(" [ Stock_Finance ::::   start to get_stock_holder_jg]")
        p.get_stock_holder_jg()
        print(" [ Stock_Finance ::::   start to get_stock_holder_info] ")
        p.get_stock_holder_info()
        print(" [ Stock_Finance ::::   start to stock_holder_top_ten]")
        p.stock_holder_top_ten()
        
        if len(HOLDER_NUM) >= 500:
            name_list = p.get_holder_name_data()
            holders = set(HOLDER_NUM)
            difference = [[v] for v in holders if v not in name_list]
            if difference:
                print('insert_stock_holder_name ::::::::::::')
                p.insert_stock_holder_name(difference)
                del HOLDER_NUM[:]
        if len(STOCK_FINANCE) >= 500:
            print('inster_stock_finace_data :::::::::::: ')
            p.inster_stock_finace_data(STOCK_FINANCE)
            del STOCK_FINANCE[:]
        if len(SROCK_FINANCE_BASIC) >= 300:
            print('insert_stock_finance_basic_data ::::::::::::')
            p.insert_stock_finance_basic_data(SROCK_FINANCE_BASIC)
            del SROCK_FINANCE_BASIC[:]
        if len(TOP_TEN) > 300:
            print('insert_top_ten_info ::::::::::::')
            p.insert_top_ten_info(TOP_TEN)
            del TOP_TEN[:]
        num += 1
        print('\n')
    if HOLDER_NUM:
        name_list = p.get_holder_name_data()
        holders = set(HOLDER_NUM)
        difference = [[v] for v in holders if v not in name_list]
        if difference:
            p.insert_stock_holder_name(difference)
    if STOCK_FINANCE:
        p.inster_stock_finace_data(STOCK_FINANCE)
    if SROCK_FINANCE_BASIC:
        p.insert_stock_finance_basic_data(SROCK_FINANCE_BASIC)
    if TOP_TEN:
        p.insert_top_ten_info(TOP_TEN)
    print("[ Stock_Finance ::::   finished]")
    conn.close()
    att_conn.close()
    server.close()
