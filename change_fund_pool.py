import MySQLdb as mysql
from MySQLdb import cursors
import time


config = {
    'HOST': '39.97.179.178',
    'PORT': 3306,
    'USER': 'chatbot',
    'PASSWD': 'XUxs52LSjzFn4i99',
    'DB':'investor',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}


conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                           user=config['USER'], passwd=config['PASSWD'],
                           db=config['DB'], charset=config['CHARSET'],
                           cursorclass=config.get('CURSOR', cursors.DictCursor))

CODE_MAP = {}



def main():
    print("====" * 4)
    today = time.strftime('%Y%m%d')
    print( " today ===   ", today)
    is_trading = get_stock_date_is_trading(today)
    if is_trading:
        
        # 基金
        fund_insert_list = []
        fund_current_data = get_fund_current_data()
        for data in fund_current_data:
            data_id = data.get("id", 0)
            fund_pool_data = get_fund_pool_data(data_id)
            if not fund_pool_data:
                item = [data.get("fund_code", ""), data.get("tr_date", ""), data.get("show_code", ""), data.get("fund_name", ""), data.get("reason", ""), data.get("id", "")]
                fund_insert_list.append(item)
                if len(fund_insert_list) >= 100:
                    row = insert_fund_pool(fund_insert_list)
                    print(" insert_fund_pool row == ", str(row))
                    del fund_insert_list[:]
        if fund_insert_list:
            row = insert_fund_pool(fund_insert_list)
            print(" insert_fund_pool row == ", str(row))
            del fund_insert_list[:]
        
        # 行业分析
        industry_current_list = []
        industry_current_page_data = get_industry_current_page_data()
        for data in industry_current_page_data:
            data_id = data.get("id", 0)
            industry_analyse_data = get_industry_analyse_data(data_id)
            if not industry_analyse_data:
                item = [data.get("name", ""), data.get("tr_date", ""), data.get("reason", ""), data.get("id", "")]
                industry_current_list.append(item)
                if len(industry_current_list) >= 100:
                    row = insert_industry_analyse(industry_current_list)
                    print(" insert_industry_analyse row == ", str(row))
                    del industry_current_list[:]
        if industry_current_list:
            row = insert_industry_analyse(industry_current_list)
            print(" insert_industry_analyse row == ", str(row))
            del industry_current_list[:]
        
        
    print("====" * 4)
    print("\n")
    print("\n")
    print("\n")
    
    
                
def get_stock_date_is_trading(today):
    """今日是否为交易日"""
    table = 'stock2.stock_calendar'
    conn, cursor = get_cursor()
    sql = "SELECT is_trading FROM %(table)s where tr_date = %(today)s" % {"table": table, "today": today}
    cursor.execute(sql)
    rest = cursor.fetchone()
    cursor.close()
    conn.close()
    return int(rest.get("is_trading", 0))



def get_fund_current_data():
    cursor = conn.cursor()
    
    table = 'investor.fund_current_page'
    sql = "select * from %(table)s " % {"table": table}
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


def get_fund_pool_data(data_id):
    cursor = conn.cursor()
    
    table = 'investor.fund_pool'
    sql = "select * from %(table)s where remark = %(data_id)s" % {"table": table, "data_id":data_id}
    cursor.execute(sql)
    data = cursor.fetchone()
    cursor.close()
    return data


def insert_fund_pool(data_list):
    table = 'investor.fund_pool'
    cursor = conn.cursor()
    row = ''
    
    insert_columns = ['`fund_code`', '`tr_date`', '`show_code`', '`fund_name`', '`reason`', '`remark`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = data_list
    try:
        row = cursor.executemany(sql_insert, params)
    except Exception as e:
        print(' insert_fund_pool is error %s ' % e)
    else:
        print(' insert_fund_pool is ok ')
        conn.commit()
    cursor.close()    
    return row


def get_industry_current_page_data():
    cursor = conn.cursor()
    
    table = 'investor.industry_current_page'
    sql = "select * from %(table)s " % {"table": table}
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


def get_industry_analyse_data(data_id):
    cursor = conn.cursor()
    
    table = 'investor.industry_analyse'
    sql = "select * from %(table)s where remark = %(data_id)s" % {"table": table, "data_id":data_id}
    cursor.execute(sql)
    data = cursor.fetchone()
    cursor.close()
    return data


def insert_industry_analyse(data_list):
    table = 'investor.industry_analyse'
    cursor = conn.cursor()
    row = ''
    
    insert_columns = ['`name`', '`tr_date`', '`reason`', '`remark`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = data_list
    try:
        row = cursor.executemany(sql_insert, params)
    except Exception as e:
        print(' insert_industry_analyse is error %s ' % e)
    else:
        print(' insert_industry_analyse is ok ')
        conn.commit()
    cursor.close()    
    return row


if __name__ == "__main__":
    main()
    conn.close()
    