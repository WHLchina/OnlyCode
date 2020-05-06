
import MySQLdb as mysql
from MySQLdb import cursors
import re
import time
from sshtunnel import SSHTunnelForwarder  
from _ast import Pass
from pinyin import get_pinyin
from first_pinyin3 import getPinyin

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



def get_max_time(name):
    '''获取上次数据最大时间'''
    table = 'stock_setting_time'
    cursor = conn.cursor()
    sql = "select `max_time` from %(table)s where `name` = '%(name)s'" % {"table": table,  "name":name}
    cursor.execute(sql)
    data = cursor.fetchone()
    if data:
        return data.get('max_time', '')
    else:
        return ''
    cursor.close()    
    

def update_time(time, name):
    '''修改数据最大时间'''
    table = 'stock_setting_time'
    row = ''
    cursor = conn.cursor()
    sql = "update %(table)s set `max_time` = '%(time)s' where name = '%(name)s'" % {"table": table, "time":time, "name": name}
    print(sql)
    try:
        row = cursor.execute(sql)
    except Exception as e:
        print(' update_time for %s is error %s ' % (name,e))
    else:
        print(' update_time for %s is  ok ' % name)
        conn.commit()
    cursor.close()  
    return row
    
    
    
def seach_stock_code(inner_code):
    '''通过inner——code 获取股票code'''
    # 获取股票code
    cursor_att = att_conn.cursor()
    table = 'stk_code'
    insert_columns = ['`STOCKCODE`']
    sql = "select %(columns)s from %(table)s where INNER_CODE = '%(inner_code)s'" % {"table": table, "columns": ", ".join(insert_columns), "inner_code":inner_code}
    cursor_att.execute(sql)
    code_data = cursor_att.fetchone()
    if code_data:
        return code_data.get('STOCKCODE', '')
    else:
        return ''


def stock_name_change():
    '''获取股票简称变更信息'''
    data_list = []
    # time 为上一次导入数据最大的时间 第一次默认为 1900-1-1
    time = get_max_time('stock_name_change')
    
    cursor_att = att_conn.cursor()
    table = 'stk_sname_chng'
    insert_columns = ['`INNER_CODE`', '`CHANGEDATE`', '`STOCKSNAME`', '`CHI_SPEL`', '`STK_ESNAME`', '`CHNG_REASON`', '`DECLAREDATE`', ]
    sql = "select %(columns)s from %(table)s where `DECLAREDATE` > '%(time)s'" % {"table": table, "columns": ", ".join(insert_columns), "time":time}
    cursor_att.execute(sql)
    stock_change_info = cursor_att.fetchall()
    if stock_change_info:
        # 取出本次数据中最大时间
        max_time = max([data.get('DECLAREDATE', 0) for data in stock_change_info])
        for stock_info in stock_change_info:
            # 获取股票信息
            stock_code = seach_stock_code(stock_info.get('INNER_CODE', ''))
            change_time = ''.join(str(stock_info.get('CHANGEDATE', '')).split(' ')[0].split('-'))
            stock_name = stock_info.get('STOCKSNAME', '')
            pinyin_first = stock_info.get('CHI_SPEL', '')
            english_name = stock_info.get('STK_ESNAME', '')
            change_reason = stock_info.get('CHNG_REASON', '')
            declar_date = ''.join(str(stock_info.get('DECLAREDATE', '')).split(' ')[0].split('-'))
            data_list.append([stock_code, change_time ,stock_name ,pinyin_first ,english_name ,change_reason ,declar_date])
            # 大于500的时候储存进数据库
            if len(data_list) >= 500:
                row = insert_stock_change_info(data_list)
                print('insert_stock_change_info row = %s' % row)
                # 执行添加操作
                del data_list[:]
        if data_list:
            #执行添加操作。
            rows = insert_stock_change_info(data_list)
            print('insert_stock_change_info row = %s' % rows)
        # 修改数据最大时间
        if str(max_time) > time:
            row = update_time(max_time, 'stock_name_change')
    else:
        print('stock_name_change nothing ')
    cursor_att.close()
    
    
def insert_stock_change_info(data_list):
    '''添加股票简称变更信息'''
    table = 'stock_name_change'
    cursor = conn.cursor()
    row = ''
    
    insert_columns = ['`code`', '`change_time`', '`stock_name`', '`pinyin_first`', '`english_name`', '`change_reason`', '`declar_date`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = data_list
    try:
        row = cursor.executemany(sql_insert, params)
    except Exception as e:
        print(' insert_stock_change_info is error %s ' % e)
    else:
        print(' insert_stock_change_info is ok ')
        conn.commit()
    cursor.close()    
    return row



def get_new_stock():
    '''获取新股上市'''
    data_list = []
    # 获取今天的日期
    now = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    cursor_att = att_conn.cursor()
    table = 'deri_stk_prc_mfore'
#     insert_columns = ['`STOCKCODE`', '`STOCKSNAME`', '`ISS_PRC`', '`LIST_DATE`', '`TOPEN`', '`TCLOSE`', '`THIGH`', '`TLOW`', '`TVGA`', '`EXCHR`', '`CHNG_PCT`']
    insert_columns = ['`STOCKCODE`', '`STOCKSNAME`']
    sql = "select %(columns)s from %(table)s where LIST_DATE  = '%(time)s' group by `STOCKCODE`" % {"table": table, "columns": ", ".join(insert_columns), "time":now}
    cursor_att.execute(sql)
    new_stock_info = cursor_att.fetchall()
    if new_stock_info:

        for stock_info in new_stock_info:
            # 获取股票code，名字
            stock_code = stock_info.get('STOCKCODE', '')
            stock_name = stock_info.get('STOCKSNAME', '')
            type = 'gg'
            # 获取股票拼音和首拼
            all_pinyin = ''.join(get_pinyin(stock_name)).upper()
            all_first_py = getPinyin(stock_name)
            tag = stock_code + '|' + all_pinyin + '|' + all_first_py + '|' + stock_name
            data_list.append([stock_code,stock_name, type, tag])
            if len(data_list) >= 500:
                row = insert_new_stock_info(data_list)
                print('get_new_stock row = %s' % row)
                # 执行添加操作
                del data_list[:]
        if data_list:
            #执行添加操作。
            rows = insert_new_stock_info(data_list)
            print('get_new_stock row = %s' % rows)
    else:
        print('get_new_stock nothing')
    cursor_att.close()

    

def insert_new_stock_info(data_list):
    '''储存新股上市信息'''
    table = 'stock_code'
    cursor = conn.cursor()
    row = ''
    
    insert_columns = ['`code`', '`name`', '`type`', '`tag`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = data_list
    try:
        row = cursor.executemany(sql_insert, params)
    except Exception as e:
        print(' insert_new_stock_info is error %s ' % e)
    else:
        print(' insert_new_stock_info is ok ')
        conn.commit()
    cursor.close()    
    return row


def run():
    print('start....')
    stock_name_change()
    get_new_stock()
    print('finish....')


if __name__ == "__main__":
    run()
    server.close()
    
    