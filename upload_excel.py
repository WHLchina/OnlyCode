import pymongo
import MySQLdb as mysql
from MySQLdb import cursors
import xlrd
from datetime import datetime
from xlrd import xldate_as_tuple


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
    result_list = []
    workbook = xlrd.open_workbook(filename='./选基历史记录.xlsx')
    name_list = workbook.sheet_names()
    for name in name_list:
        table = workbook.sheet_by_name(name)
        nrows = table.nrows  #行数
        ncole = table.ncols  #列数
        for i in range(1, nrows):
            colnames = table.row_values(i)
            year = str(int(colnames[0]))
            ctype = sheet.cell(i, 1).ctype  # 表格的数据类型
            cell = sheet.cell_value(i, 1)
            if ctype == 2 and cell % 1 == 0:  # 如果是整形
                cell = int(cell)
            elif ctype == 3:
                # 转成datetime对象
                date = datetime(*xldate_as_tuple(cell, 0))
                cell = date.strftime('%Y/%d/%m')
            print(cell)
            date_str = colnames[1]
            date_str = date_str.split("/")
            tr_date = year + date_str[1] if len(date_str[1]) > 1 else "0" + date_str[1] + date_str[2] if len(date_str[2]) > 1 else "0" + date_str[2]
            # if len(date_str) < 4:
            #     date_str = "0" + date_str
            # tr_date = year + date_str
            code = colnames[2]
            fund_name = CODE_MAP.get(code, "")
            if not fund_name:
                code_data = get_fund_info(code)
                fund_name = code_data.get("name", "")
                CODE_MAP[code] = fund_name
            
            reason = colnames[4]
            reason_str = "<p>" + reason + '</p>'
            item = [code, tr_date, code, fund_name, reason_str]
            print(item)
            result_list.append(item)
            if len(result_list) >= 500:
                insert_fund_pool(result_list)
                del result_list[:]
    if result_list:
        insert_fund_pool(result_list)
        del result_list[:]
        
    # 用完记得删除 
    workbook.release_resources()
    del workbook
      

def get_fund_info(code):
    cursor = conn.cursor()
    table = 'investor.fund_code_new'
    sql = "select * from %(table)s where code = '%(code)s'" % {"table": table, "code": code}
    cursor.execute(sql)
    data = cursor.fetchone()
    cursor.close()
    return data
        

def insert_fund_pool(data_list):
    table = 'investor.fund_pool'
    cursor = conn.cursor()
    row = ''
    
    insert_columns = ['`fund_code`', '`tr_date`', '`show_code`', '`fund_name`', '`reason`']
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


if __name__ == "__main__":
    main()
    conn.close()
    