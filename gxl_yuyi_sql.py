
import MySQLdb as mysql
from MySQLdb import cursors
import re
from chatbot2.short_url import weibo_short_url
from sshtunnel import SSHTunnelForwarder  

config = {
    'HOST': '123.57.24.229',
    'PORT': 3306,
    'USER': 'chatbot',
    'PASSWD': 'yoquant123.',
    'DB':'test_gxl',
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
                       db='yoquant_db',
                       charset='utf8')  
# config_att = {
#     'HOST': '101.201.155.139',
#     'PORT': 3306,
#     'USER': 'root',
#     'PASSWD': 'Yoquant130612',
#     'DB':'yoquant_test',
#     'THREADED': True,
#     'AUTOCOMMIT': True,
#     'CHARSET':'utf8',
# }
conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                           user=config['USER'], passwd=config['PASSWD'],
                           db=config['DB'], charset=config['CHARSET'],
                           cursorclass=config.get('CURSOR', cursors.Cursor))
# 
# att_conn = conn = mysql.connect(host=config_att['HOST'], port=config_att['PORT'],
#                            user=config_att['USER'], passwd=config_att['PASSWD'],
#                            db=config_att['DB'], charset=config_att['CHARSET'],)

cursor = conn.cursor()
cursor_att = att_conn.cursor()

def get_answer_info():
    print('start..')
    table = 't_yuyi_axxx_zt'
    insert_columns = ['`QID`', '`CONTENT`', '`TYPE`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    for temp in res:
        print(temp)
        QID = temp[0]
        CONTENT = temp[1]
        TYPE = temp[2]
        insert_answer_info(QID=QID, CONTENT=CONTENT, TYPE=TYPE)
    print('finish...')
    cursor_att.close()
    cursor.close()
        

def insert_answer_info(QID, CONTENT):
    
    table = 'answer'
    insert_columns = ['`adapter`', '`code`', '`content`', '`type`',]
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
#     if QID.startswith('Z'):
#         adapter = 'Z'
#     else:
#         adapter = 'ZYJZ'
#     adapter = 'ZYMH'
    adapter = 'JZ'
    params = [adapter, QID, str(CONTENT), 'text',]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert answer is error : [%s]' % e)
    else:
        print('insert answer is ok')
        conn.commit()
    
    
def get_word_info():
    print('start..')
    table = 't_yuyi_qxxx'
    insert_columns = ['`QID`', '`CONTENT`', '`DESC`', '`TOPIC`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    temp_dict = {}
    for temp in res:
        temp_dict.setdefault(temp[0], [])
        if not temp_dict.get(temp[0], ''):
            temp_dict.get(temp[0], '').append(temp[1])
            temp_dict.get(temp[0], '').append(temp[2])
            temp_dict.get(temp[0], '').append(temp[3])
        else:
            temp_dict.get(temp[0], '')[0] += '|' + temp[1]
            temp_dict.get(temp[0], '')[1] = temp[2]
            temp_dict.get(temp[0], '')[2] = temp[3]
    for key, val in temp_dict.items():
        insert_word_info(QID=key, val=val)
    print('finish...')
    cursor_att.close()
    cursor.close()
    print('close...')


def insert_word_info(QID, word):
    
    table = 'word'
#     insert_columns = ['`adapter`', '`code`', '`word`', '`remark`', '`theme`',]
    insert_columns = ['`adapter`', '`code`', '`word`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          

    adapter = 'ZYMH'
#     word = val[0]
#     remark = val[1]
#     if not remark:
#         remark = ''
#     theme = val[2]
#     if not theme:
#         theme = ''
#     params = [adapter, QID, word, remark, theme]
    params = [adapter, QID, word]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert word is error : [%s]' % e)
    else:
        print('insert word is ok')
        conn.commit()
    

def get_rule_info():
    print('start..')
    table = 't_yuyi_main_rule'
    insert_columns = ['`TRIGER_COND`', '`COMBINE_COND`', '`RESP`', '`DESC`', '`TYPE`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    for temp in res:
        temp_dict = {}
        if temp[0].startswith('A') or temp[0].startswith('Q'):
            if temp[0].count(':') > 0:
                item = temp[0].split(':')
                temp_dict['ask_range'] = item[0].replace('Q','').replace('A','')
                temp_dict['ask'] = item[1]
            else:
                temp_dict['ask'] = temp[0]
            if temp[1]:
                if temp[1].startswith('A') or temp[1].startswith('Q'):
                    rest = temp[1].split(':')
                    temp_dict['answer_range'] = rest[0].replace('Q','').replace('A','')
                    temp_dict['answer_last'] = rest[1]
            temp_dict['answer'] = temp[2]
            temp_dict['remark'] = temp[3]
            temp_dict['ab'] = temp[4]
            insert_rule_info(temp_dict)
        else:
            continue
    print('finish...')
    cursor_att.close()
    cursor.close()
    print('close...')


def insert_rule_info(temp_dict):
    
    table = 'rule'
    insert_columns = ['`adapter`', '`ask`', '`ask_range`', '`ab`', '`answer_last`', '`answer_range`', '`answer`', '`remark`', ]
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          

    adapter = 'ZYJZ'
    if temp_dict.get('answer',''):
        if temp_dict.get('answer','').count('|') > 0:
            answer_list = temp_dict.get('answer','').split('|')
            for answer in answer_list:
                params = [adapter, temp_dict.get('ask',''), temp_dict.get('ask_range',''), temp_dict.get('ab',''), temp_dict.get('answer_last',''),
                  temp_dict.get('answer_range',''), answer, temp_dict.get('remark','')]
            try:
                cursor.execute(sql_insert, params)
            except Exception as e:
                print('insert is error : [%s]' % e)
            else:
                print('insert is ok')
                conn.commit()
        else:
            params = [adapter, temp_dict.get('ask',''), temp_dict.get('ask_range',''), temp_dict.get('ab',''), temp_dict.get('answer_last',''),
                  temp_dict.get('answer_range',''), temp_dict.get('answer',''), temp_dict.get('remark','')] 
            try:
                cursor.execute(sql_insert, params)
            except Exception as e:
                print('insert is error : [%s]' % e)
            else:
                print('insert is ok')
                conn.commit()
  
mohu_num = 1000000
answer_num = 1000000


def get_mohu_info():
    print('start..')
    table = 't_yuyi_main_rule_mohu'
    insert_columns = ['`TRIGER_COND`', '`COMBINE_COND`', '`RESP`', '`ANSWER_S`', '`STATUS_CODE`']
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    temp_dict = {}
    for temp in res:
        temp_dict.setdefault(temp[2], [])
        if not temp_dict.get(temp[2], ''):
            temp_dict.get(temp[2], '').append(temp[0])
            temp_dict.get(temp[2], '').append(temp[1])
            temp_dict.get(temp[2], '').append(temp[3])
            temp_dict.get(temp[2], '').append(temp[4])
        else:
            temp_dict.get(temp[2], '')[0] += '|' + temp[0]
            temp_dict.get(temp[2], '')[1] = temp[1]
            temp_dict.get(temp[2], '')[2] = temp[3]
            temp_dict.get(temp[2], '')[2] = temp[4]
    for key, val in temp_dict.items():
        ask_list = val[0].split('|')
        
        if key.startswith('ZT'):
                answer_code = key
        else:
            global answer_num
            answer_num += 1
            answer_code = ('MHA' + str(answer_num)).replace('MHA1', 'MHA')
            insert_answer_info(answer_code, key)
            
        for item in ask_list:
            answer_last = val[1]
            post = val[2]
            remark = val[3]
            global mohu_num
            mohu_num += 1
            mohu_code = ('MHW' + str(mohu_num)).replace('MHW1', 'MHW')
            
            insert_word_info(mohu_code, item)
            insert_mohu_info(mohu_code, answer_last, answer_code, post, remark)

    print('finish...')
    cursor_att.close()
    cursor.close()
    print('close...')


def insert_mohu_info(mohu_code, answer_last, answer_code, post, remark):
    
    table = 'rule'
    insert_columns = ['`adapter`', '`ask`', '`answer_last`', '`answer`', '`post`', '`remark`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          

    adapter = 'ZYMH'
    if not post:
        post = ''
    params = [adapter, mohu_code, answer_last, answer_code, post, remark]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert rule is error : [%s]' % e)
    else:
        print('insert rule is ok')
        conn.commit()
    
    
def get_jz_categroy_info():
    print('start..')
    table = 't_matrix_category'
    insert_columns = ['`code`', '`name`', '`tag`', '`sort`']
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    for temp in res:
        code = temp[0]
        name = temp[1]
        tag = temp[2]
        sort = temp[3]
        insert_jz_categroy_info(code=code, name=name, remark=tag, sort=sort)
    print('finish...')
    cursor_att.close()
    cursor.close()

        

def insert_jz_categroy_info(code, name, remark, sort):
    
    table = 'matrix_category'
    insert_columns = ['`code`', '`name`', '`remark`', '`sort`']
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = [code, name, remark, sort]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert matrix_category is error : [%s]' % e)
    else:
        print('insert matrix_category is ok')
        conn.commit()
          
          
def get_matrix_xyz_info():
    print('start..')
    table = 't_matrix_xy'
    insert_columns = ['`code`', '`name`', '`type`', '`category`', '`sort`', '`tag`', '`list_id`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    for temp in res:
        code = temp[0]
        name = temp[1]
        type = temp[2]
        category = temp[3]
        sort = temp[4]
        tag = temp[5]
        list_id = temp[6]
        insert_matrix_xyz_info(code=code, name=name, type=type, category=category, sort=sort,  content=tag, remark=list_id)
    print('finish...')
    cursor_att.close()
    cursor.close()


def insert_matrix_xyz_info(code, name, type, category, sort, content, remark):
    
    table = 'matrix_xyz'
    insert_columns = ['`code`', '`name`', '`type`', '`category`', '`sort`', '`content`', '`remark`', ]
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = [code, name, type, category, sort, content, remark]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert matrix_xyz is error : [%s]' % e)
    else:
        print('insert matrix_xyz is ok')
        conn.commit()
    
        
def get_matrix_zxx_info():
    print('start..')
    table = 't_yuyi_zxxx'
    insert_columns = ['`QID`', '`CONTENT`', '`TOPIC`', '`LIST_ID`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    temp_dict = {}
    for temp in res:
        if not temp[1]:
            continue
        temp_dict.setdefault(temp[0], [])
        if not temp_dict.get(temp[0], ''):
            temp_dict.get(temp[0], '').append(temp[1])
            temp_dict.get(temp[0], '').append(temp[2])
            temp_dict.get(temp[0], '').append(temp[3])
        else:
            temp_dict.get(temp[0], '')[0] += '|' + temp[1]
            temp_dict.get(temp[0], '')[1] = temp[2]
            temp_dict.get(temp[0], '')[2] = temp[3]
    for key,val in temp_dict.items():
        code = key
        content = val[0]
        name = val[1]
        remark = val[2]
        insert_matrix_xyz_info(code=code, name=name, type='z', category='', sort='',  content=content, remark=remark)
    print('finish...')
    cursor_att.close()
    cursor.close()
    print ('close....')
    return None
    
answer_num = 1000000
def get_matrix_content_info():
    print('start..')
    table = 't_matrix_content'
    insert_columns = ['`x`', '`y`', '`content_wu`', '`category`', '`list_id`',]
    s_sql = "select %(columns)s from %(table)s" % {"table": table, "columns": ", ".join(insert_columns)}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchall()
    print('miding..')
    for temp in res:
        x = temp[0]
        y = temp[1]
        content = temp[2]
        global answer_num
        answer_num += 1
        answer_code = ('JZA' + str(answer_num)).replace('JZA1', 'JZA')
        insert_answer_info(answer_code, content)
        category = temp[3]
        list_id = temp[4]
        z_code_data = get_z_code(category)
        if z_code_data:
            z = z_code_data[0]
        else:
            z = ''
        tag_data = get_tag_data(list_id)
        if tag_data:
            tag = tag_data[0]
        else:
            tag = ''
        insert_matrix_content_info(x=x, y=y, z=z, category=category, content=answer_code,  tag=tag,)
    print('finish...')
    cursor_att.close()
    cursor.close()



def get_z_code(category):
    table = 't_matrix_category'
    insert_columns = ['`tag`',]
    s_sql = "select %(columns)s from %(table)s where `code` = '%(code)s'" % {"table": table, "columns": ", ".join(insert_columns), "code":category}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchone()
    return res
    
    
def get_tag_data(list_id):
    table = 't_matrix_list'
    insert_columns = ['`name`',]
    s_sql = "select %(columns)s from %(table)s where `id` = %(list_id)s" % {"table": table, "columns": ", ".join(insert_columns), "list_id":list_id}
    cursor_att.execute(s_sql)
    res = cursor_att.fetchone()
    return res


def insert_matrix_content_info(x, y, z, category, content, tag):
    
    table = 'matrix_content'
    insert_columns = ['`x`', '`y`', '`z`', '`category`', '`content`', '`tag`',]
    sql_insert = "insert into %(table)s (%(columns)s) values(%(values)s)" % {"table": table, "columns": ", ".join(insert_columns), "values": ", ".join(["%s" for i in range(0, len(insert_columns))])}          
    params = [x, y, z, category, content, tag]
    try:
        cursor.execute(sql_insert, params)
    except Exception as e:
        print('insert matrix_xyz is error : [%s]' % e)
    else:
        print('insert matrix_xyz is ok')
        conn.commit()
        
        
if __name__ == "__main__":
    get_matrix_content_info()
    
