# -*- coding:utf-8 -*-
import pymongo

import xlrd
import MySQLdb as mysql
from MySQLdb import cursors


config = {
    'HOST': '172.17.175.179',
    'PORT': 3306,
    'USER': 'chatbot',
    'PASSWD': 'XUxs52LSjzFn4i99',
    'DB':'chatbot_seven',
    'THREADED': True,
    'AUTOCOMMIT': True,
    'CHARSET':'utf8',
}


conn = mysql.connect(host=config['HOST'], port=config['PORT'],
                           user=config['USER'], passwd=config['PASSWD'],
                           db=config['DB'], charset=config['CHARSET'],
                           cursorclass=config.get('CURSOR', cursors.DictCursor))



def main():
        mongo_client = pymongo.MongoClient("127.0.0.1", 27017)
        mongo_db = mongo_client['chatbot']
        col = mongo_db['index_data']
        workbook = xlrd.open_workbook('./self_db.xlsx')
        table = workbook.sheet_by_index(0)
        nrows = table.nrows
        ncole = table.ncols
        first_nrow = table.row_values(0)
        second_nrow = table.row_values(1)
        col_list = [first_nrow[i] for i in range(0, ncole)]
        col_info_list = [second_nrow[i] for i in range(0, ncole)]
        
        for i in range(2, nrows):
            colnames = table.row_values(i)
            item = {"divide_id": 1}
            for j in range(0, ncole):
                item[col_list[j]] = str(colnames[j])
            # print("item == ", item)
            rest_id = col.insert(item)
            print(" S001 == ", item.get("S001", ""))
            print("rest_id === ", str(rest_id))
#             index_data = item.get("S001", "")
#             if index_data:
#                 success = judge_text(index_data)
#                 if success:
#                     index_data = format_logic_rule(index_data)
#                     index_data = strQ2B(index_data)
#                     flag = find_logic_sign(index_data)
#                     if not flag:
#                         index_data = '(' + str(index_data) + ')'
#                         index_data = index_data.replace("��", "(").replace("��", ")").replace("((", "(").replace("))", ")")
#                     if isinstance(index_data, int):
#                         index_data = '(' + str(index_data) + ')'
#                     if not index_data.startswith("(") and not index_data.endswith(")"):
#                         index_data = '(' + index_data + ')'
#                     data = {"adapter": "INDEX"}
#                     temp = settings.ROBOT_WORD_SEQUENCE_CODE_KEY
#                     prefix, sql_field = temp[0], temp[1]
#                     sql_field = str(robot_id) + '_' + sql_field
#                     code_data = get_next_code_by_sql('code', prefix, sql_field, code_len=7, conn_name=conn_name, mysql_config=mysql_config)
#                     data["code"] = code_data.get("code", "")
#                     data["word"] = index_data
#                     data["extra"] = str(rest_id)
#                     data["divide_id"] = robot_id
#                     db_create_word_data(conn_name, mysql_config, data)
        
        workbook.release_resources()
        del workbook
    
def update_data():
    sql_str = """update chatbot_seven.word set extra = '5eae35542692c90ddc239129' where adapter = 'index' and theme = 'm1|蟹状星云|金牛座&m1|金牛座&蟹状星云|ngc1952|m1&弥漫星云|蟹状星云&金牛座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912a' where adapter = 'index' and theme = 'm2|宝瓶座&球状星团|宝瓶座&m2|m2&球状星团|ngc7089';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912b' where adapter = 'index' and theme = 'm3|ngc5272|m3&猎犬座|猎犬座&球状星团|m3&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912c' where adapter = 'index' and theme = 'm4|ngc6121|m4&天蝎座|天蝎座&球状星团|m4&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912d' where adapter = 'index' and theme = 'm5|ngc5904|m5&巨蛇座|m5&球状星团|巨蛇座&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912e' where adapter = 'index' and theme = 'm6|ngc6405|m6&疏散星团|m6&天蝎座|疏散星团&天蝎座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23912f' where adapter = 'index' and theme = 'm7|ngc6475|m7&疏散星团|m7&天蝎座|疏散星团&天蝎座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239130' where adapter = 'index' and theme = 'm8|礁湖星云|m8&人马座|ngc6523|m8&礁湖星云|湖礁星云|m8&发射星云|人马座&发射星云';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239131' where adapter = 'index' and theme = 'm9|ngc6333|m9&球状星团|蛇夫座&球状星团|m9&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239132' where adapter = 'index' and theme = 'm10|ngc6254|m10&球状星团|m10&蛇夫座|球状星团&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239133' where adapter = 'index' and theme = 'm11|ngc6705|m11&疏散星团|m11&盾牌座|盾牌座&疏散星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239134' where adapter = 'index' and theme = 'm12|ngc6218|m12&球状星团|m12&蛇夫座|球状星团&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239135' where adapter = 'index' and theme = 'm13|ngc6205|m13&球状星团|m13&武仙座|武仙座&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239136' where adapter = 'index' and theme = 'm14|ngc6402|m14&球状星团|m14&蛇夫座|蛇夫座&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239137' where adapter = 'index' and theme = 'm15|ngc7078|m15&球状星团|m15&飞马座|球状星团&飞马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239138' where adapter = 'index' and theme = '鹰状星云|老鹰星云|m16|ngc6611|m16&发射星云|m16&巨蛇座|发射星云&巨蛇座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239139' where adapter = 'index' and theme = '奥米加星云|奥米伽星云|m17|ngc6618|m17&发射星云|m17&人马座|发射星云&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913a' where adapter = 'index' and theme = 'm18|ngc6613|m18&疏散星团|m18&人马座|疏散星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913b' where adapter = 'index' and theme = 'm19|ngc6273|m19&球状星团|m19&蛇夫座|球状星团&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913c' where adapter = 'index' and theme = '三叶星云|m20|ngc6514|m20&发射星云|m20&人马座|发射星云&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913d' where adapter = 'index' and theme = 'm21|ngc6521|m21&疏散星团|m21&人马座|疏散星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913e' where adapter = 'index' and theme = 'm22|ngc6656|m22&球状星团|m22&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23913f' where adapter = 'index' and theme = 'm23|ngc6494|m23&疏散星团|m23&人马座|疏散星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239140' where adapter = 'index' and theme = 'm24|ngc6603|m24&疏散星团|m24&人马座|疏散星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239141' where adapter = 'index' and theme = 'm25|ic4725|m25&疏散星团|m25&人马座|疏散星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239142' where adapter = 'index' and theme = 'm26|ngc6694|m26&疏散星团|m26&盾牌座|疏散星团&盾牌座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239143' where adapter = 'index' and theme = '哑铃星云|m27|ngc6853|m27&行星状星云|m27&狐狸座|行星状星云&狐狸座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239144' where adapter = 'index' and theme = 'm28|ngc6626|m28&球状星团|m28&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239145' where adapter = 'index' and theme = 'm29|ngc6913|m29&疏散星团|m29&天鹅座|疏散星团&天鹅座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239146' where adapter = 'index' and theme = 'm30|ngc7099|m30&球状星团|m30&摩羯座|球状星团&摩羯座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239147' where adapter = 'index' and theme = '仙女&星系|m31|ngc224|m31&漩涡星系|m31&仙女座|漩涡星系&仙女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239148' where adapter = 'index' and theme = 'm32|ngc221|m32&椭圆星系|m32&仙女座|椭圆星系&仙女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239149' where adapter = 'index' and theme = 'm33|ngc598|m33&三角座|m33&旋涡星系|旋涡星系&三角座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914a' where adapter = 'index' and theme = 'm34|ngc1039|m34&疏散星团|m34&英仙座|疏散星团&英仙座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914b' where adapter = 'index' and theme = 'm35|ngc2168|m35&疏散星团|m35&双子座|疏散星团&双子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914c' where adapter = 'index' and theme = 'm36|ngc1960|m36&疏散星团|m36&御夫座|疏散星团&御夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914d' where adapter = 'index' and theme = 'm37|ngc2099|m37&疏散星团|m37&御夫座|疏散星团&御夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914e' where adapter = 'index' and theme = 'm38|ngc1912|m38&疏散星团|m38&御夫座|疏散星团&御夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23914f' where adapter = 'index' and theme = 'm39|ngc7092|m39&疏散星团|m39&天鹅座|疏散星团&天鹅座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239150' where adapter = 'index' and theme = 'm40|m40&光学双星|m40&大熊座|大熊座&光学双星';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239151' where adapter = 'index' and theme = 'm41|ngc2287|m41&疏散星团|m41&大犬座|疏散星团&大犬座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239152' where adapter = 'index' and theme = '猎户座&星云|m42|ngc1976|m42&发射星云|m42&猎户座|发射星云&猎户座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239153' where adapter = 'index' and theme = 'm43|ngc1982|m43&发射星云|m43&猎户座|发射星云&猎户座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239154' where adapter = 'index' and theme = '蜂巢星团|m44|ngc2632|m44&疏散星团|m44&巨蟹座|疏散星团&巨蟹座|蜂巢星团&巨蟹座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239155' where adapter = 'index' and theme = '昴星团|m45|m45&疏散星团|m45&金牛座|疏散星团&金牛座|金牛座&昴星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239156' where adapter = 'index' and theme = 'm46|ngc2437|m46&疏散星团|m46&船尾座|疏散星团&船尾座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239157' where adapter = 'index' and theme = 'm47|ngc2422|m47&疏散星团|m47&船尾座|疏散星团&船尾座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239158' where adapter = 'index' and theme = 'm48|ngc2548|m48&疏散星团|m48&长蛇座|疏散星团&长蛇座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239159' where adapter = 'index' and theme = 'm49|ngc4472|m49&椭圆星系|m49&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915a' where adapter = 'index' and theme = 'm50|ngc2323|m50&疏散星团|m50&麒麟座|疏散星团&麒麟座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915b' where adapter = 'index' and theme = 'm51|ngc5194|m51&旋涡星系|m51&猎犬座|旋涡星系&猎犬座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915c' where adapter = 'index' and theme = 'm52|ngc7654|m52&疏散星团|m52&仙后座|疏散星团&仙后座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915d' where adapter = 'index' and theme = 'm53|ngc5024|m53&球状星团|m53&后发座|球状星团&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915e' where adapter = 'index' and theme = 'm54|ngc6715|m54&球状星团|m54&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23915f' where adapter = 'index' and theme = 'm55|ngc6809|m55&球状星团|m55&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239160' where adapter = 'index' and theme = 'm56|ngc6779|m56&球状星团|m56&天琴座|球状星团&天琴座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239161' where adapter = 'index' and theme = '环状星云|m57|ngc6720|m57&行星状星云|m57&天琴座|行星状星云&天琴座|环状星云&天琴座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239162' where adapter = 'index' and theme = 'm58|ngc4579|m58&旋涡星系|m58&室女座|旋涡星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239163' where adapter = 'index' and theme = 'm59|ngc4621|m59&椭圆星系|m59&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239164' where adapter = 'index' and theme = 'm60|ngc4649|m60&椭圆星系|m60&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239165' where adapter = 'index' and theme = 'm61|ngc4303|m61&旋涡星系|m61&室女座|旋涡星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239166' where adapter = 'index' and theme = 'm62|ngc6266|m62&球状星团|m62&蛇夫座|球状星团&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239167' where adapter = 'index' and theme = 'm63|ngc5055|m63&旋涡星系|m63&猎犬座|旋涡星系&猎犬座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239168' where adapter = 'index' and theme = 'm64|ngc4826|m64&旋涡星系|m64&后发座|旋涡星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239169' where adapter = 'index' and theme = 'm65|ngc3623|m65&旋涡星系|m65&狮子座|旋涡星系&狮子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916a' where adapter = 'index' and theme = 'm66|ngc3627|m66&旋涡星系|m66&狮子座|旋涡星系&狮子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916b' where adapter = 'index' and theme = 'm67|ngc2682|m67&疏散星团|m67&巨蟹座|疏散星团&巨蟹座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916c' where adapter = 'index' and theme = 'm68|ngc4590|m68&球状星团|m68&长蛇座|球状星团&长蛇座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916d' where adapter = 'index' and theme = 'm69|ngc6637|m69&球状星团|m69&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916e' where adapter = 'index' and theme = 'm70|ngc6681|m70&球状星团|m70&人马座|球状星团&人马座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23916f' where adapter = 'index' and theme = 'm71|ngc6838|m71&球状星团|m71&天箭座|球状星团&天箭座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239170' where adapter = 'index' and theme = 'm72|ngc6981|m72&球状星团|m72&宝瓶座|球状星团&宝瓶座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239171' where adapter = 'index' and theme = 'm73|ngc6994|m73&疏散星团|m73&宝瓶座|疏散星团&宝瓶座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239172' where adapter = 'index' and theme = 'm74|ngc628|m74&旋涡星系|m74&双鱼座|旋涡星系&双鱼座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239173' where adapter = 'index' and theme = '球状星团&人马座|m75|ngc6864|m75&人马座|m75&球状星团';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239174' where adapter = 'index' and theme = 'm76|ngc651|m76&行星状星云|m76&英仙座|行星状星云&英仙座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239175' where adapter = 'index' and theme = 'm77|ngc1068|m77&漩涡星系|m77&鲸鱼座|漩涡星系&鲸鱼座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239176' where adapter = 'index' and theme = 'm78|ngc2068|m78&反射星云|m78&猎户座|反射星云&猎户座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239177' where adapter = 'index' and theme = 'm79|ngc1904|m79&球状星团|m79&天兔座|球状星团&天兔座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239178' where adapter = 'index' and theme = 'm80|ngc6093|m80&球状星团|m80&天蝎座|球状星团&天蝎座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239179' where adapter = 'index' and theme = 'm81|ngc3031|m81&旋涡星系|m81&大熊座|旋涡星系&大熊座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917a' where adapter = 'index' and theme = 'm82|ngc3034|m82&不规则星系|m82&大熊座|不规则星系&大熊座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917b' where adapter = 'index' and theme = 'm83|ngc5236|m83&旋涡星系|m83&长蛇座|旋涡星系&长蛇座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917c' where adapter = 'index' and theme = 'm84|ngc4374|m84&椭圆星系|m84&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917d' where adapter = 'index' and theme = 'm85|ngc4382|m85&椭圆星系|m85&后发座|椭圆星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917e' where adapter = 'index' and theme = 'm86|ngc4406|m86&室女座|m86&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23917f' where adapter = 'index' and theme = 'm87|ngc4486|m87&椭圆星系|m87&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239180' where adapter = 'index' and theme = 'm88|ngc4501|m88&旋涡星系|m88&后发座|旋涡星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239181' where adapter = 'index' and theme = 'm89|ngc4552|m89&椭圆星系|m89&室女座|椭圆星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239182' where adapter = 'index' and theme = 'm90|ngc4569|m90&旋涡星系|m90&室女座|旋涡星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239183' where adapter = 'index' and theme = 'm91|ngc4548|m91&棒旋星系|m91&后发座|棒旋星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239184' where adapter = 'index' and theme = 'm92|ngc6341|m92&球状星团|m92&武仙座|球状星团&武仙座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239185' where adapter = 'index' and theme = 'm93|ngc2447|m93&疏散星团|m93&船尾座|疏散星团&船尾座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239186' where adapter = 'index' and theme = 'm94|ngc4736|m94&旋涡星系|m94&猎犬座|旋涡星系&猎犬座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239187' where adapter = 'index' and theme = 'm95|ngc3351|m95&棒旋星系|m95&狮子座|棒旋星系&狮子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239188' where adapter = 'index' and theme = 'm96|ngc3368|m96&旋涡星系|m96&狮子座|旋涡星系&狮子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239189' where adapter = 'index' and theme = '夜枭星云|m97|ngc3587|m97&行星状星云|m97&大熊座|行星状星云&大熊座|大熊座&夜枭星云';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918a' where adapter = 'index' and theme = 'm98|ngc4192|m98&旋涡星系|m98&后发座|旋涡星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918b' where adapter = 'index' and theme = 'm99|ngc4254|m99&旋涡星系|m99&后发座|旋涡星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918c' where adapter = 'index' and theme = 'm100|ngc4321|m100&旋涡星系|m100&后发座|旋涡星系&后发座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918d' where adapter = 'index' and theme = 'm101|ngc5457|m101&旋涡星系|m101&大熊座|旋涡星系&大熊座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918e' where adapter = 'index' and theme = 'm102|ngc5866|m102&旋涡星系|m102&天龙座|旋涡星系&天龙座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc23918f' where adapter = 'index' and theme = 'm103|ngc581|m103&疏散星团|m103&仙后座|疏散星团&仙后座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239190' where adapter = 'index' and theme = '草帽星系|m104|ngc4594|m104&旋涡星系|m104&室女座|旋涡星系&室女座|草帽星系&室女座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239191' where adapter = 'index' and theme = 'm105|ngc3379|m105&椭圆星系|m105&狮子座|椭圆星系&狮子座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239192' where adapter = 'index' and theme = 'm106|ngc4258|m106&旋涡星系|m106&猎犬座|旋涡星系&猎犬座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239193' where adapter = 'index' and theme = 'm107|ngc6171|m107&球状星团|m107&蛇夫座|球状星团&蛇夫座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239194' where adapter = 'index' and theme = 'm108|ngc3556|m108&旋涡星系|m108&大熊座|旋涡星系&大熊座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239195' where adapter = 'index' and theme = 'm109|ngc3992|m109&棒旋星系|m109&大熊座|棒旋星系&大熊座';
    update chatbot_seven.word set extra = '5eae35542692c90ddc239196' where adapter = 'index' and theme = 'm110|ngc205|m110&椭圆星系|m110&仙女座|椭圆星系&仙女座';
    """
    sql_list = sql_str.split(";")
    cursor = conn.cursor()
    for sql in sql_list:
        cursor.execute(sql)
        conn.commit()
    cursor.close()

if __name__ == "__main__":
    update_data
    # main()
    conn.close()
