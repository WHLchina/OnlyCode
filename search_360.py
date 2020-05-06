
import datetime
import json as json_util
import random
import re
import requests
import urllib

from copy import deepcopy
from html.parser import HTMLParser
from pyquery.pyquery import PyQuery as pq
from scrapy.selector import Selector

user_agent_list = [
    
    # TOP1、搜狗浏览器 版本 6.2.5.21202
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36 SE 2.X MetaSr 1.0",
    # TOP2、QQ浏览器 版本 9.4.7658 Chromium47.0.2526.80/ IE11.0.9600.18230
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 Core/1.47.277.400 QQBrowser/9.4.7658.400",
    # TOP3、UC浏览器 版本 5.6.12150.8 极速内核版本 48.0.2564.116 兼容内核版本 11.0.9600.18231
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 UBrowser/5.6.12150.8 Safari/537.36",
    # TOP4、百度浏览器 版本 7.6.100.2089 内核版本 Chrome 42.0.2311.135 & IE 11.0.9600.18231
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 BIDUBrowser/7.6 Safari/537.36",
    # TOP5、猎豹浏览器 版本 5.3.108.10912
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.154 Safari/537.36 LBBROWSER",
    # TOP6、IE浏览器 版本 11.0.9600.18230
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
    # TOP7、Chrome浏览器 版本  56.0.2924.87
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
    # TOP8、360浏览器 版本 8.1.1.208 内核 45.0.2454.101
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
    # TOP9、遨游浏览器 版本 4.9.2.1000
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.9.2.1000 Chrome/39.0.2146.0 Safari/537.36",
    # TOP10、世界之窗浏览器 版本 版本 6.2.0.128 内核版本 31.0.1650.63
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36 TheWorld 6",
    # TOP11、火狐浏览器 版本 46.0.1
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
    # TOP12、Chromium浏览器 版本 36.0.1979.0 (268678)
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1979.0 Safari/537.36",
    # TOP13、Opera浏览 版本 37.0.2178.43
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36 OPR/37.0.2178.43",
    # TOP14、2345王牌浏览器 版本 7.1.0.12550 IE内核  11.0.9600.18230|chrome内核  47.0.2526.108
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.108 Safari/537.36 2345Explorer/7.1.0.12550",
    # TOP15、天天浏览器 版本 1.0.1.2
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; TianTian 1.0)",
    # TOP16、七星浏览器 版本 1.42.9.385 极速内核版本 42.0.2311.152
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36",
    # TOP17、糖果浏览器 版本 2.64 (build 0428/0094) IE版本 11.0.9600.18230
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0; Tangguo2.64) like Gecko",
    # TOP18、超速浏览器 版本 5.3 20140118.001
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0; ChaoSu5.3) like Gecko",
    # TOP19、闪游浏览器 版本 3.39
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0; SaaYaa) like Gecko",
    # TOP20、GreenBrowser 版本 6.8.1228
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; GreenBrowser)",
    # TOP21、千寻浏览器 版本 1.0.300.869
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0; QianXun 1.0.3) like Gecko",
    # TOP22、115浏览器 版本 7.0.0.37
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36 115Browser/7.0.0",
    # TOP23、Lunascape 版本 6.13.0.27542 IE 11.0.9600.18230
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; rv:11.0; Lunascape 6.13.0.27542) like Gecko",
    # TOP24、黑客浏览器(TouchNet Browser) 版本 1.30
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; TouchNet Browser 1.30)",
    # TOP25、松果游戏浏览器 版本 1.80正式
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; SGuo 1.80)",
    # TOP26、枫树极速浏览器 版本 2.0.9.20
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36 CoolNovo/2.0.9.20",
    # TOP27、云游浏览器 版本 5.0.37.5
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36",
    # TOP28、云浏览器 版本 1.0.0
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; YunBrowser 1.0)",
    # TOP30、瑞影浏览器 版本 3.3.0.0
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; RuiYing 3.3)",
    # TOP31、智慧云浏览器 版本 3.0
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; ZhiHuiYun 3.0)",
    # TOP32、天行浏览器 版本 3.0.0820.0 内核版本 32.0.1653.0
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1653.0 Safari/537.36",
    # TOP33、Apple Safari 版本 5.34.57.2
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
    # 网络流行浏览器 UserAgent
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    # PC端
    # iMac27 Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36",
    # iMac27 Firefox
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:46.0) Gecko/20100101 Firefox/46.0",
    # iMac27 Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/601.5.17 (KHTML, like Gecko) Version/9.1 Safari/601.5.17",
    # Ubunt Kylin 16.04 X64 Chromium 49.0.2623.108
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/49.0.2623.108 Chrome/49.0.2623.108 Safari/537.36",
    # Ubunt Kylin 16.04 X64 Firefox 46.0
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0",
    
    # safari 5.1 – MAC
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    # safari 5.1 – Windows
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    # IE 9.0
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    # IE 8.0
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    # IE 7.0
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    # IE 6.0
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    # Firefox 4.0.1 – MAC
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    # Firefox 4.0.1 – Windows
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    # Opera 11.11 – MAC
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    # Opera 11.11 – Windows
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    # Chrome 17.0 – MAC
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    # 傲游（Maxthon）
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    # 腾讯TT
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    # 世界之窗（The World） 2.x
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    # 世界之窗（The World） 3.x
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    # 搜狗浏览器 1.x
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    # 360浏览器
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    # Avant
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    # Green Browser
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    # 移动设备
    # iPhone 6 Plus QQ浏览器
    "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13E238 QQ/6.3.3.432 V1_IPH_SQ_6.3.3_1_APP_A Pixel/1080 Core/UIWebView NetType/WIFI Mem/198",
    # iPhone 6 Plus Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13E238 Safari/601.1",
    # iPhone 6 Plus Chrome
    "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_1 like Mac OS X) AppleWebKit/601.1 (KHTML, like Gecko) CriOS/50.0.2661.95 Mobile/13E238 Safari/601.1.46",
    # iPhone 6 puls iOS8.4
    "Mozilla/5.0 (iPhone; CPU iPhone OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12H143 QQ/6.3.1.402 V1_IPH_SQ_6.3.1_1_APP_A Pixel/1080 Core/UIWebView NetType/4G Mem/98",
    # iPad 4 QQ浏览器
    "Mozilla/5.0 (iPad; CPU OS 8_1_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12B440 IPadQQ/6.0.0.244 QQ/6.0.0.244",
    # iPad 4 Chrome
    "Mozilla/5.0 (iPad; CPU OS 8_1_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) CriOS/47.0.2526.107 Mobile/12B440 Safari/600.1.4",
    # iPad 4 Safari
    "Mozilla/5.0 (iPad; CPU OS 8_1_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B440 Safari/600.1.4",
    # Nexus 7 Chrome|浏览器
    "Mozilla/5.0 (Linux; Android 5.0.2; Nexus 7 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.89 Safari/537.36",
    # Desire Z Chrome|浏览器
    "Mozilla/5.0 (Linux; Android 4.2.2; HTC Vision Build/JDQ39E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.89 Mobile Safari/537.36",
    # safari iOS 4.33 – iPhone
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # safari iOS 4.33 – iPod Touch
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # safari iOS 4.33 – iPad
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # Android N1
    "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    # Android QQ浏览器 For android
    "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    # Android Opera Mobile
    "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
    # Android Pad Moto Xoom
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    # BlackBerry
    "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
    # WebOS HP Touchpad
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
    # Nokia N97
    "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
    # Windows Phone Mango
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
    # UC无
    "UCWEB7.0.2.37/28/999",
    # UC标准
    "NOKIA5700/ UCWEB7.0.2.37/28/999",
    # UCOpenwave
    "Openwave/ UCWEB7.0.2.37/28/999",
    # UC Opera
    "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
]


class SoSearchCrawlSpider(object):
    '''360搜索'''
    
    # 重试次数
    TRY_TIME = 20
    # 数据类型 -- 文本
    DATA_TYPE = 'text'
    # 搜索基础Url
    BASIC_URL = 'https://www.so.com/s?ie=utf-8&&q=%(keyword)s'
    # 初始化数据结果格式
    result = {
        'title': '',
        'url': '',
        'content': '',
    }
    
    def __init__(self):
        """初始化"""
        
        # 随机User-Agent
        user_agent = random.choice(user_agent_list)
        self.headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Cache-Control': 'max-age=0',
            'User-Agent': user_agent,
            'Connection': 'keep-alive'
        }
    
    def start_requests(self, keywords='', *args, **kwargs):
        """发起请求
        @param keywords: 搜索关键词
        @return: 搜索结果，字典：{'title': '', 'url': '', 'content': '' }"""
        if keywords:
            # 刷新随机User-Agent
            headers = deepcopy(self.headers)
            user_agent = random.choice(user_agent_list)
            headers.update({'User-Agent': user_agent, })
            session = requests.Session()
            session.headers.update(headers)
            status_code = 0
            renderedBody = ''
            times = 1
            while status_code != 200 and times <= self.TRY_TIME:
                try:
                    url = self.BASIC_URL % {'keyword': keywords.replace(' ', '+'), }
                    response = session.get(url=url, headers=headers, timeout=30)
                    status_code = response.status_code
                    if type(response.content) == bytes:
                        renderedBody = response.content.decode(encoding=response.encoding)
                    else:
                        renderedBody = str(response.content, encoding=response.encoding)
                except Exception as e:
                    print('[SoSearchCrawlSpider :: start_requests] parse content error : ', e)
                
                result = self.parse_item(response=renderedBody)
                times += 1
        else:
            result = deepcopy(self.result)
        return result
    
    
    def parse_item(self, response):
        """解析数据
        @param response: Http响应
        @return: 搜索结果 """
        result = deepcopy(self.result)
        # 标题
        title = ''
        # 内容
        content = ''
        # 链接
        url = ''
        # 信息来源
        source = ''
        # 默认信息来源
        default_source = u'360搜索'
        
        selector = Selector(text=response)
        
        # 定位所有的元素块
        all_divs_xpath = '//div[@id="main"]//ul[@class="result"]/li'
        all_divs = selector.xpath(all_divs_xpath)
        times = 0
        
        if all_divs:
            
            # 随机产生一个数据元素，并对它进行解析
            while (not title or not content or not url) and times <= self.TRY_TIME:
                
                index = random.randrange(0, len(all_divs))
                element_div = all_divs[index]
                
                url_xpath = './/h3[@class="res-title "]//@href'
                url = all_divs[index].xpath(url_xpath).extract() and all_divs[index].xpath(url_xpath).extract()[0] or ''
                
                if not url:
                    url_xpath = './/h3[@class="title"]//@href'
                    url = all_divs[index].xpath(url_xpath).extract() and all_divs[index].xpath(url_xpath).extract()[0] or ''
                
                if not url:
                    url_xpath = './/h3[@class="res-title"]//@href'
                    url = all_divs[index].xpath(url_xpath).extract() and all_divs[index].xpath(url_xpath).extract()[0] or ''
                
                title_xpath = './/h3[@class="res-title "]//text()'
                title = element_div.xpath(title_xpath).extract()
                
                if not title:
                    title_xpath = './/h3[@class="title"]//text()'
                    title = element_div.xpath(title_xpath).extract()
                
                if not title:
                    title_xpath = './/h3[@class="res-title"]//text()'
                    title = element_div.xpath(title_xpath).extract()
                
                html = element_div.extract()
                p = pq(html)
                p('p').filter('.res-linkinfo').remove()
                element_div = Selector(text=p.html())
                
                content_xpath = './/div[@class="best-ans"]/text()'
                content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/p[@class="res-desc"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/div[@class="mh-cont mohe-wrap"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/div[@class="res-comm-con"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/table[@class="mh-detail-table"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/div[@class="intro"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                if not content:
                    content_xpath = './/span[@class="so-ask-best"]//text()'
                    content = element_div.xpath(content_xpath).extract()
                
                times += 1
            
            # 解析到数据进行清洗
            if title:
                title = ''.join(title)
                title = title.replace('  ', ' ').replace('\t', '').replace('\r', ' ').replace('\n', '').strip()
            
            if content:
                content = ''.join(content)
                content = content.replace('  ', ' ').replace('\t', '').replace('\r', ' ').replace('\n', '').strip()
            
            new_url = ''.join(re.findall('link\?url=(.*)&q=', url))
            if new_url:
                url = new_url
            
            result['title'] = title
            result['url'] = url
            result['content'] = content and content or "抱歉，360搜索没有搜到相关结果"
            
            return result
        
        else:
            content_xpath = '//div[@id="no-result"]/p[@class="tips"]/text()'
            keywords_xpath = '//div[@id="no-result"]/p[@class="tips"]/em/text()'
            content_ele = selector.xpath(content_xpath).extract()
            keywords = selector.xpath(keywords_xpath).extract() and selector.xpath(keywords_xpath).extract()[0] or ''
            if keywords and content_ele and len(content_ele) > 1:
                content_list = content_ele[0].split('，')
                content_start = content_list[0] + '，360搜索' + content_list[1]
                content = content_start + keywords + content_ele[1]
            else:
                content = '抱歉，360搜索没有找到相关的结果。'
            url = ''
            title = ''
            post_time = ''
            result['content'] = content
            # result['all_content'] = content
            
            return result



def get_parameter(**kwargs):
    """技能通用方法
    @param kwargs: 关键词参数dict
    @return: {变量名: 变量值, ...}"""
    pass
    return {}



def get_widget(**kwargs):
    """技能通用方法
    @param kwargs: 关键词参数dict
    @return: {}"""
    ask = kwargs.get("ask", "")
    so_search = SoSearchCrawlSpider()
    result = so_search.start_requests(keywords=ask, **kwargs)
    return result
    
    
if __name__ == '__main__':
    
    ask = '搜一搜北京'
    kwargs = {"ask": ask}
    data = get_parameter(ask=ask)
    kwargs.update(data)
    result = get_widget(**kwargs)
    print(result)
    
