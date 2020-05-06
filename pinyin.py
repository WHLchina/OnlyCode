# coding: utf-8

# version v2.1.20170117
# author ZhangYunfang
# pip install pypinyin


def get_words_first_letter(str, codec="UTF8", is_upper=True):
    """获取词组拼音首字母
    @param str: 词组字符串
    @param codec: 编码
    @param is_upper: 首字母是否大写
    @return: 字符串的拼音首字母"""
    l = []
    for i in range(0, len(str), 3):
        s = get_cn_first_letter(str[i:i + 3], codec)
        if s:
            l.append(s)
    if is_upper:
        return "".join(l).upper()
    else:
        return "".join(l).lower()


def get_cn_first_letter(str, codec="UTF8"):
    """获取字符的中文首字母
    @param str: 中文字符
    @param codec: 编码
    @return: 中文字符的拼音首字母"""
    if codec != "GBK":
        if codec != "unicode":
            str = str.decode(codec)
        str = str.encode("GBK")
    
    if str < "\xb0\xa1" or str > "\xd7\xf9":
        return ""
    if str < "\xb0\xc4":
        return "a"
    if str < "\xb2\xc0":
        return "b"
    if str < "\xb4\xed":
        return "c"
    if str < "\xb6\xe9":
        return "d"
    if str < "\xb7\xa1":
        return "e"
    if str < "\xb8\xc0":
        return "f"
    if str < "\xb9\xfd":
        return "g"
    if str < "\xbb\xf6":
        return "h"
    if str < "\xbf\xa5":
        return "j"
    if str < "\xc0\xab":
        return "k"
    if str < "\xc2\xe7":
        return "l"
    if str < "\xc4\xc2":
        return "m"
    if str < "\xc5\xb5":
        return "n"
    if str < "\xc5\xbd":
        return "o"
    if str < "\xc6\xd9":
        return "p"
    if str < "\xc8\xba":
        return "q"
    if str < "\xc8\xf5":
        return "r"
    if str < "\xcb\xf9":
        return "s"
    if str < "\xcd\xd9":
        return "t"
    if str < "\xce\xf3":
        return "w"
    if str < "\xd1\x88":
        return "x"
    if str < "\xd4\xd0":
        return "y"
    if str < "\xd7\xf9":
        return "z"


def get_pinyin(ustring, shouzimu=False, duoyinzi=False):
    """获取汉字拼音
    @param ustring: 中文字符串，utf-8格式
    @param shouzimu: 首字母还是全拼音，True-首字母，False-全拼
    @param duoyinzi: 是否考虑返回多音字，True-多音字，False-不考虑
    @return: 汉字拼音"""
    from pypinyin import pinyin, lazy_pinyin
    import pypinyin
    if shouzimu:
        return [x[0] for x in pinyin(ustring, style=pypinyin.FIRST_LETTER) if x]
    else:
        if not duoyinzi:
            return lazy_pinyin(ustring)
        else:
            return pinyin(ustring, heteronym=True)


def debug():
    """测试"""
    print(get_words_first_letter("欢迎使用拼音模块").upper())

