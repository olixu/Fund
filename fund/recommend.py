# -*- coding:utf-8 -*-

"""
该文件通过分析数据库中的数据，生成推荐，并推送到微信

:copyright: (c) 2020 by olixu
:license: MIT, see LICENCE for more details.
"""
# 微信推送code：SCU99784Te024d0b9ba13f4e59deaf0fbc30ed1df5ed0dbc5548c4


import sqlite3
import time
import json
import argparse
import numpy as np
import pandas as pd
import requests
import datetime


# 数据库设置
conn1 = sqlite3.connect("../database/fundinfo.db")
c1 = conn1.cursor()
conn2 = sqlite3.connect("../database/fundhistory.db")
c2 = conn2.cursor()

codes = []
data = []
tables = c2.execute("select tbl_name from sqlite_master")
date_p = str(datetime.datetime.now().date())

for i in tables:
    codes.append(i[0])

# 删除code中重复的部分
codes = sorted(set(codes), key = codes.index)

for code in codes:
    # 先删除所有的历史的最新数据，方便验证
    #outcome = c2.execute("select * from '" + code + "' where date='{}'".format('2020-07-20'))
    outcome = c2.execute("select * from '" + code + "' where date='{}'".format(date_p))
    for _ in outcome:
        x = list(_)
        x.insert(0,code)
        data.append(x)

df = pd.DataFrame(data, columns=['代码', '时期', '净值', '累计净值', '日涨跌幅', '最近一周涨跌幅', '最近一月涨跌幅', '最近三周涨跌幅', '最近六涨跌幅', '最近一年涨跌幅', '最近两年涨跌幅', '最近三年涨跌幅', '今年以来涨跌幅', '成立以来涨跌幅'])
# 将数据转化成浮点数
df[['净值', '累计净值', '日涨跌幅', '最近一周涨跌幅', '最近一月涨跌幅', '最近三周涨跌幅', '最近六涨跌幅', '最近一年涨跌幅', '最近两年涨跌幅', '最近三年涨跌幅', '今年以来涨跌幅', '成立以来涨跌幅']] = df[['净值', '累计净值', '日涨跌幅', '最近一周涨跌幅', '最近一月涨跌幅', '最近三周涨跌幅', '最近六涨跌幅', '最近一年涨跌幅', '最近两年涨跌幅', '最近三年涨跌幅', '今年以来涨跌幅', '成立以来涨跌幅']].apply(pd.to_numeric, errors='ignore')

# 设置一定的策略来挑选合适的基金
# 选择最近一年涨幅大于50且小于1000的
record = df[(df['最近一年涨跌幅']>50) & (df['最近一年涨跌幅']<1000)]
record1 = record[record['最近一周涨跌幅']<-2.0]
print(record1)
record2 = record[record['最近一周涨跌幅']>1.0]
print(record2)

# 关闭数据库连接
conn1.commit()
conn1.close()
conn2.commit()
conn2.close()

# 设置推送
buy_codes = record1['代码']
sell_codes = record2['代码']

buy = '''
## 建议申购的基金

'''

sell = '''
## 建议赎回的基金

'''
for i in buy_codes:
    buy = buy + i + '\n\n'

for i in sell_codes:
    sell = sell + i + '\n\n'

data1 = {
    'text': '今日基金推荐',
    'desp': buy + '\n\n' + sell
}

response = requests.post(url="https://sc.ftqq.com/SCU99784Te024d0b9ba13f4e59deaf0fbc30ed1df5ed0dbc5548c4.send", data=data1)
print(response)
