# -*- coding:utf-8 -*-

"""
该文件爬取基金信息和所有基金的历史数据，存储到数据库中，只需要运行一次即可。

:copyright: (c) 2020 by olixu
:license: MIT, see LICENCE for more details.
"""
import sys
import requests
import re
from lxml import etree
import sqlite3
import time
import json
import argparse
from multiprocessing.dummy import Pool as ThreadPool


# 数据库设置
conn1 = sqlite3.connect("../database1/fundinfo.db", check_same_thread=False)
c1 = conn1.cursor()
conn2 = sqlite3.connect("../database1/fundhistory.db", check_same_thread=False)
c2 = conn2.cursor()
sql1 = []
sql2 = []

# 爬虫的浏览器头
headers = {
    "Accept": '*/*',
    "Accept-Encoding": 'gzip, deflate',
    "Accept-Language": 'zh-CN,zh;q=0.9',
    "Connection": 'keep-alive',
    "Connection": 'keep\-alive',
    "Host": 'fund.eastmoney.com',
    "Referer": 'http://fund.eastmoney.com/data/fundranking.html',
    "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36'
}

# 创建基金基本信息数据库
create_info_table = '''
create table if not exists info(
    code text primary key,
    full_name text,
    fund_url text,
    tpye text,
    publish_date text,
    setup_date_and scale text,
    asset_scale text,
    amount_scale text,
    company text,
    company_url text,
    bank text,
    bank_url text,
    manager text,
    manager_url text,
    profit_situation text,
    management_feerate text,
    trustee_feerate text,
    standard_compared text,
    followed_target text
);
'''

# 创建历史数据库
create_history_table = '''
create table if not exists "{}"(
    date text primary key,
    net_value text,
    accumulative_value text,
    rate_day text,
    rate_recent_week text,
    rate_recent_month text,
    rate_recent_3month text,
    rate_recent_6month text,
    rate_recent_year text,
    rate_recent_2year text,
    rate_recent_3year text,
    rate_from_this_year text,
    rate_from_begin text
);
'''

# 获得所有基金的当日净值
def get_fund_earning_perday(only_code=True):
    print("正在获得所有基金的当日净值")
    codes = []
    response = requests.get(url="http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=dm&st=asc&sd=2019-05-30&ed=2020-05-30&qdii=&tabSubtype=,,,,,&pi=1&pn=10000&dx=1&v=0.2826407199538974", headers=headers)
    response = response.text
    response = re.findall(r"var rankData = (.*?);$", response)[0]
    response = re.findall(r"\[(.*?)]", response)[0]
    fund_list = response.split('"')
    fund_list = [i for i in fund_list if i != ""]
    fund_list = [i for i in fund_list if i != ","]
    for fund in fund_list:
        f = fund.split(",")
        code = f[0]
        item = {
                "date": f[3], 
                "net_value": f[4],
                "accumulative_value": f[5], 
                "rate_day": f[6], 
                "rate_recent_week": f[7], 
                "rate_recent_month": f[8],
                "rate_recent_3month": f[9], 
                "rate_recent_6month": f[10], 
                "rate_recent_year": f[11],
                "rate_recent_2year": f[12], 
                "rate_recent_3year": f[13], 
                "rate_from_this_year": f[14],
                "rate_from_begin": f[15]}
        if only_code==True:
            codes.append(code)
        else:
            try:
                c2.execute("insert or ignore into '{}' values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(code), list(item.values()))
                pass
            except Exception as e:
                print(e)
    return codes

# 获得基金的信息
def fund_info(code):
    print("正在获得获得基金的信息")
    try:
        #c1.execute(create_info_table)
        #sql1.append(create_info_table)
        response = requests.get(url="http://fundf10.eastmoney.com/jbgk_"+code+".html")
        #print(response.text)
        response = etree.HTML(response.text)
        item = {"code": code,
                "full_name": response.xpath("//th[text()='基金全称']/../td[1]/text()"), 
                "fund_url": 'http://fundf10.eastmoney.com/jbgk_{}.html'.format(code.strip()),
                "type": response.xpath("//th[text()='基金类型']/../td[2]/text()"),
                "publish_date": response.xpath("//th[text()='发行日期']/../td[1]/text()"),
                "setup_date_and_scale": response.xpath("//th[text()='成立日期/规模']/../td[2]/text()"),
                "asset_scale": response.xpath("//th[text()='资产规模']/../td[1]/text()"),
                "amount_scale": response.xpath("//th[text()='份额规模']/../td[2]/a/text()"),
                "company": response.xpath("//th[text()='基金管理人']/../td[1]/a/text()"),
                "company_url": response.xpath("//th[text()='基金管理人']/../td[1]/a/@href"),
                "bank": response.xpath("//th[text()='基金托管人']/../td[2]/a/text()"),
                "bank_url": response.xpath("//th[text()='基金托管人']/../td[2]/a/@href"),
                "manager": response.xpath("//th[text()='基金经理人']/../td[1]//a/text()"),
                "manager_url": response.xpath("//th[text()='基金经理人']/../td[1]//a/@href"),
                "profit_situation": response.xpath("//th[text()='基金经理人']/../td[2]/a/text()"),
                "management_feerate": response.xpath("//th[text()='管理费率']/../td[1]/text()"),
                "trustee_feerate": response.xpath("//th[text()='托管费率']/../td[2]/text()"),
                "standard_compared": response.xpath("//th[text()='业绩比较基准']/../td[1]/text()"),
                "followed_target": response.xpath("//th[text()='跟踪标的']/../td[2]/text()")}
        #print(item)
        for i_name in ["full_name", "type", "publish_date", "setup_date_and_scale", "asset_scale", "amount_scale",
                        "company", "company_url", "bank", "bank_url", "manager", "manager_url", "profit_situation",
                        "management_feerate", "trustee_feerate", "standard_compared", "followed_target"]:
            item[i_name] = item[i_name][0] if len(item[i_name]) > 0 else None
        for i in ["company_url", "bank_url", "manager_url"]:
            item[i] = "http:" + item[i]
        #c1.execute("insert or ignore into info values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(item.values()))
        sql1.append("insert or ignore into info values ('{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')".format(*list(item.values())))
        #print(sql1)
        return True
    except Exception as e:
        print(e)
        return False

# 获得基金自成立以来的每日净值
def fund_history(code):
    #while(len(sql2)>100):
        #print("正在sleep")
        #time.sleep(5)
    print("正在获得获得基金自成立以来的每日净值")
    # 创建一个表
    try:
        #c2.execute(create_history_table.format(code))
        sql2.append(create_history_table.format(code))
        url = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183036648984792081185_1575425405289&fundCode="+ code +"&pageIndex=1&pageSize=20000"    # pageSize写2万一次加载完。一个月就写20。
        headers={"Referer": "http://fundf10.eastmoney.com"}
        response = requests.get(url=url, headers=headers)
        response = response.text
        data = re.findall(r'\((.*?)\)$', response)[0]
        data = json.loads(data)
        for i in data.get("Data").get("LSJZList")[::-1]:
            item = {"date": i.get("FSRQ"),
                    "net_value": i.get("DWJZ"),
                    "accumulative_value": i.get("LJJZ"), 
                    "rate_day": i.get("JZZZL"),
                    "rate_recent_week": '', 
                    "rate_recent_month": '',
                    "rate_recent_3month": '', 
                    "rate_recent_6month": '', 
                    "rate_recent_year": '',
                    "rate_recent_2year": '', 
                    "rate_recent_3year": '', 
                    "rate_from_this_year": '',
                    "rate_from_begin": ''}
            #c2.execute("insert or ignore into '{}' values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(code), list(item.values()))
            sql2.append("insert or ignore into '{}' values ('{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}')".format(code, *list(item.values())))
            #print(sql2)
        return True
    except Exception as e:
        print(e)
        return False

def detail(code):
    # 爬取所有基金的详情信息
    status = fund_info(code)
    print("当前的基金代码： ", code)
    while(status==False):
        print("ERROR：抓取基金的基本信息数据时发生错误，sleep 10秒后继续抓取，当前的code是：", code)
        time.sleep(10)
        status = fund_info(code)
    # 爬取所有基金的历史详细数据
    status = fund_history(code)
    while(status==False):
        print("ERROR：抓取历史详细数据时发生错误，sleep 10秒后继续抓取，当前的code是：", code)
        time.sleep(10)
        status = fund_history(code)

# 将sql命令写入sqlite3
def write2sql():
    for sql in sql1:
        c1.execute(sql)
    for sql in sql2:
        c2.execute(sql)

# 将codes中的任务进行分解，以避免资源占用过高的问题
def code_split(codes, n):
    for i in range(0, len(codes), n):
        yield codes[i:i+n]

def get_past_data(thread):
    global sql1, sql2
    print("正在获取基金的历史数据")
    # 获取所有的基金的code
    codes = get_fund_earning_perday()
    c1.execute(create_info_table)
    code = code_split(codes, 100)
    for i in code:
        # 多线程爬取
        print("正在以：", thread, "个线程爬取历史数据")
        pool = ThreadPool(thread)
        pool.map_async(detail, i)

        # 开始数据库插入进程
        #p = Thread(target=write2sql)
        #p.start()

        pool.close()
        pool.join()
        #p.join()
        write2sql()
        sql1 = []
        sql2 = []

    # 关闭数据库连接
    conn1.commit()
    conn1.close()
    conn2.commit()
    conn2.close()

def get_new_data():
    print("正在获取最新的基金数据")
    get_fund_earning_perday(only_code=False)
    conn1.commit()
    conn1.close()
    conn2.commit()
    conn2.close()


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="基金数据采集工具")
    parser.add_argument('-m', '--mode', default="new", help="两种可选的模式：past:采集历史净值，new:采集最新净值，默认为new")
    parser.add_argument('-t', '--thread', default=30, help="爬取历史净值的线程数，默认为10")
    args = parser.parse_args()
    mode = args.mode 
    thread = int(args.thread)
    if mode!= 'past' and mode!='new':
        print("输入参数错误，只接受一个参数，参数可选为past/new，默认为new")
        exit()
    if mode=='past':
        start = time.time()
        get_past_data(thread)
        print("总共耗时：",time.time()-start)
    if mode=='new':
        start = time.time()
        get_new_data()
        print("总共耗时：",time.time()-start)
