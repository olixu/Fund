import streamlit as st
import pandas as pd
import numpy as np
import os
import time
import funddata
import recommend

st.title('基金推荐和数据分析工具')

# 按钮：推荐最新数据，先查看database目录下是否有历史数据库，没有的话输出：请爬取历史数据
st.header("爬取最新数据")
if st.button('爬取最新数据'):
    if funddata.check_databases():
        st.write("正在生成推荐")
        os.system('python recommend.py')
        st.write("推送已发送")
    else:
        st.write("没有历史数据库，请爬取历史数据")

# 爬取历史数据
st.header("爬取历史数据")
if st.button('爬取历史数据'):
    if funddata.check_databases():
        st.write("有历史数据，无需重新爬取！")
    else:
        st.write("没有历史数据，需要重新爬取！")
        os.system('python funddata.py -m past')

# 强制重新爬取基金历史数据
st.header("强制重新爬取基金历史数据")
if st.button('强制重新爬取基金历史数据'):
    funddata.delete_databases()
    st.write("历史数据库已经删除，正在重新爬取数据")
    start_time = time.time()
    exec_time = funddata.get_past_data(thread=30)
    st.write("数据爬取完成，花费了", time.time()-start_time, 's')