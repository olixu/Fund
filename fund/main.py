import streamlit as st
import pandas as pd
import numpy as np
import os
import time
import funddata
import recommend


st.title('基金推荐和数据分析工具 :sunglasses:')
st.info('[贡献本项目](https://github.com/olixu/Fund)')

# 按钮：推荐最新数据，先查看database目录下是否有历史数据库，没有的话输出：请爬取历史数据
st.markdown("## **功能1**：爬取最新数据并生成推荐，建议晚上11点至12点运行，等晚上净值更新后才有结果。")
para1 = st.number_input("最近一年涨幅下限：", value=50)
para2 = st.number_input("申购基金条件：最近一周跌幅：",value=-5.0)
para3 = st.number_input("赎回基金条件：最近一周涨幅：", value=5.0)
if st.button('爬取最新数据'):
    if funddata.check_databases():
        st.info("正在生成推荐")
        st.success("推荐已发送到微信")
        buy, sell = recommend.recom(para1, para2, para3)
        st.markdown(buy)
        st.markdown(sell)
    else:
        st.write("没有历史数据库，请爬取历史数据")

# 爬取历史数据
st.markdown("## **功能2**：爬取历史数据")
if st.button('爬取历史数据'):
    if funddata.check_databases():
        st.info("有历史数据，无需重新爬取！")
    else:
        st.info("没有历史数据，需要重新爬取！系统正在爬数据")
        start_time = time.time()
        exec_time = funddata.get_past_data(thread=30)
        st.info("数据爬取完成，花费了"+str(time.time()-start_time)+'s')


# 强制重新爬取基金历史数据
st.markdown("## **功能3**：强制重新爬取基金历史数据")
if st.button('强制重新爬取基金历史数据'):
    st.warning("演示站点停止了该功能，因为重新爬数据需要比较大的资源，其他人就看不了了，如需使用，请取消注释")
    #funddata.delete_databases()
    #st.write("历史数据库已经删除，正在重新爬取数据")
    #start_time = time.time()
    #exec_time = funddata.get_past_data(thread=30)
    #st.info("数据爬取完成，花费了"+str(time.time()-start_time)+'s')

