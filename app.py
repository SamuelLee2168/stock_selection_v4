import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import streamlit as st

#Finds the largest value that is smaller than the target value. This is useful for finding the most recent date that is a trade day, given a non trade day
def find_largest_smaller_value(my_list, target_value):
    # Initialize a variable to store the largest value found so far
    largest_smaller_value = None

    # Iterate through the list
    for value in my_list:
        # Check if the value is smaller than the target_value and if it is greater than the current largest_smaller_value
        if value < target_value and (largest_smaller_value is None or value > largest_smaller_value):
            largest_smaller_value = value
            
    return largest_smaller_value

def read_splitted_data(folder_path):
    #Large data are stored as multiple csv files so they can be uploaded to github. To use them, we have to read and concatenate them together into 1 df.
    dfs = []
    for file_name in os.listdir(folder_path):
        dfs.append(pd.read_csv(folder_path+file_name))
    if len(dfs)>0:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()

def get_stock_basic():
    return pd.read_csv("data/stock_basic.csv")

def get_company_data_of_date(date):
    #Gets the daily company data of a single given day

    #Since we most likely use data of the current year, we can first only check if the data of given date is in the most recent data, which is stored as the first file in the folder.
    recent_company_data = pd.read_csv("data/daily_company_data/1.csv")
    #Check if the data in the correct date has more than 5300 stocks, in case of the data missing some stocks
    data_of_date = recent_company_data.loc[recent_company_data['trade_date']==date]
    if data_of_date.shape[0]>5300:
        return data_of_date

    #If the data corresponding to the date cannot be found in the first chunk, then search through all data chunks
    company_data = read_splitted_data("data/daily_company_data/")
    most_recent_date = find_largest_smaller_value(set(company_data['trade_date']),date)
    data_of_date = company_data.loc[company_data['trade_date']==most_recent_date]
    return data_of_date

def get_daily_std_data():
    return pd.read_csv("data/daily_std_data.csv")

def get_daily_vol_data():
    return read_splitted_data("data/daily_vol_data/")

def get_dates_for_c1(end_date,days_to_calculate):
    #Finds the start_date for calculations of c1, given the end_date and days_to_calculate
    #Note that some days are non trade days

    unique_trade_days = list(get_daily_std_data().sort_values('trade_date')['trade_date'].unique())
    
    if end_date in unique_trade_days:
        index_of_end_date = unique_trade_days.index(end_date)
        value_of_valid_end_date = end_date
    else:
        value_of_valid_end_date = find_largest_smaller_value(unique_trade_days,end_date)
        if value_of_valid_end_date is None:
            st.error(f"没有找到{end_date}的数据！请注意开始提供数据的日期！")
        index_of_end_date = unique_trade_days.index(value_of_valid_end_date)
    
    start_date = unique_trade_days[index_of_end_date-days_to_calculate+1]
    return start_date,value_of_valid_end_date

def get_avg_std_in_time_period(start,end):
    df = get_daily_std_data()
    df = df.loc[df['trade_date'] >= start]
    df = df.loc[df['trade_date'] <= end]
    df = df[['ts_code','std']].groupby('ts_code').mean().reset_index()
    df['std'] = df['std'].astype(float)
    return df

def calculate_c1(end_date,days_to_calculate):
    start_date,end_date = get_dates_for_c1(end_date,days_to_calculate)
    result_df = get_avg_std_in_time_period(start_date,end_date).sort_values("std",ascending=False).reset_index(drop=True)
    result_df.columns = ['ts_code','c1_rating']
    return result_df

def get_dates_for_c2_or_c3(input_end_date,past_days,recent_days,rating_name):
    #c2 and c3 have 2 periods: "past" and "recent". 
    #This function gets the past_start_date, past_end_date and recent_start_date, given the recent_end_date, and the number of days of each period.
    #Note that some days are not trade days, and the input end date also might not be a trade day

    if rating_name=="c2":
        unique_trade_days = list(get_daily_std_data().sort_values('trade_date')['trade_date'].unique())
    else:
        unique_trade_days = list(get_daily_vol_data().sort_values('trade_date')['trade_date'].unique())
        
    if input_end_date in unique_trade_days:
        index_of_recent_end_date = unique_trade_days.index(input_end_date)
        value_of_valid_end_date = input_end_date
    else:
        value_of_valid_end_date = find_largest_smaller_value(unique_trade_days,input_end_date)
        if value_of_valid_end_date is None:
            st.error(f"没有找到{input_end_date}的数据！请注意开始提供数据的日期！")
        index_of_recent_end_date = unique_trade_days.index(value_of_valid_end_date)
    
    recent_start_date_index = index_of_recent_end_date-recent_days+1
    past_end_date_index = recent_start_date_index-1
    past_start_date_index = past_end_date_index-past_days+1
    recent_start_date = unique_trade_days[recent_start_date_index]
    past_end_date = unique_trade_days[past_end_date_index]
    past_start_date = unique_trade_days[past_start_date_index]
    return past_start_date,past_end_date,recent_start_date,value_of_valid_end_date

def calculate_c2(recent_end_date,past_period_day_count,recent_period_day_count):
    past_start_date,past_end_date,recent_start_date,recent_end_date = get_dates_for_c2_or_c3(recent_end_date,past_period_day_count,recent_period_day_count,rating_name="c2")
    
    recent_std = get_avg_std_in_time_period(recent_start_date,recent_end_date)
    recent_std.columns = ['ts_code','c2_recent_std']
    past_std = get_avg_std_in_time_period(past_start_date,past_end_date)
    past_std.columns = ['ts_code','c2_past_std']
    result_df = recent_std.merge(past_std,on='ts_code',how='inner')
    
    std_changes = []
    for i in np.arange(result_df.shape[0]):
        row = result_df.iloc[i]
        recent_std = row['c2_recent_std']
        past_std = row['c2_past_std']
        if past_std != 0:
            std_changes.append((recent_std-past_std)/past_std*100)
        else:
            std_changes.append(0)
    
    result_df['c2_rating'] = std_changes
    result_df = result_df.sort_values("c2_rating",ascending=False).reset_index(drop=True)
    return result_df

def calculate_avg_vol_in_period(vol_data,start_date,end_date):
    result_df = vol_data.loc[vol_data['trade_date']>=start_date]
    result_df = result_df.loc[result_df['trade_date']<=end_date]
    result_df = result_df[['ts_code','vol']].groupby('ts_code').mean().reset_index()
    result_df['vol'] = result_df['vol'].astype(float)
    return result_df
    
def calculate_c3(recent_end_date,past_period_day_count,recent_period_day_count):
    past_start_date,past_end_date,recent_start_date,recent_end_date = get_dates_for_c2_or_c3(recent_end_date,past_period_day_count,recent_period_day_count,rating_name="c3")
    vol = get_daily_vol_data()

    past_vol = calculate_avg_vol_in_period(vol,past_start_date,past_end_date)
    past_vol.columns = ['ts_code','c3_past_vol']
    recent_vol = calculate_avg_vol_in_period(vol,recent_start_date,recent_end_date)
    recent_vol.columns = ['ts_code','c3_recent_vol']
    
    result_df = past_vol.merge(recent_vol,on='ts_code',how='inner')
    result_df['c3_rating'] = (result_df['c3_recent_vol']-result_df['c3_past_vol'])/result_df['c3_past_vol']*100
    result_df = result_df.sort_values("c3_rating",ascending=False).reset_index(drop=True)
    return result_df

def display_table(df):
    st.dataframe(df)

def improve_index_column(df):
    #Improves the index column of a df for user readability, naming the index column into chinese, and increasing the indexes by 1
    df['排名'] = np.arange(1,df.shape[0]+1)
    df = df.set_index('排名')
    return df

def get_stock_name_given_code(code):
    stock_basic = get_stock_basic()
    selected_df = stock_basic.loc[stock_basic['ts_code']==code].reset_index()
    if selected_df.shape[0] == 0:
        return None
    else:
        return selected_df['name'][0]

def clean_stocks_to_display_input(raw_input):
    #Cleans inputted stock specifications from user, which is 1 string, similar to "000001.SZ,平安银行，000005.SZ"
    #The user might either use the stock code or the stock name (in chinese), as well as using english or chinese commas to separate the stocks

    if raw_input == "":
        return []

    #Split by both english and chinese commans
    splitted_list_stage_1 = raw_input.split(",")
    splitted_list = []
    for i in splitted_list_stage_1:
        result_list = i.split("，")
        splitted_list.extend(result_list)

    #Convert data into a list of stock names
    processed_results = []
    for stock_name_or_code in splitted_list:
        #Checking whether the stock name/code contains a period, is a good way differentiate between stock code and stock name
        if "." in stock_name_or_code:
            stock_name = get_stock_name_given_code(stock_name_or_code)
            if stock_name is None:
                st.error(f"没有找到‘{stock_name_or_code}’对应的股票数据，请确认代码输入正确")
            processed_results.append(stock_name)
        else:
            processed_results.append(stock_name_or_code)

    return processed_results

def get_last_update_date(rating_name):
    if rating_name=="c1" or rating_name=="c2":
        df = get_daily_std_data()
    else:
        df = get_daily_vol_data()
        
    df = df[['ts_code','trade_date']].groupby('ts_code').max()
    value_counts = df['trade_date'].value_counts().reset_index()
    most_common_date = value_counts['trade_date'][0]
    return most_common_date

def add_ranking_column(df,column_to_sort,ranking_column_name):
    result_df = df.sort_values(column_to_sort,ascending=False).reset_index(drop=True)
    result_df = result_df.reset_index().rename(columns={"index":ranking_column_name})
    result_df[ranking_column_name] = result_df[ranking_column_name]+1
    return result_df

def set_special_color_for_columns(df,columns,transparency):
    return df.style.set_properties(**{'background-color': f'rgba(255, 255, 0, {transparency})'}, subset=columns)

def calculate_rating_rankings(recent_end_date,c1_recent_period_day_count,c2_past_period_day_count,c2_recent_period_day_count,c3_past_period_day_count,c3_recent_period_day_count):
    c1_data = calculate_c1(recent_end_date,c1_recent_period_day_count)
    c2_data = calculate_c2(recent_end_date,c2_past_period_day_count,c2_recent_period_day_count)
    c3_data = calculate_c3(recent_end_date,c3_past_period_day_count,c3_recent_period_day_count)
    
    stock_basic = get_stock_basic()
    company_data = get_company_data_of_date(recent_end_date)
    total_data = c1_data.merge(c2_data,on='ts_code',how='inner').merge(c3_data,on='ts_code',how='inner')
    total_data = total_data.merge(stock_basic,on='ts_code',how='left').merge(company_data,on='ts_code',how='left')
    total_data = add_ranking_column(total_data,"c1_rating","c1_ranking")
    total_data = add_ranking_column(total_data,"c2_rating","c2_ranking")
    total_data = add_ranking_column(total_data,"c3_rating","c3_ranking")
    total_data = total_data[['name','list_date','industry','total_mv','c1_ranking','c1_rating','c2_ranking','c2_rating','c3_ranking','c3_rating','c2_past_std','c2_recent_std','c3_past_vol','c3_recent_vol']]
    total_data['total_mv'] = np.round(total_data['total_mv'].astype(float)/10000,2)
    total_data['list_date'] = total_data['list_date'].astype(str)
    total_data['c1_rating'] = np.round(total_data['c1_rating'],4)
    total_data['c2_rating'] = np.round(total_data['c2_rating'],2)
    total_data['c3_rating'] = np.round(total_data['c3_rating'],2)
    total_data['c2_past_std'] = np.round(total_data['c2_past_std'],4)
    total_data['c2_recent_std'] = np.round(total_data['c2_recent_std'],4)
    total_data['c3_past_vol'] = np.round(total_data['c3_past_vol'],2)
    total_data['c3_recent_vol'] = np.round(total_data['c3_recent_vol'],2)
    total_data = total_data.sort_values("c3_rating",ascending=False)
    total_data.columns = ['股票名称','上市日期','行业','总市值(亿元)','C1排名','C1数值(波动性)','C2排名','C2数值(波动性涨跌%)','C3排名','C3数值(交易量涨跌%)','C2以前波动性','C2近期波动性','C3以前交易量','C3近期交易量']
    total_data = total_data.reset_index(drop=True)
    return total_data

#---------------------------------------------
st.title("股票强势系数分析-V4")

st.markdown('<p style="font-size:20px; color:red;"> 数据更新日期：', unsafe_allow_html=True)
c1_last_update_date = get_last_update_date("c1")
st.markdown(f"注：C1数据从20230101开始提供，目前最新数据的日期为{c1_last_update_date}")
c2_last_update_date = get_last_update_date("c2")
st.markdown(f"注：C2数据从20230101开始提供，目前最新数据的日期为{c2_last_update_date}")
c3_last_update_date = get_last_update_date("c3")
st.markdown(f"注：C3数据从20020101开始提供，目前最新数据的日期为{c3_last_update_date}")

st.markdown('<p style="font-size:20px; color:red;"> 调整参数：', unsafe_allow_html=True)
end_date = int(st.text_input("以下输入截止计算日期(如果想要最新的数据，就输入今天的日期。格式是'YYYYMMDD')",value=datetime.now().date().strftime('%Y%m%d'),key=1))
c1_recent_period_day_count = int(st.text_input("C1计算的交易日天数",value=5))
c2_past_period_day_count = int(st.text_input("C2'以前'时间段的交易日天数",value=20,key=9))
c2_recent_period_day_count = int(st.text_input("C2'近期'时间段的交易日天数",value=5,key=10))
c3_past_period_day_count = int(st.text_input("C3'以前'时间段的交易日天数",value=20,key=7))
c3_recent_period_day_count = int(st.text_input("C3'近期'时间段的交易日天数",value=5,key=8))

specific_stocks_to_display_raw = st.text_input("下面可以添加你特别想关注的股票（空白代表不需要添加。可接受股票代码或者名称。比如说“平安银行”或“000001.SZ”。可以同时选择多个股票，注意用逗号分离。）",value="",key=2)
specific_stocks_to_display = clean_stocks_to_display_input(specific_stocks_to_display_raw)

ratings_ranking = calculate_rating_rankings(end_date,c1_recent_period_day_count,c2_past_period_day_count,c2_recent_period_day_count,c3_past_period_day_count,c3_recent_period_day_count)

st.markdown('<p style="font-size:20px; color:red;"> 强势系数排名列表：', unsafe_allow_html=True)
st.markdown("注：点击列名称可切换排列顺序。点击列表右侧的放大按钮可全屏观看列表")
display_table(ratings_ranking)

if len(specific_stocks_to_display)>0:
    st.markdown("以下是您具体关注的股票：")
    specific_data = ratings_ranking.loc[ratings_ranking['股票名称'].isin(specific_stocks_to_display)].sort_values("C3数值(交易量涨跌%)",ascending=False)
    display_table(specific_data)