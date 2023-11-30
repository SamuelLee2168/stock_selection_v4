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
    result_df = get_avg_std_in_time_period(start_date,end_date).sort_values("std",ascending=False)

    stock_basic = get_stock_basic()
    company_data = get_company_data_of_date(end_date)
        
    result_df = result_df.merge(stock_basic,on='ts_code',how='left').merge(company_data,on='ts_code',how='left')[['name','list_date','industry','total_mv','std']]
    result_df['total_mv'] = np.round(result_df['total_mv']/10000,2)
    result_df['std'] = np.round(result_df['std'],4)
    result_df['list_date'] = result_df['list_date'].astype(str)
    result_df.columns = ['股票名称','上市日期','行业','总市值(亿元)','波动性']
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
    recent_std.columns = ['ts_code','recent_std']
    past_std = get_avg_std_in_time_period(past_start_date,past_end_date)
    past_std.columns = ['ts_code','past_std']
    result_df = recent_std.merge(past_std,on='ts_code',how='inner')
    
    std_changes = []
    for i in np.arange(result_df.shape[0]):
        row = result_df.iloc[i]
        recent_std = row['recent_std']
        past_std = row['past_std']
        if past_std != 0:
            std_changes.append((recent_std-past_std)/past_std*100)
        else:
            std_changes.append(0)
    
    result_df['std_change_pct'] = std_changes
    result_df = result_df.sort_values("std_change_pct",ascending=False)

    stock_basic = get_stock_basic()
    company_data = get_company_data_of_date(recent_end_date)
        
    result_df = result_df.merge(stock_basic,on='ts_code',how='left').merge(company_data,on='ts_code',how='left')[['name','list_date','industry','total_mv','past_std','recent_std','std_change_pct']]
    result_df['total_mv'] = np.round(result_df['total_mv'].astype(float)/10000,2)
    result_df['past_std'] = np.round(result_df['past_std'],4)
    result_df['recent_std'] = np.round(result_df['recent_std'],4)
    result_df['std_change_pct'] = np.round(result_df['std_change_pct'],2)
    result_df['list_date'] = result_df['list_date'].astype(str)
    result_df.columns = ['股票名称','上市日期','行业','总市值(亿元)','以前波动性','近期波动性','波动性涨跌百分比']
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
    past_vol.columns = ['ts_code','past_vol']
    recent_vol = calculate_avg_vol_in_period(vol,recent_start_date,recent_end_date)
    recent_vol.columns = ['ts_code','recent_vol']
    
    result_df = past_vol.merge(recent_vol,on='ts_code',how='inner')
    result_df['vol_pct_change'] = (result_df['recent_vol']-result_df['past_vol'])/result_df['past_vol']*100
    result_df = result_df.sort_values("vol_pct_change",ascending=False)

    stock_basic = get_stock_basic()
    company_data = get_company_data_of_date(recent_end_date)
        
    result_df = result_df.merge(stock_basic,on='ts_code',how='left').merge(company_data,on='ts_code',how='left')[['name','list_date','industry','total_mv','past_vol','recent_vol','vol_pct_change']]
    result_df['total_mv'] = np.round(result_df['total_mv'].astype(float)/10000,2)
    result_df['past_vol'] = np.round(result_df['past_vol'],2)
    result_df['recent_vol'] = np.round(result_df['recent_vol'],2)
    result_df['vol_pct_change'] = np.round(result_df['vol_pct_change'],2)
    result_df['list_date'] = result_df['list_date'].astype(str)
    result_df.columns = ['股票名称','上市日期','行业','总市值(亿元)','以前平均成交量(手)','近期平均交易量(手)','交易量涨跌百分比']
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

#---------------------------------------------
st.title("股票强势系数分析-V4")


with st.expander("强势系数C1"):
    last_update_date = get_last_update_date("c1")
    st.markdown(f"注：C1数据从20230101开始提供，目前最新数据的日期为{last_update_date}")
    end_date = int(st.text_input("以下输入截止计算日期(如果想要最新的数据，就输入今天的日期。格式是'YYYYMMDD')",value=datetime.now().date().strftime('%Y%m%d'),key=1))
    days_to_calculate = int(st.text_input("以下输入想要计算的交易日天数",value=5))
    specific_stocks_to_display_raw = st.text_input("下面可以添加你特别想关注的股票（空白代表不需要添加。可接受股票代码或者名称。比如说“平安银行”或“000001.SZ”。可以同时选择多个股票，注意用逗号分离。）",value="",key=2)
    specific_stocks_to_display = clean_stocks_to_display_input(specific_stocks_to_display_raw)
    c1_output = calculate_c1(end_date,days_to_calculate)
    c1_output = improve_index_column(c1_output)
    st.markdown("以下是波动性最高的股票：")
    display_table(c1_output)
    if len(specific_stocks_to_display)>0:
        st.markdown("以下是您具体关注的股票：")
        specific_data = c1_output.loc[c1_output['股票名称'].isin(specific_stocks_to_display)].sort_values("波动性",ascending=False)
        display_table(specific_data)


with st.expander("强势系数C2"):
    last_update_date = get_last_update_date("c2")
    st.markdown(f"注：C2数据从20230101开始提供，目前最新数据的日期为{last_update_date}")
    end_date = int(st.text_input("以下输入截止计算日期(如果想要最新的数据，就输入今天的日期。格式是'YYYYMMDD')",value=datetime.now().date().strftime('%Y%m%d'),key=3))
    past_period_day_count = int(st.text_input("'以前'时间段的交易日天数",value=20,key=9))
    recent_period_day_count = int(st.text_input("'近期'时间段的交易日天数",value=5,key=10))
    specific_stocks_to_display_raw = st.text_input("下面可以添加你特别想关注的股票（空白代表不需要添加。可接受股票代码或者名称。比如说“平安银行”或“000001.SZ”。可以同时选择多个股票，注意用逗号分离。）",value="",key=4)
    specific_stocks_to_display = clean_stocks_to_display_input(specific_stocks_to_display_raw)
    c2_output = calculate_c2(end_date,past_period_day_count,recent_period_day_count)
    c2_output = improve_index_column(c2_output)
    st.markdown("以下是波动性涨跌最高的股票：")
    display_table(c2_output)
    if len(specific_stocks_to_display)>0:
        st.markdown("以下是您具体关注的股票：")
        specific_data = c2_output.loc[c2_output['股票名称'].isin(specific_stocks_to_display)].sort_values("波动性涨跌百分比",ascending=False)
        display_table(specific_data)

with st.expander("强势系数C3"):
    last_update_date = get_last_update_date("c3")
    st.markdown(f"注：C3数据从20020101开始提供，目前最新数据的日期为{last_update_date}")
    end_date = int(st.text_input("以下输入截止计算日期(如果想要最新的数据，就输入今天的日期。格式是'YYYYMMDD')",value=datetime.now().date().strftime('%Y%m%d'),key=5))
    past_period_day_count = int(st.text_input("'以前'时间段的交易日天数",value=20,key=7))
    recent_period_day_count = int(st.text_input("'近期'时间段的交易日天数",value=5,key=8))
    specific_stocks_to_display_raw = st.text_input("下面可以添加你特别想关注的股票（空白代表不需要添加。可接受股票代码或者名称。比如说“平安银行”或“000001.SZ”。可以同时选择多个股票，注意用逗号分离。）",value="",key=6)
    specific_stocks_to_display = clean_stocks_to_display_input(specific_stocks_to_display_raw)
    c3_output = calculate_c3(end_date,past_period_day_count,recent_period_day_count)
    c3_output = improve_index_column(c3_output)
    st.markdown("以下是交易量涨跌最高的股票：")
    display_table(c3_output)
    if len(specific_stocks_to_display)>0:
        st.markdown("以下是您具体关注的股票：")
        specific_data = c3_output.loc[c3_output['股票名称'].isin(specific_stocks_to_display)].sort_values("交易量涨跌百分比",ascending=False)
        display_table(specific_data)
    
