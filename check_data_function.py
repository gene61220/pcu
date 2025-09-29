# -*- coding: utf-8 -*-
from oracle_db_tool import  get_oracle_data_count_by_snap_date,  get_oracle_data_sum_by_snapdate
import datetime
import datadate as datadate
import csv
import re
from datetime import datetime, timedelta
import sqlserver_db_function
    # """
    # 先讀取檔案csv
    # 確認有哪些資料表要檢核(CHECKTYPE = today_type)
    # 檢核今天筆數(取得筆數)
    # 檢核昨天(前一次有資料的)筆數
    # 檢核一(有筆數) ，二(跟昨天有差異)，三(檢核特定欄位值)
    # 將資料存入資料庫
    # """
def check_datetype(date8):
    if not re.fullmatch(r'\d{8}', date8):
        raise ValueError("日期格式錯誤，應為 8 碼數字（yyyyMMdd）")
    date_obj = datetime.strptime(date8, "%Y%m%d")
    if date_obj.day == 1:
        return 'F'  # 月初
    next_day = date_obj + timedelta(days=1)
    if next_day.day == 1:
        return 'E'  # 月底
    return 'D'

def check_oracle_data(script_file, unformatted_date8 ,round):
    round_id = datetime.now().strftime('%Y%m%d%H%M%S')
    script_file = 'D:/PRG/PCU/script/'+script_file
    if not re.fullmatch(r'\d{8}',  unformatted_date8):
        raise ValueError(f"日期格式錯誤，應為 8 碼數字：{date8}")
    today_type = check_datetype(unformatted_date8)
    date8 = f"{ unformatted_date8[:4]}-{ unformatted_date8[4:6]}-{ unformatted_date8[6:]}"
    success = 0
    fail = 0
    with open(script_file, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            table_name = row['TABLE_NAME']
            region_type = row['REGION']
            today_local_datadate = datadate.get_datadate(date8,region_type)
            yesterday_local_date = datadate.get_yesterday_datadate(date8,region_type) 
            if row['FREQUENCY'] == "D" or row['FREQUENCY'] == today_type:
                # if row['FREQUENCY'] == today_type and today_type == 'E':
                #趕著上線，這個下次優化再加，沒有GIT只能用備註當筆記了，真難過
                try:
                    check1 = int(row['CHECK1'].strip())
                except (ValueError, TypeError):
                    check1 = 3
                try:
                    check2 = int(row['CHECK2'].strip())
                except (ValueError, TypeError):
                    check2 = 3  
                try:
                    check3 = int(row['CHECK3'].strip())
                except (ValueError, TypeError):
                    check3 = 3
                try:
                    this_row_round = int(row['ROUND'].strip())
                    # print(this_round)
                except:
                    this_row_round = 9999
            #初始化
                check1_result = 3
                check2_result = 3
                check3_result = 3
                today_count = get_oracle_data_count_by_snap_date(table_name,today_local_datadate)
                if yesterday_local_date is None:
                    yesterday_count = 0
                    yesterday_sum = 0
                else:
                    yesterday_count = get_oracle_data_count_by_snap_date(table_name, yesterday_local_date)
                today_sum=None
                yesterday_sum=None
                fail_check = 0
                if check1 == 1 :
                    if today_count > 0:
                        check1_result = 1
                    if today_count <= 0 :
                        if this_row_round < round:
                            check1_result = 0
                            sqlserver_db_function.write_in_not_this_round_check(table_name,today_local_datadate,round_id)
                        check1_result = 0
                        fail_check += 1
                if check2 == 1 :
                    if today_count - yesterday_count != 0 :
                        check2_result = 1
                    if today_count - yesterday_count == 0 :
                        check2_result = 0
                        fail_check += 1
                if check3 == 1 :
                    column_to_check = row['CHECK_COLUMN']
                    today_sum = get_oracle_data_sum_by_snapdate(table_name,column_to_check,today_local_datadate)
                    yesterday_sum  = get_oracle_data_sum_by_snapdate(table_name,column_to_check,yesterday_local_date)
                    if today_sum == yesterday_sum or today_sum == 0:
                        fail_check += 1
                        check3_result = 0
                    else:
                        check3_result = 1

                sqlserver_db_function.write_in_check(table_name,today_local_datadate,today_count,yesterday_count,today_sum,yesterday_sum,check1_result,check2_result,check3_result,round_id)
                if fail_check >= 1 :
                    fail += 1
                if fail_check == 0 :
                    success += 1
            # 處理非本輪測試的問題
            else: 
                sqlserver_db_function.write_in_when_not_check(table_name,today_local_datadate,round_id)
    return(round_id,success,fail)

if __name__ == "__main__":
    test_script_file = "data_1_EMPUSER.csv"
    test_date = "20250817"
    try:
        round_id, success, fail = check_oracle_data(test_script_file, test_date,1)
        print(f"測試完成：round_id={round_id}, 成功={success}, 失敗={fail}")
    except Exception as e:
        print(f"執行錯誤：{e}")