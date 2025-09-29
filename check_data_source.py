import sys
import os
import csv
from datetime import datetime
import oracle_db_tool as ora
import send_mail as sd

def convert_to_yyyymmdd(date12):
    return date12[:8]

def load_table(this_round, sql_string_source):
    date = datetime.now().strftime('%Y%m%d')
    sql_filename = f'{sql_string_source}.txt'
    route = f'D:/PRG/PCU/IT_selection/{sql_filename}'
    filename = f'IT_Checker_{date}-{this_round}.csv'
    with open(route, 'r', encoding='utf-8') as file:
        sql_string = file.read()# 執行查詢
    
    columns, rows = ora.get_oracle_data_by_query(sql_string)
    
    # 寫入 CSV
    output_dir = f'D:/PRG/PCU/IT_check_results/{date}/'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    file_path = output_dir + filename
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)  # 寫入欄位名稱
        writer.writerows(rows)    # 寫入資料列
    return file_path

if __name__ == "__main__":
    if len(sys.argv) < 5 :
        print('參數不足，請補齊 yyyymmddHHMM this_round objectpath recipient title content')
    else :
        datadate = convert_to_yyyymmdd(sys.argv[1])
        this_round = sys.argv[2]
        object_path = sys.argv[3]
        recipient = sys.argv[4]

        if len(sys.argv)>= 6 :
            with open(f'D:/PRG/PCU/IT_selection/{sys.argv[5]}','r', encoding='utf-8') as file:
                title = file.read()
        else :
            title = f'itchecker round{this_round}'
        with open(f'D:/PRG/PCU/RECIPIENT/{recipient}.txt','r', encoding='utf-8') as file:
            recipient_str = file.read()
        if len(sys.argv)>=7 :
            with open(f'D:/PRG/PCU/IT_selection/{sys.argv[6]}','r', encoding='utf-8') as file:
                content_txt = file.read()
        else :
            content_txt = f'itchecker round{this_round}'

        file_path = load_table(this_round, object_path)

        sd.send_mail_with_object(recipient_str, file_path, title, content_txt)