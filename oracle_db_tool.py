import os
import dc
import cx_Oracle as ora
import dc as dc
from datetime import datetime
oracle_client_initialized = False

def initialize_oracle_client():
    global oracle_client_initialized
    if not oracle_client_initialized:
        os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.AL32UTF8'
        ora.init_oracle_client(lib_dir=r"D:\deploy\prepare\tools\instantclient_19_22")
        oracle_client_initialized = True

# 建立 Oracle 連線
def create_oracle_connection():
    global oracle_client_initialized
    if oracle_client_initialized== False:
        initialize_oracle_client()
        oracle_client_initialized = True
    Oracle_IP = dc.getconfig_enc('Oracle_IP')
    Oracle_Port = dc.getconfig('Oracle_Port')
    Oracle_Owner = dc.getconfig('Oracle_Owner')
    Oracle_SID = dc.getconfig('SID')
    Oracle_Password = dc.getconfig_enc('Oracle_Password')
    os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.AL32UTF8'
    dsn = ora.makedsn(f'{Oracle_IP}', f'{Oracle_Port}', service_name=f'{Oracle_SID}') ## 參數
    return ora.connect(f'{Oracle_Owner}', f'{Oracle_Password}', dsn)

# 查詢資料筆數，回傳資料筆數
def get_oracle_data_count_by_snap_date(table_name, snap_date):
    print(table_name,datetime.now(),"START")
    if table_name[:6] == "DM_EPM":
        query = f"SELECT COUNT(1) FROM {table_name} WHERE SNAP_DATE = '{snap_date[:4]}{snap_date[5:7]}{snap_date[8:10]}'"
    elif table_name [:8] == "DM_SHARE":
        query = f"SELECT COUNT(1) FROM {table_name} WHERE SNAP_YYYYMMDD = '{snap_date[:4]}{snap_date[5:7]}{snap_date[8:10]}'"
    else:
        query = f"SELECT COUNT(1) FROM {table_name} WHERE SNAP_DATE = TO_DATE('{snap_date}','YYYY-MM-DD')"
    # print('QUERY >>>  ' , query)
    with create_oracle_connection() as conn:
        with conn.cursor() as cursor:
            try:
                retn = cursor.execute(query) 
                print(table_name,datetime.now(),"DONE")
                return cursor.fetchone()[0]
            except ora.Error as e:
                return -1

def get_oracle_data_count_by_other_date(table_name, date, datecolumn_name):
    print(date)
    query = f"SELECT COUNT(1) FROM {table_name} WHERE {datecolumn_name} = TO_DATE({date}, 'YYYY-MM-DD')"
    with create_oracle_connection() as conn:    
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()[0]
            except ora.Error as e:
                print(f"[OTHER_DATE COUNT ERROR] {e}")
                return -1
    
def get_oracle_data_sum_by_snapdate(table_name, column_name, date):
    #condition = select # column_name = 
    query = f"SELECT SUM({column_name}) FROM {table_name} WHERE SNAP_DATE = TO_DATE('{date}', 'YYYY-MM-DD')"
    with create_oracle_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()[0]
            except ora.Error as e:
                print(f"[SUM ERROR] {e}")
                return -1

# 查詢欄位加總值，回傳欄位值加總，必須給條件，無法加總給 'NAN'
def get_oracle_snap_date_column_sum(table_name, snap_date, column_name):
    sql_query = f"SELECT SUM({column_name}) FROM {table_name} WHERE SNAP_DATE = TO_DATE('{snap_date}', 'YYYY-MM-DD')"
    with create_oracle_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql_query)
                result = cursor.fetchone()[0]
                return float(result) if result is not None else 0.0
            except ora.Error as e:
                print(f"[SUM ERROR] {e}")
                return -1.0

# 查詢某欄位的不同值個數（distinct count）
def get_oracle_column_distinct(table_name, column_name, condition=''):
    sql_query = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}"
    if condition:
        sql_query += f" WHERE {condition}"
    with create_oracle_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql_query)
                result = cursor.fetchone()[0]
                return -1 if result is None else result
            except ora.Error as e:
                print(f"[DISTINCT ERROR] {e}")
                return -1


def get_oracle_data_by_query(sql_query):
    with create_oracle_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return columns, rows

               
if __name__ == "__main__":
    print("結果: ", get_oracle_data_count_by_snap_date("ODS_HK.CBSHKGLST22", "2025-01-28"))

