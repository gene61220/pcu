import logging
from oracle_db_tool import create_oracle_connection
import cx_Oracle as ora
import os
import dc as dc
import re
oracle_client_initialized = False
# 設定日誌檔案
log_dir = 'log'
os.makedirs(log_dir, exist_ok=True)  # 確保目錄存在
logging.basicConfig(
    filename=os.path.join(log_dir, 'datadate.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# # 初始化 Oracle 客戶端
# def initialize_oracle_client():
#     if 'ORACLE_HOME' not in os.environ:
#         os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.AL32UTF8'
#         ora.init_oracle_client(lib_dir=r"D:\deploy\prepare\tools\instantclient_19_22")
# oracle_client_initialized = False
# def initialize_oracle_client():
#     global oracle_client_initialized
#     if not oracle_client_initialized:
#         os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.AL32UTF8'
#         ora.init_oracle_client(lib_dir=r"D:\deploy\prepare\tools\instantclient_19_22")
#         oracle_client_initialized = True

# 建立 Oracle 資料庫連線


# 從資料庫抓取各地區資料日期
def fetch_datadate(DATA_DT_NOMINAL):
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', DATA_DT_NOMINAL):
        raise ValueError(f"日期格式錯誤：{DATA_DT_NOMINAL}，應為 yyyy-mm-dd")

    try:
        with create_oracle_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT DATA_DT_ACTUAL, DATA_ZONE FROM WA_FTPALM.PARAM_ETL_DT_FREQ WHERE DATA_DT_NOMINAL = '{DATA_DT_NOMINAL}'"""
                cursor.execute(query) 
                return cursor.fetchall()

    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"[Oracle Error] {error.message}")
        return None

    except Exception as e:
        logging.error(f"[Unexpected Error] {e}")
        return None

# 取得今天資料日期
# 根據 DATA_DT_NOMINAL + ZONE 查資料庫
def get_datadate(batch_day, zone):
    # print(f"get_data  dt zone = {zone} day = {batch_day}")
    try:
        with create_oracle_connection() as connection:
            with connection.cursor() as cursor:
                # print(batch_day,'   ',zone)
                query = f"""SELECT DATA_DT_ACTUAL FROM WA_FTPALM.PARAM_ETL_DT_FREQ WHERE DATA_DT_NOMINAL = '{batch_day}' AND DATA_ZONE = '{zone}'"""
                # print("Data date Query = ", query)
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        logging.error(f"[get_datadate DB Error] {e}")
        return None

# 查詢昨日實際資料日（直接從資料庫比對 DATA_DT_ACTUAL）
def get_yesterday_datadate(batch_day, zone):
    today = get_datadate(batch_day, zone)
    # print("YESTERDAYS TODAY = " , today)
    try:
        # print("success init")
        with create_oracle_connection() as connection1:
            # print("connection created")
            with connection1.cursor() as cursor:
                # print("cursor created")
                query = f"""SELECT DATA_DT_ACTUAL FROM WA_FTPALM.PARAM_ETL_DT_FREQ WHERE DATA_DT_ACTUAL < '{today}' AND DATA_ZONE = '{zone}' ORDER BY DATA_DT_ACTUAL DESC """
                # print("------------")
                # print("yesterday query : " , query)
                cursor.execute(query)
                result= cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        logging.error(f"[get_yesterday_datadate Error] {e}")
        return None

if __name__=="__main__":    
    b = get_datadate("2025-05-23","VN")
    a = get_yesterday_datadate("2025-05-23","VN")
    print(b)
    print(a)