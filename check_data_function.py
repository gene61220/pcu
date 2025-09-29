import os
import re
import csv
from datetime import datetime, timedelta

from oracle_db_tool import (
    get_oracle_data_count_by_snap_date,
    get_oracle_data_sum_by_snapdate,
)
import datadate as datadate
import sqlserver_db_function


# --- Helpers ---------------------------------------------------------------

def _normalize_freq(freq: str) -> str:
    """
    接受 D/E/F（大小寫）。將 M/EOM -> 'E'；BOM -> 'F'；其他值原樣回傳。
    """
    if not freq:
        return ''
    f = freq.strip().upper()
    if f in ('D', 'E', 'F'):
        return f
    if f in ('M', 'EOM', 'MONTHEND', 'MONTH_END'):
        return 'E'
    if f in ('BOM', 'MONTHSTART', 'MONTH_START'):
        return 'F'
    return f


def _is_boundary_run_day_window(unformatted_date8: str,
                                start_day: int = 2,
                                end_day: int = 5,
                                now_dt: datetime | None = None) -> bool:
    """
    月界檢核允許在『(業務日+1天).month 的 start_day ~ end_day』期間執行（含邊界）。
    不使用任何 DB flag；每天重跑是獨立的一批 round_id。
    """
    if now_dt is None:
        now_dt = datetime.now()
    d = datetime.strptime(unformatted_date8, "%Y%m%d").date()
    next_day = d + timedelta(days=1)          # 以界點隔天所在月份為「關帳月」
    begin = next_day.replace(day=start_day)   # 例如 2 號
    end   = next_day.replace(day=end_day)     # 例如 5 號
    today = now_dt.date()
    return begin <= today <= end



def _first_day_prev_month(d: datetime) -> datetime:
    first_this = d.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    return last_prev.replace(day=1)

def _last_day_prev_month(d: datetime) -> datetime:
    first_this = d.replace(day=1)
    return first_this - timedelta(days=1)

def _first_day_prev2_month(d: datetime) -> datetime:
    return _first_day_prev_month(_first_day_prev_month(d))

def _last_day_prev2_month(d: datetime) -> datetime:
    return _last_day_prev_month(_first_day_prev_month(d))

def _fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


# --- Main ------------------------------------------------------------------

def check_oracle_data(script_file: str, unformatted_date8: str, current_round: int,
                      base_dir: str = "D:/PRG/PCU/script") -> tuple[str, int, int]:
    """
    讀取檢核腳本(csv) -> 逐表做三項檢核 -> 寫入SQL Server結果 -> 回傳檢核ID、成功數量、失敗數量

      - D：當天執行，日期取用 datadate（today vs yesterday，跨區）
      - E/F：僅在『(業務日+1天) 的月份之 2 號』執行；
             查詢一律用「日曆日」(不上 datadate)：
               * 今日參照：先「上月1號」；若=0 再「上月最後一天」；若皆=0 → 今日檢核失敗
               * 對比參照：採用上上月對應日（1號或月末）
      - 非觸發日一律寫入 not-check
    僅回傳 (round_id, success, fail)；不印出任何 log。
    """
    round_id = datetime.now().strftime('%Y%m%d%H%M%S')

    # 腳本路徑
    if os.path.isabs(script_file) and os.path.exists(script_file):
        script_path = script_file
    else:
        script_path = os.path.join(base_dir, script_file)

    # 業務日格式檢查
    if not re.fullmatch(r'\d{8}', unformatted_date8):
        raise ValueError(f"日期格式錯誤，應為 8 碼數字（yyyyMMdd）：{unformatted_date8}")
    date_obj = datetime.strptime(unformatted_date8, "%Y%m%d")
    date8_dash = f"{unformatted_date8[:4]}-{unformatted_date8[4:6]}-{unformatted_date8[6:]}"

    success = 0
    fail = 0

    with open(script_path, 'r', encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file)
        required_cols = {'TABLE_NAME', 'REGION', 'FREQUENCY', 'CHECK1', 'CHECK2', 'CHECK3', 'ROUND', 'CHECK_COLUMN'}
        missing = required_cols - set(col.strip() for col in (reader.fieldnames or []))
        if missing:
            raise ValueError(f"CSV 欄位缺少: {', '.join(sorted(missing))}")

        for row_idx, row in enumerate(reader, start=2):
            try:
                table_name = (row.get('TABLE_NAME') or '').strip()
                region_type = (row.get('REGION') or '').strip()
                freq = _normalize_freq(row.get('FREQUENCY') or '')

                # 缺關鍵資訊 → 記 not-check
                if not table_name or not region_type:
                    try:
                        tl = datadate.get_datadate(date8_dash, region_type or 'ALL')
                        sqlserver_db_function.write_in_when_not_check(table_name or f'ROW{row_idx}', tl, round_id)
                    except Exception:
                        pass
                    continue

                # D 用：仍採跨區業務日；E/F 的 not-check 也用這個日期入庫，維持 schema 一致
                today_local_datadate = datadate.get_datadate(date8_dash, region_type)
                yesterday_local_date = datadate.get_yesterday_datadate(date8_dash, region_type)

                # 是否本輪檢核（嚴格模式）
                if freq == 'D':
                    do_check = True
                elif freq in ('E', 'F'):
                    do_check = _is_boundary_run_day_window(unformatted_date8, start_day=2, end_day=5)

                else:
                    do_check = False

                if not do_check:
                    sqlserver_db_function.write_in_when_not_check(table_name, today_local_datadate, round_id)
                    continue

                # 轉型工具（3=不檢核）
                def _to_int_or(val: str, default: int = 3) -> int:
                    try:
                        return int((val or '').strip())
                    except Exception:
                        return default

                check1 = _to_int_or(row.get('CHECK1'), 3)
                check2 = _to_int_or(row.get('CHECK2'), 3)
                check3 = _to_int_or(row.get('CHECK3'), 3)
                this_row_round = _to_int_or(row.get('ROUND'), 9999)
                column_to_check = (row.get('CHECK_COLUMN') or '').strip()

                # 初始化
                check1_result = check2_result = check3_result = 3
                fail_check = 0
                today_sum = None
                yesterday_sum = None

                # ======================= D：日常邏輯（使用 datadate） =======================
                if freq == 'D':
                    today_count = get_oracle_data_count_by_snap_date(table_name, today_local_datadate)
                    if yesterday_local_date is None:
                        yesterday_count = 0
                    else:
                        yesterday_count = get_oracle_data_count_by_snap_date(table_name, yesterday_local_date)

                    if check1 == 1:
                        if today_count > 0:
                            check1_result = 1
                        else:
                            check1_result = 0
                            if this_row_round < current_round:
                                try:
                                    sqlserver_db_function.write_in_not_this_round_check(
                                        table_name, today_local_datadate, round_id
                                    )
                                except Exception:
                                    pass
                            fail_check += 1

                    if check2 == 1:
                        if (today_count - yesterday_count) != 0:
                            check2_result = 1
                        else:
                            check2_result = 0
                            fail_check += 1

                    if check3 == 1:
                        today_sum = get_oracle_data_sum_by_snapdate(table_name, column_to_check, today_local_datadate)
                        if yesterday_local_date is None:
                            yesterday_sum = 0
                        else:
                            yesterday_sum = get_oracle_data_sum_by_snapdate(table_name, column_to_check, yesterday_local_date)

                        if today_sum == yesterday_sum or today_sum == 0:
                            check3_result = 0
                            fail_check += 1
                        else:
                            check3_result = 1

                # ====== E/F：月界邏輯（嚴格隔月2號；用日曆日，不走 datadate） ======
                else:
                    # 關帳月基準 = (業務日 + 1 天) 的月份
                    next_day = date_obj + timedelta(days=1)

                    # 上月 / 上上月（相對於關帳月）
                    prev_first  = _first_day_prev_month(next_day)   # 上月1號
                    prev_last   = _last_day_prev_month(next_day)    # 上月最後一天
                    prev2_first = _first_day_prev2_month(next_day)  # 上上月1號
                    prev2_last  = _last_day_prev2_month(next_day)   # 上上月最後一天

                    pf_str  = _fmt_date(prev_first)
                    pl_str  = _fmt_date(prev_last)
                    p2f_str = _fmt_date(prev2_first)
                    p2l_str = _fmt_date(prev2_last)

                    # 今日: 先查上月初；若=0 -> 再上月底=0 -> 失敗
                    cnt_pf = get_oracle_data_count_by_snap_date(table_name, pf_str)
                    if cnt_pf != 0:
                        today_anchor = 'F'
                        today_key = pf_str
                        today_count = cnt_pf # 對比基準：上上月初
                        yesterday_key = p2f_str
                        yesterday_count = get_oracle_data_count_by_snap_date(table_name, yesterday_key)
                    else:
                        cnt_pl = get_oracle_data_count_by_snap_date(table_name, pl_str)
                        if cnt_pl != 0:
                            today_anchor = 'L'
                            today_key = pl_str
                            today_count = cnt_pl # 對比基準：上上月底日
                            yesterday_key = p2l_str
                            yesterday_count = get_oracle_data_count_by_snap_date(table_name, yesterday_key)
                        else:
                            today_anchor = 'N'
                            today_key = None
                            today_count = 0
                            yesterday_key = None
                            yesterday_count = 0

                    # 檢核一：今日要有資料
                    if check1 == 1:
                        if today_count > 0:
                            check1_result = 1
                        else:
                            check1_result = 0
                            if this_row_round < current_round and today_anchor == 'N':
                                try:
                                    sqlserver_db_function.write_in_not_this_round_check(
                                        table_name, today_local_datadate, round_id
                                    )
                                except Exception:
                                    pass
                            fail_check += 1

                    # 檢核二：與上上月對應日筆數有差異
                    if check2 == 1:
                        if (today_count - yesterday_count) != 0:
                            check2_result = 1
                        else:
                            check2_result = 0
                            fail_check += 1

                    # 檢核三：合計比較（同 anchor 的對應日）
                    if check3 == 1:
                        if today_anchor in ('F', 'L'):
                            today_sum = get_oracle_data_sum_by_snapdate(table_name, column_to_check, today_key)
                            yesterday_sum = get_oracle_data_sum_by_snapdate(table_name, column_to_check, yesterday_key)
                        else:
                            today_sum = 0
                            yesterday_sum = 0

                        if today_sum == yesterday_sum or today_sum == 0:
                            check3_result = 0
                            fail_check += 1
                        else:
                            check3_result = 1

                # 寫入結果（業務日仍記原本 today_local_datadate，維持 schema）
                sqlserver_db_function.write_in_check(
                    table_name,
                    today_local_datadate,
                    today_count,
                    yesterday_count,
                    today_sum,
                    yesterday_sum,
                    check1_result,
                    check2_result,
                    check3_result,
                    round_id
                )

                if fail_check >= 1:
                    fail += 1
                else:
                    success += 1

            except Exception:
                # 單列例外：盡量寫入 not-check；不中斷整批，也不輸出
                try:
                    tl = datadate.get_datadate(date8_dash, (row.get('REGION') or '').strip() or 'ALL')
                    sqlserver_db_function.write_in_when_not_check(row.get('TABLE_NAME', f'ROW{row_idx}'), tl, round_id)
                except Exception:
                    pass
                continue

    return (round_id, success, fail)
