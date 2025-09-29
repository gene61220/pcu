import pyodbc
import dc
from datetime import datetime

def generate_html_report(round_id, date8, success, fail, total, output_path,this_round, total_round,footer):
    # DB config
    server = f"{dc.getconfig_enc('sqlserverIP')},{dc.getconfig('sqlserverPort')}"
    database = "BKP"
    username = dc.getconfig("sqlserverUser")
    password = dc.getconfig_enc("sqlserverPassword")
    driver = "{ODBC Driver 17 for SQL Server}"
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name, DATADATE, today_count, yesterday_count,
               result1, result2, result3,today_sum,yesterday_sum 
        FROM PCU_check_results_hist
        WHERE round_id = ?
        and result1 >= 0
        ORDER BY ID asc, RESULT2 asc, RESULT3 asc, table_name
    """, round_id)

    rows = cursor.fetchall()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    html_header = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8" />
  <title>資料檢測報表</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      font-size: 14px;
      color: #333;
      background-color: #f9f9f9;
      margin: 0;
      padding: 0;
    }}
    .container {{
      max-width: 1100px;
      margin: 40px auto;
      background-color: #fff;
      padding: 24px;
      border: 1px solid #ddd;
      border-radius: 6px;
      position: relative;
    }}
    h2 {{ color: #007bff; }}
    .summary {{
      position: absolute;
      top: 24px;
      right: 24px;
      font-size: 14px;
      font-weight: bold;
      background-color: #f4f4f4;
      padding: 6px 12px;
      border-radius: 4px;
      border: 1px solid #ccc;
    }}
    .summary .success {{ color: green; margin-left: 8px; }}
    .summary .fail {{ color: red; margin-left: 8px; }}
    .summary .total {{ color: black; margin-left: 8px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 16px;
    }}
    th, td {{
      padding: 4px 10px;
      border: 1px solid #ccc;
      text-align: left;
    }}
    th {{ background-color: #f0f0f0; }}
    .pass {{ color: green; font-weight: bold; }}
    .eeee {{ color: #FF7F27; font-weight: bold; }}
    .ffff {{ color: grey; font-weight: bold; }}
    .fail {{ color: red; font-weight: bold; }}
    .nottoday{{color: #ACACAC;}}
    .footer {{
      margin-top: 20px;
      font-size: 12px;
      color: #777;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="summary">
      <span class="total">TOTAL: {total}</span>
      <span class="success">SUCCESS: {success}</span>
      <span class="fail">FAIL: {fail}</span>
    </div>
    <h2>{date8} - 到檔檢核結果通知</h2>
    <p>本日第 {this_round} 輪(共 {total_round} 輪)檢核</p>
    <p>以下為系統於 <strong style="font-weight:bold;font-size:large">{now_str}</strong> 自動檢查結果：</p>
    <table>
      <thead>
        <tr>
          <th>編號</th>
          <th>資料表</th>
          <th>資料日期</th>
          <th>匯入狀態</th>
          <th>與前日筆數檢核</th>
          <th>今日筆數</th>
          <th>前日筆數</th>
          <th>加總檢核</th>
          <th>今日加總</th>
          <th>前日加總</th>
        </tr>
      </thead>
      <tbody>
"""

    html_rows = ""
    for idx, row in enumerate(rows, 1):
        
        def status_tag(value):
            if value == 1:
                return '<span class="pass">通過</span>'
            elif value == 0:
                return '<span class="fail">失敗</span>'
            elif value == -1:
                return '<span class="eeee">本輪不適用</span>'
            elif value == -2:
                return '<span class="eeee">本輪不適用</span>'
            else:
                return '<span class="na">不適用</span>'
        def sum_check(value):
            if value == -99999999999999:
                return '不適用'
            else:
                return value

        html_rows += f"""        <tr>
          <td>{idx}</td>
          <td>{row.table_name}</td>
          <td>{row.DATADATE}</td>
          <td>{status_tag(row.result1)}</td>
          <td>{status_tag(row.result2)}</td>
          <td>{row.today_count}</td>
          <td>{row.yesterday_count}</td>
          <td>{status_tag(row.result3)}</td>
          <td>{sum_check(float(row.today_sum))}</td>
          <td>{sum_check(float(row.yesterday_sum))}</td>
          </tr>
"""
        row.today_sum="不適用"
        row.yesterday_sum="不適用"
    cursor.close()
    conn.close()
    html_footer = f"""      </tbody>
    </table>
    
    <hr>
    <span style = "font-size:large;font-weight:bold">以下為月檔清單</span>
    <table>
      <thead>
        <tr>
          <th>編號</th>
          <th>資料表</th>
          <th>資料日期</th>
          <th>匯入狀態</th>
          <th>與前日差異檢核</th>
          <th>今日筆數</th>
          <th>前日筆數</th>
          <th>加總比較</th>
          <th>今日加總</th>
          <th>前日加總</th>
        </tr>
      </thead>
      <tbody>
      """
    conn1 = pyodbc.connect(conn_str)
    cursor1 = conn1.cursor()
    cursor1.execute("""
        SELECT table_name, DATADATE, today_count, yesterday_count,
               result1, result2, result3
        FROM PCU_check_results_hist
        WHERE round_id = ?
        and result1 < 0
        ORDER BY ID asc, RESULT2 asc, RESULT3 asc, table_name
    """, round_id)
    rows1 = cursor1.fetchall()
    html_rows1=""""""
    for idx, row in enumerate(rows1, 1):
      def status_tag(value):
            if value == 1:
                return '<span class="pass">通過</span>'
            elif value == 0:
                return '<span class="fail">失敗</span>'
            elif value == -1:
                return '<span class="eeee">本輪不適用</span>'
            elif value == -2:
                return '<span class="ffff">月檔</span>'
            else:
                return '<span class="na">不適用</span>'
      html_rows1 += f"""        <tr>
          <td>{idx}</td>
          <td>{row.table_name}</td>
          <td>{row.DATADATE}</td>
          <td>{status_tag(row.result1)}</td>
          <td>{status_tag(row.result2)}</td>
          <td>{row.today_count}</td>
          <td>{row.yesterday_count}</td>
          <td>不適用</td>
          <td>不適用</td>
          <td>不適用</td>
          </tr>
"""
    footer1=  f"""
      </table>
      
      <div class="footer">{footer}</div>
    </div>
  </body>
  </html>
  """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_header + html_rows + html_footer +html_rows1+ footer1)
    cursor1.close()
    conn1.close()