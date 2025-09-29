import sys
import os
from datetime import datetime
from check_data_function import check_oracle_data
from send_mail import send_mail
from generate_html_report import generate_html_report
def convert_to_yyyymmdd(date12):
    return date12[:8]

def parse_script_filename(script_file):
    base = os.path.basename(script_file)
    parts = base.replace(".csv", "").split("_")
    turn = 0
    if len(parts) >= 3:
        turn = int(parts[1])  # e.g., script_1_user.csv → 1
        recipient_group = parts[2]  # e.g., script_1_user.csv → user
    else:
        turn = 9999
        recipient_group = "NA"
    return turn, recipient_group

def load_recipients(recipient_group):
    filename = f"D:/PRG/PCU/recipient/{recipient_group}.txt"
    if not os.path.exists(filename):
        return None
    with open(filename, 'r', encoding='utf-8') as f:
        recipients = f.read().strip()
    return recipients

def main():
    if len(sys.argv) < 5:
        print("不合規範，請使用方式: python main.py <資料日:YYYYMMDDxxxx> <SCRIPT.csv>")
        sys.exit(1)

    input_date12 = sys.argv[1]
    script_file = sys.argv[2]
    total_round = sys.argv[3]
    footer = sys.argv[4]
    # print(input_date12)

    date8 = convert_to_yyyymmdd(input_date12)
    round, recipient_group = parse_script_filename(script_file)
    recipients = load_recipients(recipient_group)

    print(f"資料日：{date8}")
    print(f"腳本：{script_file}")
    print(f"收件群組：{recipient_group}")
    print(f"當日第{round}輪")

    if not recipients:
        print(f"找不到收件者設定清單（{recipient_group}.txt），將跳過發信。")

    # V 執行檢查
    print("開始執行資料檢核...")
    try:
        round_id, success, fail = check_oracle_data(script_file, date8, round)
        total = success + fail
        print(f"檢核完成：成功 {success}、失敗 {fail}，執行批號 {round_id}")

        # 產生 HTML 報表
        report_dir = "report"
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)

        output_path = os.path.join(report_dir, f"{round_id}_check_report.html")
        generate_html_report(round_id, date8, success, fail, total, output_path,round, total_round, footer)
        print(f"報表已產出：{output_path}")

        # 寄信（如有收件者）
        if recipients:
            print("正在寄送通知信...")
            send_mail(output_path, recipients)
            print("信件已發送。")
        else:
            print("未指定收件者，跳過寄送。")

    except Exception as e:
        print(f"失敗!發生錯誤：{str(e)}")

if __name__ == "__main__":
    main()