import time
import subprocess
from datetime import datetime, timedelta


def generate_month_days(year, month):
    # 创建一个列表用于存储日期字符串
    days = []
    # 创建指定月份的第一个日期
    date = datetime(year, month, 1)
    while date.month == month:
        # 将日期格式化为字符串 YYYYMMDD
        day_str = date.strftime('%Y%m%d')
        days.append(day_str)
        # 增加一天
        date += timedelta(days=1)
    return days


def run_cmd(cmd_str: str):
    print("Execute: ", cmd_str)

    # 执行命令并获取输出
    result = subprocess.run(cmd_str, shell=True, capture_output=True, text=True)

    # 打印命令的输出
    print(result.stdout)

    # 打印命令的错误输出（如果有）
    if result.stderr:
        print('Error:', result.stderr)


if __name__ == '__main__':
    days_list = generate_month_days(2024, 5)
    for day in days_list:
        time.sleep(2)
        log_tar_file = 'message_179.170.130.210.bn.2iij.net_' + day + '.log.tar.gz'
        log_file = 'message_179.170.130.210.bn.2iij.net_' + day + '.log'

        cmd = 'tar -zxvf ' + log_tar_file
        run_cmd(cmd)







