import subprocess
from datetime import datetime, timedelta


def get_yesterday():
    current_date = datetime.now()
    yesterday = current_date - timedelta(days=1)
    formatted_yesterday = yesterday.strftime("%Y%m%d")
    return formatted_yesterday


def get_yesterday_log_name():
    logname_prefix = "/var/log/syslog/179.170.130.210.bn.2iij.net/message_179.170.130.210.bn.2iij.net_"
    logname_postfix = ".log"
    yesterday = get_yesterday()
    yesterday_log = logname_prefix + yesterday + logname_postfix
    return yesterday_log


def get_compress_log_name(origin_logname):
    compress_log_name = origin_logname + ".tar.gz"
    return compress_log_name


def compress_logs_using_tar(log_name, compress_logname):
    compress_cmd = "sudo tar -czvf " + compress_logname + " " + log_name
    result = subprocess.run(compress_cmd, shell=True, capture_output=True, text=True)
    rm_cmd = "sudo rm " + ori_log
    result = subprocess.run(rm_cmd, shell=True, capture_output=True, text=True)
    return True


def compress_and_delete_log(the_date):
    logname_prefix = "/var/log/syslog/179.170.130.210.bn.2iij.net/message_179.170.130.210.bn.2iij.net_"
    logname_postfix = ".log"
    ori_log = logname_prefix + the_date + logname_postfix
    tar_log = ori_log + ".tar.gz"
    cmd = "sudo tar -czvf " + tar_log + " " + ori_log
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result)
    cmd = "sudo rm " + ori_log
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result)


if __name__ == "__main__":
    # remote deployment test
    ori_log = get_yesterday_log_name()
    tar_log = get_compress_log_name(ori_log)
    compress_logs_using_tar(ori_log, tar_log)