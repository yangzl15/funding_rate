# config.py
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(f"BASE_DIR: {BASE_DIR}")
# 日志目录
log_dir = os.path.join(BASE_DIR, "logs")

# 存放下载好的csv文件的目录
csv_history_dir = os.path.join(BASE_DIR, "data") # 历史
csv_realtime_dir = os.path.join(BASE_DIR, "data_rt") # 实时

# 存放处理好的parquet文件的目录
output_dir = os.path.join(BASE_DIR, "output")

# 存放debug需要用的中间数据的目录
debug_data_dir = os.path.join(BASE_DIR, "debug_data")


#开始处理的日期
start_date = "2020-01-01"
# 结束日期 今天
end_date = (pd.Timestamp.now()).strftime("%Y-%m-%d")
# rt_start_date = end_date

# 
configurations = [
    {"interval": "1h", "divisor": 12},
    {"interval": "2h", "divisor": 24},
    {"interval": "4h", "divisor": 48},
    {"interval": "8h", "divisor": 96}
]
exchange = "binance"

"""
存放把原始csv(不同Uid, 同一Uid不同天的频率可能都不一样, 需要先统一按资金费率公布的时刻去fill每天288个5min bar对应的Label)
fill好的两个parquet是用来debug的,不需要debug可以不存, 处理成parquet的目录
"""

# ===========================downloder.py===========================
futures_data_dir = "/data-platform/crypto/output_parquet" ## 线上的话需要改成对应的全量期货数据路径

