# 资费数据下载与处理流程说明

## 系统概述

该系统用于下载和处理交易标的的历史和实时资金费率数据，主要包含三个Python脚本文件：

1. `1_downloder_history.py` - 历史数据下载器
2. `2_downloder_rt.py` - 实时数据下载器
3. `3_data_handler.py` - 数据处理程序

## 文件功能说明

### 1_downloder_history.py

- ​**功能**：通过调用历史接口下载历史数据
- ​**数据限制**：只能获取到前一天的数据
- ​**存储位置**：`data`文件夹
- ​**文件命名**：`symbolname+funding_history.csv`
- ​**注意事项**：即使标的已下架，仍会生成对应的CSV文件，并可能推送重复数据（如AGIX-USDT持续推送0.0001000）(忽略)

### 2_downloder_rt.py

- ​**功能**：通过调用实时接口下载实时数据
- ​**数据限制**：只能获取到当前时间的数据
- ​**存储位置**：`data_rt`文件夹
- ​**文件命名**：`symbolname+funding_realtime.csv`
- ​**运行频率**：每5分钟运行一次

### 3_data_handler.py

- ​**功能**：
  1. 处理历史数据，将历史数据转换为5分钟频率的DataFrame
  2. 处理实时数据，将实时数据增量更新到历史数据中
  3. 执行数据填充、清洗和频率转换

- ​**输出文件**：`dubug_data.funding_5min_combined.parquet`

## 配置与运行步骤

### 初始设置，下载任务

1. 一次性运行`1_downloder_history.py`下载截止到T-1日的历史数据
2. 设置定时任务，每隔5分钟运行`2_downloder_rt.py`下载实时数据

### 数据处理

3. 首次运行`3_data_handler.py`：
   - 设置`first_run = False`
   - 程序将处理T-1日之前的历史数据 + T日当前时刻的实时数据
   - 输出文件：`dubug_data.funding_5min_combined.parquet`

4. 后续持续运行：
   - 设置`first_run = True`
   - 配置5分钟运行一次的定时任务执行`3_data_handler.py`

## 定时任务配置

```bash
# 每5分钟运行实时数据下载器
*/5 * * * * /usr/bin/python3 /path/to/2_downloder_rt.py

# 每5分钟运行数据处理程序
*/5 * * * * /usr/bin/python3 /path/to/3_data_handler.py