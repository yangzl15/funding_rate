import importlib
import pandas as pd
import logging
import os
import time
import shutil
import config

# 加载期货数据获取需要下载的symbol列表
df_last_futures = pd.read_parquet(f"{config.futures_data_dir}/pv_last.parquet")
symbol_list = df_last_futures.columns.tolist()
print(f"len(symbol_list): {len(symbol_list)}")

symbol_list_path = f"{config.BASE_DIR}/symbol_list.txt"

with open(symbol_list_path, "w") as f:
    for symbol in symbol_list:
        f.write(f"{symbol}\n")
        
time_str = pd.Timestamp.now().strftime("%H%M%S")
today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
log_base_path = config.log_dir
log_date_path = f"{log_base_path}/{today_str}"
os.makedirs(log_date_path, exist_ok=True)
log_path = f"{log_date_path}/download_{time_str}.log"

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("symbol_list: %s", symbol_list)
EXCHANGE = "binance"
START_DATE = config.start_date
END_DATE = config.end_date

try:
    exchange_module = importlib.import_module(f'exchanges.{EXCHANGE}')
    ExchangeClass = getattr(exchange_module, EXCHANGE.capitalize())
except Exception as e:
    logging.error(f"Failed to import exchange module or get class for {EXCHANGE}. Error: {e}")
    raise e

# 每次会把文件夹内容清空，防止有些天获取不到文件但是后续handler会读取到老的无效数据
storage_base = config.csv_history_dir
os.makedirs(storage_base, exist_ok=True)
for f in os.listdir(storage_base):
    p = os.path.join(storage_base, f)
    if os.path.isfile(p) or os.path.islink(p):
        os.unlink(p)
    else:
        shutil.rmtree(p)

def main():
    for symbol in symbol_list:
        try:
            exchange = ExchangeClass(symbol, START_DATE, END_DATE)
            data = exchange.fetch_data()
            data.to_csv(f"{storage_base}/{symbol}_funding_history.csv", index=False)
            time.sleep(1)
            logging.info(f"Successfully downloaded data for {symbol}.")
        except Exception as e:
            logging.error(f"Failed to download data for {symbol}. Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()