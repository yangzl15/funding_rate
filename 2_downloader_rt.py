from datetime import datetime
import pandas as pd
import logging
import os
from exchanges.binance_rt import BinanceRealtime
import config

today_str = datetime.now().strftime('%Y-%m-%d')
logdir = config.log_dir
os.makedirs(f"{logdir}/{today_str}", exist_ok=True)
log_path = f"{logdir}/{today_str}/download_realtime_{today_str}.log"
logging.basicConfig(filename=log_path, filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

csv_realtime_dir = config.csv_realtime_dir
os.makedirs(csv_realtime_dir, exist_ok=True)

exchange = BinanceRealtime()
try:
    df = exchange.fetch_data()
    df = df[df['Symbol'].str.endswith('USDT')]
    df['Symbol'] = df['Symbol'].str.lower().str.replace(r'usdt$', '-usdt', regex=True)
    df['Date'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    df['Funding Rate'] = df['LastFundingRate']
    df['Next Funding Time'] = pd.to_datetime(df['NextFundingTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df[['Symbol', 'Date', 'Funding Rate', 'Next Funding Time']]
    for _, row in df.iterrows():
        path = os.path.join(csv_realtime_dir, f"{row['Symbol']}_realtime.csv")
        exists = os.path.isfile(path)
        pd.DataFrame([row]).to_csv(path, mode='a', header=not exists, index=False)
    logging.info("real-time data appended")
except Exception as e:
    logging.error(f"failed to fetch or append realtime data: {e}")
