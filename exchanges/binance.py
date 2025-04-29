
import requests
import pandas as pd
from datetime import datetime
from exchanges.exchange_base import ExchangeBase
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class Binance(ExchangeBase):
    def fetch_data(self):
        print("Start processing Binance funding rates. It can take some minutes. Please wait. ....")
        start_ts, end_ts = self.convert_start_end_time()
        url = 'https://down.metatomind.com/fapi/v1/fundingRate'
        symbol_converted = self.symbol.replace("-", "")
        all_data = []
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[502,503,504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        while start_ts < end_ts:
            params = {"symbol": symbol_converted, "startTime": start_ts, "limit": 1000}
            response = session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                funding_rates = response.json()
                if not funding_rates:
                    break
                for rate in funding_rates:
                    all_data.append({"Symbol": self.symbol, "Date": datetime.fromtimestamp(rate["fundingTime"]/1000), "Funding Rate": rate["fundingRate"]})
                last_funding_time = funding_rates[-1]["fundingTime"]
                if last_funding_time >= end_ts:
                    break
                start_ts = last_funding_time + 1
            else:
                print("Failed to fetch data from Binance")
                break
        df = pd.DataFrame(all_data)
        if "Date" in df.columns:
            df = df[df["Date"] < datetime.fromtimestamp(end_ts/1000)]
        else:
            print("No data fetched or Date column not found in DataFrame.")
        return df
