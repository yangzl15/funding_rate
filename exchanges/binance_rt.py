from exchanges.exchange_base import ExchangeBase
import requests
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class BinanceRealtime(ExchangeBase):
    def __init__(self):
        super().__init__(None, None, None)
        self.url = 'https://down.metatomind.com/fapi/v1/premiumIndex'
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)

    def fetch_data(self):
        response = self.session.get(self.url, timeout=10)
        items = response.json() if response.status_code == 200 else []
        records = []
        for item in items:
            records.append({
                'Symbol': item['symbol'],
                'MarkPrice': item['markPrice'],
                'IndexPrice': item['indexPrice'],
                'EstimatedSettlePrice': item['estimatedSettlePrice'],
                'LastFundingRate': item['lastFundingRate'],
                'InterestRate': item['interestRate'],
                'NextFundingTime': datetime.fromtimestamp(item['nextFundingTime'] / 1000),
                'Time': datetime.fromtimestamp(item['time'] / 1000)
            })
        return pd.DataFrame(records)
