
import requests
import json
import pandas as pd


today_str = '2020-12-18'
api_url = r'https://192.168.50.41:8443/api/v1/dailyreport/tradetrx/view/%(date)s' % {'date': today_str}
#api_url = r'https://192.168.50.41:8443/api/v1/info'
r = requests.get(api_url, verify=False)
list_data = json.loads(r.content)["result"]["data"]
trade_summary_data_df = pd.DataFrame(list_data)

print(trade_summary_data_df)

