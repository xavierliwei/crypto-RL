import requests, pandas as pd, pyarrow.parquet as pq, pyarrow as pa, os, time
API = os.getenv("GLASSNODE_KEY")

def fetch_metric(metric, since='2015-01-01'):
    url = f"https://api.glassnode.com/v2/metrics/{metric}"
    params = dict(api_key=API, a='BTC', s=int(pd.Timestamp(since).timestamp()))
    df = pd.DataFrame(requests.get(url, params=params).json())
    df['t']  = pd.to_datetime(df['t'], unit='s', utc=True)
    df.rename(columns={'t':'ts','v':'value'}, inplace=True)
    df['metric'] = metric
    return df

for m in ["market/price_usd", "indicators/mvrv_z_score"]:
    df = fetch_metric(m)
    pq.write_table(pa.Table.from_pandas(df), f"data/raw/glassnode_daily/metric={m.replace('/','_')}.parquet")
    time.sleep(1)           # API courtesy
    