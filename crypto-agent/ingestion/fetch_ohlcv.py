import ccxt, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
from datetime import datetime, timezone

def fetch_binance(symbol='BTC/USDT', timeframe='4h',
                  since='2015-01-01T00:00:00Z', batch_ms=1000*60*60*24*30):
    ex = ccxt.binance({'enableRateLimit': True})
    since_ms = ex.parse8601(since)
    all_rows = []
    while since_ms < ex.milliseconds():
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, since_ms, limit=1000)
        if not ohlcv:
            break
        since_ms = ohlcv[-1][0] + int(batch_ms)
        all_rows.extend(ohlcv)
    df = pd.DataFrame(all_rows, columns=['ts','open','high','low','close','volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    return df

if __name__ == "__main__":
    df = fetch_binance()
    df['dt'] = df['ts'].dt.date
    for date, grp in df.groupby('dt'):
        path = f"data/raw/candles_4h/dt={date}/candles.parquet"
        pq.write_table(pa.Table.from_pandas(grp.drop(columns='dt')), path)