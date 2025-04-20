import ccxt, pandas as pd, pyarrow.parquet as pq, pyarrow as pa
import os
from datetime import datetime, timezone

def fetch_coinbase(symbol='BTC-USD', timeframe='1h',
                  since='2019-01-01T00:00:00Z', batch_ms=1000*60*60*24*30):
    ex = ccxt.coinbase({'enableRateLimit': True})
    since_ms = ex.parse8601(since)
    print(f"Fetching {symbol} {timeframe} candles from {since}")
    all_rows = []
    # Get timeframe duration in milliseconds
    timeframe_duration_ms = ex.parse_timeframe(timeframe) * 1000

    while since_ms < ex.milliseconds():
        print(f"Fetching candles since {ex.iso8601(since_ms)}")
        # Use limit=300 (slightly less than max 350 for safety)
        ohlcv = ex.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=300)
        if not ohlcv:
            print("No more data returned.")
            break

        # Important: Update since_ms to the timestamp of the *last* candle + timeframe duration
        # This prevents overlapping requests and fetches the next batch correctly.
        since_ms = ohlcv[-1][0] + timeframe_duration_ms
        all_rows.extend(ohlcv)

    print(f"Fetched {len(all_rows)} total candles.")
    if not all_rows:
        return pd.DataFrame(columns=['ts','open','high','low','close','volume'])

    df = pd.DataFrame(all_rows, columns=['ts','open','high','low','close','volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    return df

if __name__ == "__main__":
    df = fetch_coinbase()
    print(df.head())

    # Check if DataFrame is empty before processing
    if df.empty:
        print("DataFrame is empty. Exiting.")
    else:
        df['dt'] = df['ts'].dt.date
        for date, grp in df.groupby('dt'):
            path = f"crypto-agent/data/raw/candles_1h/dt={date}/candles.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pq.write_table(pa.Table.from_pandas(grp.drop(columns='dt')), path)