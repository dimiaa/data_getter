import requests
import pandas as pd
import json
from datetime import datetime, timedelta

base_url = "https://api.binance.com/api/v3"
end_dt = datetime.today()
end_dt = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
start_dt = end_dt - timedelta(hours=16)

df_columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close',
              'volume', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume',
              'taker_buy_quote_asset_volume', 'ignore', 'open_timestamp', 'close_timestamp']


def get_historical_price(symbol: str, currency: str, start_dt: datetime, end_dt: datetime, interval: str):
    start_timestamp = round(start_dt.timestamp()) * 1000
    end_timestamp = round(end_dt.timestamp()) * 1000 - 1

    r = requests.get(
        f'{base_url}/klines?symbol={symbol}{currency}&interval={interval}&startTime={start_timestamp}&endTime={end_timestamp}&limit=1000')
    content = json.loads(r.content)

    if len(content) > 0:
        df = pd.DataFrame.from_records(content, columns=['open_timestamp', 'open', 'high', 'low', 'close', 'volume',
                                                         'close_timestamp', 'quote_asset_volume', 'num_trades',
                                                         'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                         'ignore'])
        df['open_time'] = df.open_timestamp.apply(lambda ts: datetime.fromtimestamp(ts / 1000))
        df['close_time'] = df.close_timestamp.apply(lambda ts: datetime.fromtimestamp(ts / 1000))
        return df[df_columns].sort_values('open_time', ascending=False)
    else:
        print('NO DATA RETRIEVED')
        print(f'RESPONSE: {content}')
        return None


START_DATE_LIMIT = datetime(2023, 5, 12)

TOKENS = ['ETH']

for SYMBOL in TOKENS:
    CURRENCY = 'USDT'
    print(f'[START] {SYMBOL}/{CURRENCY}')

    end_dt = datetime.today()
    start_dt = end_dt - timedelta(hours=16)

    print(f'{SYMBOL} Start Datetime: {start_dt} | End Datetime: {end_dt}')
    df = get_historical_price(SYMBOL, CURRENCY, start_dt, end_dt, "1m")

    reached_first_trading_day = False
    while START_DATE_LIMIT < start_dt and not reached_first_trading_day:
        end_dt = start_dt
        start_dt = end_dt - timedelta(hours=16)
        df_hp = get_historical_price(SYMBOL, CURRENCY, start_dt, end_dt, "1m")
        if df_hp is not None and len(df_hp.index) > 0:
            print(f'{SYMBOL} - {start_dt} - {end_dt} - RETRIEVED {len(df_hp.index)} ROWS')
            df = pd.concat([df, df_hp[df_columns]])
        else:
            print(f'{SYMBOL} - {start_dt} - STOPPING LOOP - NO DATA RETRIEVED')
            reached_first_trading_day = True

    print(f'[FINISHED] {SYMBOL} - {start_dt} - {end_dt} - SAVING {len(df.index)} ROWS')
    filename = f'{SYMBOL}_{CURRENCY}_{start_dt.year}_{str(start_dt.month).zfill(2)}_{str(start_dt.day).zfill(2)}_{end_dt.year}_{str(end_dt.month).zfill(2)}_{str(end_dt.day).zfill(2)}_{len(df.index)}.csv'
    df.to_csv(filename, index=False)
