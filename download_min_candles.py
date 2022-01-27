import requests
from pprint import pprint
import datetime
import time
import sqlite3
import pandas as pd
import pytz
from dateutil import parser

# setup
# отсчёт ведётся от настоящего времени (или времени БД) вниз. Когда скрипт достигнет этой даты, то он остановится
start_date = datetime.datetime(2017, 9, 1)
end_date = datetime.datetime.utcnow()
continue_download = False  # докачать ещё истории в существующую таблицу. Берёт самую раннюю дату из БД и качает до start_date
refresh_db = False  # обновляем БД до текущей даты. Не включай одновременно с continue download. Берёт самую последнюю дату из БД и качает до неё
base = 'algo'
quote = 'usdt'

# DB
db_name = "crypto_1min"
table_name = f"{base}_{quote}_1min".lower()
conn = sqlite3.connect(f"{db_name}.db")

# API
URL = "https://api.binance.com/"
# Лёшин API:
API_KEY = "30nrR1jAk1srMQiFWoMfLF0u2rIEiTsPrGJeBIM1Zp7RG8adsEJyBAlEu6Ckdkt3"
# # armvdata@gmail.com API key
# API_KEY = "3TSF53tKBGsfUs8ifKloRNN3FEIrwK1fsxW4IkkhS1xTG50kgScvony6DCf0WkWm"
HEADERS = {'X-MBX-APIKEY': API_KEY}
requests_per_min = 1100  # limit = 1200 - buffer

if continue_download:
    stmt = f"""
    SELECT time_open FROM {table_name} ORDER BY time_open ASC LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(stmt)
    t_open_from_db = cur.fetchall()[0][0]
    t_open_from_db = parser.parse(t_open_from_db)
    print("downloading before time_open: ", t_open_from_db)
    download_from = t_open_from_db - datetime.timedelta(seconds=1)
else:
    download_from = end_date
    print("downloading most recent candles")

if refresh_db:
    stmt = f"""
        SELECT time_open FROM {table_name} ORDER BY time_open DESC LIMIT 1
        """
    cur = conn.cursor()
    cur.execute(stmt)
    t_open_from_db = cur.fetchall()[0][0]
    t_open_from_db = parser.parse(t_open_from_db)
    start_date = t_open_from_db
else:
    start_date = start_date

print("downloading to time_open: ", start_date)


def transform_list_of_lists_into_list_of_dicts(list_of_lists: list, keys: list):
    """
    Превращает list of lists в list of dicts
    :param list_of_lists: то, что необходимо трансформировать
    :param keys: ключи для новых dictionaries
    :return: list of dicts
    """
    return [dict(zip(keys, list_item)) for list_item in list_of_lists]

def download_min_candles(asset_id_base,
                         asset_id_quote,
                         time_start=None,
                         time_end=None,
                         limit=1000):
    symbol_id = f"{asset_id_base}{asset_id_quote}".upper()
    period_id = "1m"
    if time_start:
        time_start = pytz.utc.localize(time_start)
        time_start = int(datetime.datetime.timestamp(time_start) * 1000)  # Умножаю на 1000, т.к. бинанс работает с timestamp в милисекундах
    if time_end:
        time_end = pytz.utc.localize(time_end)
        time_end = int(datetime.datetime.timestamp(time_end) * 1000)

    params = {
        'symbol': symbol_id,
        'interval': period_id,
        'limit': limit,
        'startTime': time_start,
        'endTime': time_end
    }

    specific_url = URL + 'api/v3/klines'
    response = requests.get(specific_url, headers=HEADERS, params=params)
    headers, api_response = response.headers, response.json()

    keys = [
        'time_open',
        'price_open',
        'price_high',
        'price_low',
        'price_close',
        'volume_traded',
        'time_close',
        'quote_asset_volume',
        'trades_count',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]

    dicts_from_api_response = transform_list_of_lists_into_list_of_dicts(api_response, keys)

    for d in dicts_from_api_response:
        d['time_open'] = datetime.datetime.fromtimestamp(d['time_open'] / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)  # Делим на 1000, т.к. binance API выдаёт результат в милисекундах
        d['price_open'] = float(d['price_open'])
        d['price_high'] = float(d['price_high'])
        d['price_low'] = float(d['price_low'])
        d['price_close'] = float(d['price_close'])
        d['volume_traded'] = float(d['volume_traded'])
        d['time_close'] = datetime.datetime.fromtimestamp(d['time_close'] / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)  # cтандартизируем время на utc, .replace(tzinfo=None) убирает инфу о timezone
        d['quote_asset_volume'] = float(d['quote_asset_volume'])
        d['trades_count'] = int(d['trades_count'])
        d['taker_buy_base_asset_volume'] = float(d['taker_buy_base_asset_volume'])
        d['taker_buy_quote_asset_volume'] = float(d['taker_buy_quote_asset_volume'])

    return headers, dicts_from_api_response

headers, ohlcv = download_min_candles(base, quote, time_end=download_from, limit=1000)
used_limit = int(headers['x-mbx-used-weight-1m'])
oldest_time_open = ohlcv[0]['time_open'] - datetime.timedelta(seconds=1)

while True:
    if used_limit >= requests_per_min:
        # Ждём окончания минуты, чтобы стартануть заново
        curr_time = datetime.datetime.utcnow()
        wait_until = curr_time + (datetime.datetime.min - curr_time) % datetime.timedelta(minutes=1)
        print('used limit, waiting')
        time.sleep(max(0, (wait_until - curr_time).total_seconds()))

    df = pd.DataFrame(ohlcv)
    df.to_sql(table_name, conn, if_exists="append", index=False)

    try:
        oldest_time_open = ohlcv[0]['time_open']
        print(oldest_time_open, " - downloaded;", "used limit: ", used_limit)
    except IndexError:
        print('index out of range => history finished before start_date')
        print('Ending download')
        break


    if oldest_time_open <= start_date:
        break

    oldest_time_open = ohlcv[0]['time_open'] - datetime.timedelta(seconds=1)
    while True:
        try:
            headers, ohlcv = download_min_candles(base, quote, time_end=oldest_time_open, limit=1000)
            used_limit = int(headers['x-mbx-used-weight-1m'])
        except:
            print('failed to connect, sleep 1 sec')
            time.sleep(1)
            continue
        break

# Это тут так на всякий случай, если вдруг будешь обновлять таблицу, то нужно будет потом дубликаты удалить
print('Deleting duplicate entries')
delete_duplicates = f"""
DELETE FROM {table_name} WHERE rowid NOT IN (SELECT min(rowid) FROM {table_name} GROUP BY time_open)
"""
cur = conn.cursor()
cur.execute(delete_duplicates)
conn.commit()

conn.close()
print('Done')
