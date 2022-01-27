from rest_api_handlers import BinanceRestAPI
import requests
from pprint import pprint
import datetime
import time
import sqlite3
import pandas as pd

# setup
# отсчёт ведётся от настоящего времени (или времени БД) вниз. Когда скрипт достигнет этой даты, то он остановится
start_date = datetime.datetime(2018, 1, 1)
base = 'eth'
quote = 'usdt'
continue_download = False  # поставить True, если хотим продолжить грузить данные. Например, год скачал, хочешь ещё год скачать

# DB
db_name = "crypto_trades_agg"
table_name = f"{base}_{quote}".lower()
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
    SELECT id FROM {table_name} ORDER BY id ASC LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(stmt)
    oldest_id = cur.fetchall()[0][0]
    print("downloading before id: ", oldest_id)
    download_from_id = oldest_id - 1000  # минус 1000, т.к. наш лимит запроса
else:
    download_from_id = None
    print("downloading most recent trades")


def get_agg_trades(asset_id_base: str = None, asset_id_quote: str = None, limit: int = 1000, from_id: int = None):
    # NB! У этого запроса вес = 5. Лимит - 1200/мин
    symbol_id = f"{asset_id_base}{asset_id_quote}".upper()
    url_cont = 'api/v3/aggTrades'
    params = {
        'symbol': symbol_id,
        'limit': limit,
        'fromId': from_id
    }

    specific_url = URL + url_cont
    response = requests.get(specific_url, headers=HEADERS, params=params)
    return response.headers, response.json()


headers, trades = get_agg_trades(base, quote, limit=1000, from_id=download_from_id)
used_limit = int(headers['x-mbx-used-weight-1m'])
# pprint(trades)

while True:
    if used_limit >= requests_per_min:
        # Ждём окончания минуты, чтобы стартануть заново
        curr_time = datetime.datetime.utcnow()
        wait_until = curr_time + (datetime.datetime.min - curr_time) % datetime.timedelta(minutes=1)
        print('used limit, waiting')
        time.sleep(max(0, (wait_until - curr_time).total_seconds()))

    for trade in trades:  # конвертируем timestamp в datetime
        trade['T'] = datetime.datetime.fromtimestamp(trade['T'] / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)  # Делим на 1000, т.к. binance API выдаёт результат в милисекундах
        trade['id'] = trade.pop('a', None)
        trade['price'] = trade.pop('p', None)
        trade['quantity'] = trade.pop('q', None)
        trade['first_trade_id'] = trade.pop('f', None)
        trade['last_trade_id'] = trade.pop('l', None)
        trade['time'] = trade.pop('T', None)
        trade['isBuyerMaker'] = trade.pop('m', None)
        trade['isBestMatch'] = trade.pop('M', None)

    df = pd.DataFrame(trades)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(trades[0]['time'], " - downloaded;", "used limit: ", used_limit)

    if trades[0]['time'] < start_date:
        break

    oldest_id = trades[0]['id']
    while True:
        try:
            headers, trades = get_agg_trades(base, quote, limit=1000, from_id=(oldest_id - 1000))
            used_limit = int(headers['x-mbx-used-weight-1m'])
        except:
            print('failed to connect, sleep 1 sec')
            time.sleep(1)
            continue
        break

# Это тут так на всякий случай, если вдруг будешь обновлять таблицу, то нужно будет потом дубликаты удалить
print('Deleting duplicate entries')
delete_duplicates = f"""
DELETE FROM {table_name} WHERE rowid NOT IN (SELECT min(rowid) FROM {table_name} GROUP BY id)
"""
cur = conn.cursor()
cur.execute(delete_duplicates)
conn.commit()

conn.close()
