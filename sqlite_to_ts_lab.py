import datetime
import sqlite3
import pandas as pd
import numpy as np

# SETUP
start_date = datetime.datetime(2019, 1, 7)  # None => качает с самого начала, дата => качает с даты включительно
end_date = None  # None => качает до конца, дата => качает вплоть до даты (end_date = time_close)
base = 'btc'
quote = 'usdt'

db_name = "crypto_1min"
table_name = f"{base}_{quote}_1min"
conn = sqlite3.connect(f"{db_name}.db")

# Создаём условие по времени скачивания
if start_date or end_date:
    if start_date:
        start_cond = f"time_open >= '{start_date}'"
    else:
        start_cond = ""

    if end_date:
        end_cond = f"time_open < '{end_date}'"
    else:
        end_cond = ""

    if start_date and end_date:
        time_cond = f"WHERE {start_cond} AND {end_cond}"
    else:
        time_cond = f"WHERE {start_cond}{end_cond}"
else:
    time_cond = ""

select_stmt = f"""
SELECT * FROM {table_name} {time_cond} ORDER BY time_open ASC
"""

print('Reading SQL...')
table = pd.read_sql(select_stmt, conn)
table = table.astype({'time_open': 'datetime64', 'time_close': 'datetime64'})

print('Transforming data into TS-lab format...')
tslab_table = pd.DataFrame(index=table.index)
tslab_table['<TICKER>'] = f"{base}_{quote}".upper()
tslab_table['<PER>'] = 1
tslab_table['<DATE>'] = table['time_open'].apply(lambda x: x.strftime('%Y%m%d'))
tslab_table['<TIME>'] = table['time_open'].apply(lambda x: x.strftime('%H%M%S'))
tslab_table['<OPEN>'] = table['price_open']
tslab_table['<LOW>'] = table['price_low']
tslab_table['<HIGH>'] = table['price_high']
tslab_table['<CLOSE>'] = table['price_close']
tslab_table['<VOL>'] = table['volume_traded']

print('Writing to .txt...')
start_time = table['time_open'].min().strftime('%Y%m%d_%H%M%S')
end_time = table['time_close'].max().strftime('%Y%m%d_%H%M%S')
filename = f"{base}_{quote}_1m_{start_time}-{end_time}.txt"
tslab_table.to_csv(filename, index=False)


conn.close()
print(f"Created {filename}")