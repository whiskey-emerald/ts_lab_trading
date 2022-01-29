import pandas as pd

coinapi_to_pandas_freq = {
    "SEC": 'S',
    "MIN": 'min',
    "HRS": 'H',
    "DAY": 'D',
    "WKS": 'W-MON',  # Чтобы неделя начиналась с пнд
    "MTH": 'MS',  # Чтобы считали со старта месяца
    "YRS": 'YS'  # Чтобы считали со старта года
}


def extract_duration_and_timeperiod(period_id: str):
    period_id = period_id.upper()  # стандартизация написания
    time_period = period_id[-3:]  # берём последние 3 буквы
    duration = period_id[:len(period_id) - 3]  # берём цифры
    return int(duration), time_period


def convert_coinapi_candle_format_to_pandas_freq(period_id: str):
    duration, time_period = extract_duration_and_timeperiod(period_id)
    period_id = f"{duration}{coinapi_to_pandas_freq[time_period]}"
    return period_id

coinapi_to_timedelta = {
    "SEC": 'sec',
    "MIN": 'min',
    "HRS": 'hr',
    "DAY": 'day',
    "WKS": 'W',
    # НЕ РАБОТАЕТ С МЕСЯЦАМИ!
}

def convert_coinapi_to_pd_timedelta(period_id: str):
    duration, time_period = extract_duration_and_timeperiod(period_id)
    timedelta = pd.Timedelta(duration, coinapi_to_timedelta[time_period])
    return timedelta