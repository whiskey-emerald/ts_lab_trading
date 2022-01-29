coinapi_to_pandas_freq = {
    "SEC": 'S',
    "MIN": 'min',
    "HRS": 'H',
    "DAY": 'D',
    "WKS": 'W-MON',  # Чтобы неделя начиналась с пнд
    "MTH": 'MS',  # Чтобы считали со старта месяца
    "YRS": 'YS'  # Чтобы считали со старта года
}

def convert_coinapi_candle_format_to_pandas_freq(period_id: str):
    period_id = period_id.upper()  # стандартизация написания
    time_period = period_id[-3:]  # берём последние 3 буквы
    duration = period_id[:len(period_id) - 3]  # берём цифры
    period_id = f"{duration}{coinapi_to_pandas_freq[time_period]}"
    return period_id