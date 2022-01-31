import pandas as pd
import numpy as np
import datetime
from strat_analytics.helper_funcs import convert_coinapi_candle_format_to_pandas_freq, convert_coinapi_to_pd_timedelta
from pandas import Timedelta


class EquityCurve:
    def __init__(self, csv_file_path, remove_fictitious_trades=True, override_strategy_speed=False):
        # Достаём данные из тс-лаба
        self.ts_lab_data = self.parse_ts_lab_data(csv_file_path)

        # Определяем размер свеч в наших стратегиях
        if not override_strategy_speed:
            self.strategy_speed = self.infer_strategy_speed()
        else:
            self.strategy_speed = override_strategy_speed

        # self.underlying_assets_data - dict, где мы держим датафреймы
        # self.filename_to_ticker_dict - dict с парой название файла - тикер
        # Файлы, которые использовались для стратегии, должны лежать в той же папке, откуда вызывается скрипт
        self.underlying_assets_data, self.filename_to_ticker_dict = self.get_underlying_asset_data()

        # Узнаем период нашего бэктеста, т.к., скорее всего, мы использовали не все данные из файла
        self.strategy_start_date, self.strategy_end_date = self.get_strategy_start_end_time()
        # трансформируем данные по активу в свечи нужного размера
        for ticker, asset_data in self.underlying_assets_data.items():
            self.underlying_assets_data[ticker] = self.transform_candle_size(candles_to_be_converted=self.underlying_assets_data[ticker],
                                                                             target_candle_size=self.strategy_speed,
                                                                             candle_origin=self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull(), "Дата входа"].iloc[0],
                                                                             start_time=self.strategy_start_date,
                                                                             end_time=self.strategy_end_date)

        # Трансформируем данные тс-лаба в конкретные сделки
        if remove_fictitious_trades:
            self.list_of_fict_signals = self.get_fictitious_signals()
        self.trades = self.extract_trades(remove_fictitious_trades)

        self.cumulative_pos = self.calculate_cumulative_pos()

    def parse_ts_lab_data(self, csv_file_path):
        """
        Обрабатывает файл от тс-лаба и приводит его в удобоваримый формат
        :param csv_file_path: путь к csv файлу
        :return: DataFrame с обработанными данными:
        columns = ["№ Позиции",
                   "Позиция",
                   "Символ",
                   "Лоты",
                   "Изменение/Максимум Лотов",
                   "Исполнение входа",
                   "Сигнал входа",
                   "Бар входа",
                   "Дата входа",
                   "Цена входа",
                   "Комиссия входа",
                   "Исполнение выхода",
                   "Сигнал выхода",
                   "Бар выхода",
                   "Дата выхода",
                   "Цена выхода",
                   "Комиссия выхода",
                   "Средневзвешенная цена входа",
                   "П/У",
                   "П/У сделки",
                   "П/У с одного лота",
                   "Зафиксированная П/У",
                   "Открытая П/У",
                   "Продолж. (баров)",
                   "Доход/Бар",
                   "Общий П/У",
                   "% изменения",
                   "MAE",
                   "MAE %",
                   "MFE",
                   "MFE %"]
        """
        # TODO создать уникальные ID для позиций?
        ts_lab_data = pd.read_csv(csv_file_path, sep=',')
        # Если другой сепаратор
        if len(ts_lab_data.columns) == 1:
            ts_lab_data = pd.read_csv(csv_file_path, sep=';')

        # У тс-лаба зачем-то в числах есть пробелы
        ts_lab_data = ts_lab_data.astype(str)
        for column in ts_lab_data:
            ts_lab_data[column] = ts_lab_data[column].str.replace(" ", "")

        ts_lab_data.replace('nan', np.nan, inplace=True)

        # Зачем тут эта строчка кода? Что она делает?
        # В общем, у тс-лаба есть такая фича, где если у тебя есть открытая позиция с несколькими изменениями по позе,
        # то тс-лаб в конце додавляет как бы сделку, которая должна была бы произойти, чтобы закрыт позицию, но которой не было
        # Мы удалеяем это строчку, т.к. сделки по позиции у нас нет
        # Тем более, что тс-лаб по итогу цену берёт не из последнего бара, а из последней сделки, что делает эту фичу бесполезной
        ts_lab_data = ts_lab_data.loc[~((ts_lab_data["Дата выхода"] == "Открыта") & (ts_lab_data["Дата входа"].isnull()))]

        # Объединяем даты, чтобы потом можно было с ними арифметику делать
        ts_lab_data['Дата входа'] = ts_lab_data['Дата входа'] + " " + ts_lab_data['Время входа']
        ts_lab_data['Дата выхода'] = ts_lab_data['Дата выхода'] + " " + ts_lab_data['Время выхода']

        ts_lab_data.drop(columns=['Время входа', 'Время выхода'], inplace=True)
        ts_lab_data.rename(columns={"Unnamed: 0": "№ Позиции"}, inplace=True)

        # добавляем это, чтобы корректно трансформировать колонку
        ts_lab_data["№ Позиции"].replace("-", 0, inplace=True)

        # Мы там раньше превратили всё в string, а для открытых позиций надо nan ставить
        ts_lab_data.loc[ts_lab_data['Дата выхода'] == "Открыта nan",
                        ['Исполнение выхода',
                         'Сигнал выхода',
                         "Бар выхода",
                         "Дата выхода",
                         "Цена выхода",
                         "Комиссия выхода",
                         "П/У сделки",
                         "Зафиксированная П/У"]] = np.nan

        ts_lab_data.loc[ts_lab_data['Дата выхода'] == "Открыта nan", ["Дата выхода"]] = "Открыта"

        # pandas некорректно распозновал даты в некоторых случаях и путал месяц с днём. Так точно всё корректно
        ts_lab_data["Дата входа"] = pd.to_datetime(ts_lab_data["Дата входа"], dayfirst=True)
        ts_lab_data["Дата выхода"] = pd.to_datetime(ts_lab_data["Дата выхода"], dayfirst=True)

        ts_lab_data = ts_lab_data.astype({"№ Позиции": "float64",
                                          "Позиция": "string",
                                          "Символ": "string",
                                          "Лоты": "float64",
                                          "Изменение/Максимум Лотов": "float64",
                                          "Исполнение входа": "string",
                                          "Сигнал входа": "string",
                                          "Бар входа": "float64",
                                          "Дата входа": "datetime64",
                                          "Цена входа": "float64",
                                          "Комиссия входа": "float64",
                                          "Исполнение выхода": "string",
                                          "Сигнал выхода": "string",
                                          "Бар выхода": "float64",  # Тут флоут вместо инт, т.к. иначе nan нельзя использовать
                                          "Дата выхода": "datetime64",
                                          "Цена выхода": "float64",
                                          "Комиссия выхода": "float64",
                                          "Средневзвешенная цена входа": "float64",
                                          "П/У": "float64",
                                          "П/У сделки": "float64",
                                          "П/У с одного лота": "float64",
                                          "Зафиксированная П/У": "float64",
                                          "Открытая П/У": "float64",
                                          "Продолж. (баров)": "float64",
                                          "Доход/Бар": "float64",
                                          "Общий П/У": "float64",
                                          # "% изменения": "float64",  # удалено, т.к. мы с этим разбираемся отдельно
                                          "MAE": "float64",
                                          # "MAE %": "float64",  # удалено, т.к. мы с этим разбираемся отдельно
                                          "MFE": "float64",
                                          # "MFE %": "float64",  # удалено, т.к. мы с этим разбираемся отдельно
                                          })

        # Конвертируем проценты в числа
        ts_lab_data["% изменения"] = ts_lab_data["% изменения"].str.rstrip('%').astype('float') / 100.0
        ts_lab_data["MAE %"] = ts_lab_data["MAE %"].str.rstrip('%').astype('float') / 100.0
        ts_lab_data["MFE %"] = ts_lab_data["MFE %"].str.rstrip('%').astype('float') / 100.0

        num_of_pos = len(ts_lab_data.loc[~ts_lab_data["№ Позиции"].isnull()])
        ts_lab_data.loc[~ts_lab_data["№ Позиции"].isnull(), ["№ Позиции"]] = np.arange(num_of_pos) + 1  # +1 чтобы мы начинали с 1
        ts_lab_data["№ Позиции"].fillna(method="ffill", inplace=True)

        return ts_lab_data

    def infer_strategy_speed(self):
        """
        Определеяет размер свечи, который используется стратегией.
        :return: string с размером свечи в формате CoinAPI: 1SEC, 30SEC, 1MIN, 1HRS, 1DAY, 1WKS, 1MTH и т.д.
        """
        # Берём первый вход в какую-либо позицию
        enter_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull()].iloc[0]
        # Берём первый выход в какую-либо позицию
        exit_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата выхода"].isnull()].iloc[0]
        # Добавляю на всякий случай, если вдруг у нас несколько инструментов и время совпадает
        if enter_pos['Бар входа'] == exit_pos['Бар выхода']:
            exit_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата выхода"].isnull()].iloc[1]
        # Берём разницу во времени и в барах. На выходе получаем размер одной свечи
        strategy_speed = self.get_candle_size(
            date_1=enter_pos['Дата входа'],
            date_2=exit_pos['Дата выхода'],
            bar_diff=(exit_pos['Бар выхода'] - enter_pos['Бар входа'])
        )
        return strategy_speed

    def get_candle_size(self, date_1: pd.Timestamp, date_2: pd.Timestamp, bar_diff: int):
        """
        Определяет размер свечи.
        Метод определяет разницу между двумя датами сделок.
        Далее делит эту разницу на разницу в барах между двумя сделками.
        DISCLAIMER:
        Для свечей >= 60 мин, но < 1 дня доступны только часовые свечи. Свечи размером 70 мин, 80 мин, 90 мин и т.д.
        не будут корректно распознаны.
        Теоретически 30-дневные или другие примерно месячные свечи могут быть некорректно распознаны как месячные свечи.
        Сейчас метод определяет, что свеча месячная тем, что те даты, между которыми высчитывается разница, - это первые
        дни месяца. Естественно, случайно такое может совпасть и с 30-дневными свечами
        :param date_1: более ранняя дата
        :param date_2: более поздняя дата
        :param bar_diff: разница между барами, т.е. количество свечей между двумя датами
        :return: string с размером свечи в формате CoinAPI: 1SEC, 30SEC, 1MIN, 1HRS, 1DAY, 1WKS, 1MTH и т.д.
        """
        time_diff = date_2 - date_1
        candle_timedelta = time_diff / bar_diff

        if candle_timedelta.days == 0:  # свечка меньше 1 дня
            if candle_timedelta.seconds >= 3600:
                hrs_in_candle = int(candle_timedelta.seconds / 3600)
                candle_size = f"{hrs_in_candle}HRS"
            elif candle_timedelta.seconds >= 60:
                min_in_candle = int(candle_timedelta.seconds / 60)
                candle_size = f"{min_in_candle}MIN"
            else:
                candle_size = f"{candle_timedelta.seconds}SEC"
        else:  # Для свечей больше 1 дня лучше использовать override
            if candle_timedelta.days >= 28:
                if date_1.day == 1 and date_2.day == 1:
                    # Я понимаю, что это не супер надёжный способ проверки, но пока что так сойдёт
                    # TODO придумать понадёжнее способ проверять, что свечки месячные
                    month_diff = (date_2.year - date_1.year) * 12 + date_2.month - date_1.month
                    months_in_candle = int(month_diff / bar_diff)
                    candle_size = f"{months_in_candle}MTH"
                else:
                    candle_size = f"{candle_timedelta.days}DAY"
            else:
                if candle_timedelta.days % 7 == 0:
                    weeks_in_candle = int((candle_timedelta.days / 7) / bar_diff)
                    candle_size = f"{weeks_in_candle}WKS"
                else:
                    candle_size = f"{candle_timedelta.days}DAY"
        return candle_size

    def get_underlying_asset_data(self):
        """
        Достаёт данные по активу из файлов, которые использовались для создания стратегии.
        Из файла ts_lab_data из колонки Символ достаёт все уникальные наименование.
        Далее метод ищет файлы с этим названием и расширением .txt в папке, откуда мы вызываем скрипт.
        ФАЙЛ ОБЯЗАТЕЛЬНО ДОЛЖЕН ЛЕЖАТЬ В ТОЙ ЖЕ ПАПКЕ И ИМЕТЬ ТО ЖЕ НАЗВАНИЕ, ЧТО И В СТРАТЕГИИ
        Формат файла:
        <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<LOW>,<HIGH>,<CLOSE>,<VOL>

        Также создаёт dict с мэппингом названия файла к тикеру.
        :return:
        1. Dict c датафреймами со свечными данными по торгуемым активам
        Формат dict: {ticker: DataFrame, ticker: DataFrame, ... }
        Формат датафрейма:
        <DATE>, <OPEN>, <LOW>, <HIGH>, <CLOSE>, <VOL>
        2. dict с мэппингом названия файла к тикеру.
        Формат: {наименование файла: тикер, ...}
        Пример:
        {'eth_usdt_1m_20170831_142300-20220107_122459': 'ETH_USDT',
        'btc_usdt_1m_20170831_140000-20220107_120059': 'BTC_USDT'}
        """
        file_names = self.ts_lab_data["Символ"].unique()
        ohlcv_dict = {}  # dict, где мы держим датафреймы
        filename_to_ticker_dict = {}  # dict с mapping тикеров и названий файлов
        for file_name in file_names:
            ohlcv = pd.read_csv(f"{file_name}.txt")
            ohlcv = ohlcv.astype({
                "<TICKER>": "string",
                "<DATE>": "string",
                "<TIME>": "string",
            })

            ohlcv["<TIME>"] = ohlcv["<TIME>"].apply(lambda x: x.zfill(6))
            ohlcv["<DATE>"] = ohlcv["<DATE>"] + " " + ohlcv["<TIME>"]
            ohlcv["<DATE>"] = pd.to_datetime(ohlcv["<DATE>"])
            ticker = ohlcv["<TICKER>"][0]
            ohlcv.drop(columns=["<TICKER>", "<TIME>", "<PER>"], inplace=True)
            filename_to_ticker_dict[file_name] = ticker
            ohlcv_dict[ticker] = ohlcv

        return ohlcv_dict, filename_to_ticker_dict

    def get_strategy_start_end_time(self, extend_last_date_by_x_candles: int = 0):
        """
        Возвращает pd.Timestamp со стартом и концом периода бэктеста
        first_date - дата первой сделки минус кол-во баров, которое прошло до первой сделки
        last_date - дата последней сделки + одна свеча
        :param extend_last_date_by_x_candles: удлинить последнюю дату на Х свечей
        :return:
        first_date - дата начала бэктеста
        last_date - дата конца бэктеста
        """
        first_date = self.ts_lab_data[["Дата входа", "Дата выхода"]].min().min()  # два раза мин, потому что выдаёт два результата: по одному на колонку
        last_date = self.ts_lab_data[["Дата входа", "Дата выхода"]].max().max()
        first_bar = self.ts_lab_data[["Бар входа", "Бар выхода"]].min().min()

        time_delta = convert_coinapi_to_pd_timedelta(self.strategy_speed)

        first_date -= first_bar * time_delta
        last_date += time_delta * (1 + extend_last_date_by_x_candles)
        return first_date, last_date

    def transform_candle_size(self,
                              candles_to_be_converted: pd.DataFrame,
                              target_candle_size: str,
                              candle_origin: pd.Timestamp,
                              start_time: pd.Timestamp = None,
                              end_time: pd.Timestamp = None
                              ):
        """
        Преобразует свечи из одного размера в другой.
        :param candles_to_be_converted: Dataframe. Маленькие свечи, которые нужно увеличить.
        Формат датафрейма:
        <DATE>, <OPEN>, <LOW>, <HIGH>, <CLOSE>, <VOL>
        Где <DATE> - время открытия свечи
        :param target_candle_size: какой размер свечи нужен.
        Принимает формат CoinAPI: 1SEC, 30SEC, 1MIN, 1HRS, 1DAY, 1WKS, 1MTH и т.д.
        :param candle_origin: точка отсчёта свечей. Обязательно должна совпадать с точкой отсчёта в нашей стратегии.
        Это некий якорь, вокруг которого центруются свечи. В качестве такого можно использовать любую дату из файла тс-лаб
        :param start_time: с какого момента нужна история свечей. Свечи будут возвращаться от включительно
        :param end_time: до какого момента нужна история свечей. Свечи будут возвращаться до этой даты, не включая её
        :return: DataFrame с укрупнёнными свечами в формате
        <DATE>, <OPEN>, <LOW>, <HIGH>, <CLOSE>, <VOL>
        """
        if start_time:
            candles_to_be_converted = candles_to_be_converted.loc[candles_to_be_converted["<DATE>"] >= start_time]
        if end_time:
            candles_to_be_converted = candles_to_be_converted.loc[candles_to_be_converted["<DATE>"] < end_time]

        pandas_freq = convert_coinapi_candle_format_to_pandas_freq(target_candle_size)
        candles_to_be_converted = candles_to_be_converted.resample(pandas_freq, on='<DATE>', closed='left', origin=candle_origin).agg({'<DATE>': 'min',
                                                                                                                                       '<OPEN>': 'first',
                                                                                                                                       '<HIGH>': 'max',
                                                                                                                                       '<LOW>': 'min',
                                                                                                                                       '<CLOSE>': 'last',
                                                                                                                                       '<VOL>': 'sum'})
        candles_to_be_converted.reset_index(inplace=True, drop=True)
        return candles_to_be_converted

    def get_fictitious_signals(self):
        """
        Берёт из ts_lab_data названия сигналов, которые помечены как фиктивные
        :return:
        """
        fict_exits = self.ts_lab_data.loc[self.ts_lab_data['Исполнение входа'] == 'Фиктивное', 'Сигнал входа'].unique()
        fict_enters = self.ts_lab_data.loc[self.ts_lab_data['Исполнение выхода'] == 'Фиктивное', 'Сигнал выхода'].unique()
        fict_signals = np.concatenate((fict_enters, fict_exits)).tolist()
        return fict_signals

    def extract_trades(self, remove_fictitious_trades=True):
        """

        :param remove_fictitious_trades: убирать ли фиктивные сделки
        :return: DataFrame со сделками:
        ,Символ,Кол-во,Бар,Дата,Цена,Комиссия
        """
        # Берём строки с входами и выходами
        # Тут строка, где вход и выход - это одна строчка. Тут нужна отдельная логика
        # Дело в том, что когда
        enter_and_exit_combined = self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull() & ~self.ts_lab_data["Дата выхода"].isnull()]
        # Тут строки, где вход/выход в позицию - это отдельные строки, которые соответствуют сделкам
        enter_pos_df = self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull() & self.ts_lab_data["Дата выхода"].isnull()]
        exit_pos_df = self.ts_lab_data.loc[self.ts_lab_data["Дата входа"].isnull() & ~self.ts_lab_data["Дата выхода"].isnull()]

        # Почему тут Лоты, а ниже Изменение/Максимум Лотов?
        # Потому что в датафреймах, где отображен только вход или выход, "Изменение/Максимум Лотов" показывает направление
        # конкретных сделок. В строках, где вход и выход вместе, это так не работает, к сожалению.
        # Тут объём и направление сделки - это лот. Для выхода из позиции знак Лота нужно перевернуть
        # Кстати, мы тут делаем .copy(), чтобы pandas не жаловался
        enter_pos_df_from_combined = enter_and_exit_combined[["Символ",
                                                              "Лоты",
                                                              "Сигнал входа",
                                                              "Бар входа",
                                                              "Дата входа",
                                                              "Цена входа",
                                                              "Комиссия входа"]].copy()

        exit_pos_df_from_combined = enter_and_exit_combined[["Символ",
                                                             "Лоты",
                                                             "Сигнал выхода",
                                                             "Бар выхода",
                                                             "Дата выхода",
                                                             "Цена выхода",
                                                             "Комиссия выхода"]].copy()
        # Для выходов, надо перевернуть знаки, чтобы был выход из сделки
        exit_pos_df_from_combined.loc[:, "Лоты"] *= -1

        # Убираем лишние строки и стандартизируем датафреймы
        enter_pos_df = enter_pos_df[["Символ",
                                     "Изменение/Максимум Лотов",
                                     "Сигнал входа",
                                     "Бар входа",
                                     "Дата входа",
                                     "Цена входа",
                                     "Комиссия входа"]]

        exit_pos_df = exit_pos_df[["Символ",
                                   "Изменение/Максимум Лотов",
                                   "Сигнал выхода",
                                   "Бар выхода",
                                   "Дата выхода",
                                   "Цена выхода",
                                   "Комиссия выхода"]]

        # Объединяем датафреймы в список сделок
        enter_pos_df.columns = exit_pos_df.columns
        enter_pos_df_from_combined.columns = exit_pos_df.columns
        exit_pos_df_from_combined.columns = exit_pos_df.columns
        trades = pd.concat([enter_pos_df, exit_pos_df, enter_pos_df_from_combined, exit_pos_df_from_combined])

        trades.rename(columns={"Символ": "Тикер",
                               "Изменение/Максимум Лотов": "Кол-во",
                               "Сигнал выхода": "Сигнал",
                               "Бар выхода": "Бар",
                               "Дата выхода": "Дата",
                               "Цена выхода": "Цена",
                               "Комиссия выхода": "Комиссия"
                               }, inplace=True)

        # Удаляем фиктивные сделки
        if remove_fictitious_trades:
            trades = trades[~trades["Сигнал"].isin(self.list_of_fict_signals)]
        trades.drop(["Сигнал"], axis=1, inplace=True)

        # Меняем названия файлов на более понятные тикеры
        trades.replace({"Тикер": self.filename_to_ticker_dict}, inplace=True)

        trades.sort_values(by="Дата", ascending=True, inplace=True)
        trades.reset_index(inplace=True, drop=True)

        trades = trades[["Тикер", "Дата", "Бар", "Кол-во", "Цена", "Комиссия"]]
        # Зачем я переименовываю колонки? Я заебался переключать раскладку клавиатуры
        trades = trades.rename(columns={"Тикер": "<TICKER>",
                                        "Дата": "<DATE>",
                                        "Бар": "<BAR>",
                                        "Кол-во": "<AMOUNT>",
                                        "Цена": "<PRICE>",
                                        "Комиссия": "<COMMISSION>"})

        return trades

    def calculate_cumulative_pos(self):
        trading_instruments = self.trades
