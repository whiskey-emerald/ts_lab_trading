import pandas as pd
import numpy as np


# #Список колонок из csv на всякий случай тут кладу
# columns = ["№ Позиции",
#            "Позиция",
#            "Символ",
#            "Лоты",
#            "Изменение/Максимум Лотов",
#            "Исполнение входа",
#            "Сигнал входа",
#            "Бар входа",
#            "Дата входа",
#            "Цена входа",
#            "Комиссия входа",
#            "Исполнение выхода",
#            "Сигнал выхода",
#            "Бар выхода",
#            "Дата выхода",
#            "Цена выхода",
#            "Комиссия выхода",
#            "Средневзвешенная цена входа",
#            "П/У",
#            "П/У сделки",
#            "П/У с одного лота",
#            "Зафиксированная П/У",
#            "Открытая П/У",
#            "Продолж. (баров)",
#            "Доход/Бар",
#            "Общий П/У",
#            "% изменения",
#            "MAE",
#            "MAE %",
#            "MFE",
#            "MFE %"]

class EquityCurve:
    def __init__(self, csv_file_path, remove_fictitious_trades=True, override_strategy_speed=False):
        self.ts_lab_data = self.parse_ts_lab_data(csv_file_path)

        if not override_strategy_speed:
            self.strategy_speed = self.get_strategy_speed()
        else:
            self.strategy_speed = override_strategy_speed

        if remove_fictitious_trades:
            self.list_of_fict_signals = self.get_fictituous_signals()
        self.trades = self.extract_trades(remove_fictitious_trades)

    def parse_ts_lab_data(self, csv_file_path):
        """
        Обрабатывает файл от тс-лаба и приводит его в удобоваримый формат
        :param csv_file_path: путь к csv файлу
        :return: DataFrame с обработанными данными
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

    def get_fictituous_signals(self):
        """
        Берёт из ts_lab_data названия сигналов, которые помечены как фиктивные
        :return:
        """
        fict_exits = self.ts_lab_data.loc[self.ts_lab_data['Исполнение входа'] == 'Фиктивное', 'Сигнал входа'].unique()
        fict_enters = self.ts_lab_data.loc[self.ts_lab_data['Исполнение выхода'] == 'Фиктивное', 'Сигнал выхода'].unique()
        fict_signals = np.concatenate((fict_enters, fict_exits)).tolist()
        return fict_signals

    def extract_trades(self, remove_fictitious_trades=True):
        # Берём строки с входами и выходами
        enter_pos_df = self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull()]
        exit_pos_df = self.ts_lab_data.loc[~self.ts_lab_data["Дата выхода"].isnull()]

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
        trades = pd.concat([enter_pos_df, exit_pos_df])

        trades.rename(columns={"Изменение/Максимум Лотов": "Кол-во",
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

        return trades

    def get_strategy_speed(self):
        # Берём первый вход в какую-либо позицию
        enter_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата входа"].isnull()].iloc[0]
        # Берём первый выход в какую-либо позицию
        exit_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата выхода"].isnull()].iloc[0]
        # Добавляю на всякий случай, если вдруг у нас несколько инструментов и время совпадает
        if enter_pos['Бар входа'] == exit_pos['Бар выхода']:
            exit_pos = self.ts_lab_data.loc[~self.ts_lab_data["Дата выхода"].isnull()].iloc[1]
        #Берём разницу во времени и в барах. На выходе получаем размер одной свечи
        time_diff = exit_pos['Дата выхода'] - enter_pos['Дата входа']
        bar_diff = exit_pos['Бар выхода'] - enter_pos['Бар входа']
        candle_size = time_diff / bar_diff

        if candle_size.days == 0:  # свечка меньше 1 дня
            if candle_size.seconds >= 3600:
                hrs_in_candle = int(candle_size.seconds / 3600)
                strategy_speed = f"{hrs_in_candle}HRS"
            elif candle_size.seconds >= 60:
                min_in_candle = int(candle_size.seconds / 60)
                strategy_speed = f"{min_in_candle}MIN"
            else:
                strategy_speed = f"{candle_size.seconds}SEC"
        else:  # Для свечей больше 1 дня лучше использовать override
            if candle_size.days >= 28:
                if enter_pos['Дата входа'].day == 1 and exit_pos['Дата выхода'].day == 1:
                    # Я понимаю, что это не супер надёжный способ проверки, но пока что так сойдёт
                    # TODO придумать понадёжнее способ проверять, что свечки месячные
                    d1 = exit_pos['Дата выхода']
                    d2 = enter_pos['Дата входа']
                    month_diff = (d1.year - d2.year) * 12 + d1.month - d2.month
                    months_in_candle = int(month_diff / bar_diff)
                    strategy_speed = f"{months_in_candle}MTH"
                else:
                    strategy_speed = f"{candle_size.days}DAY"
            else:
                if candle_size.days % 7 == 0:
                    weeks_in_candle = int((candle_size.days / 7) / bar_diff)
                    strategy_speed = f"{weeks_in_candle}WKS"
                else:
                    strategy_speed = f"{candle_size.days}DAY"
        return strategy_speed
