import pandas as pd
import numpy as np


class EquityCurve:
    def __init__(self, csv_file_path):
        self.raw_ts_lab_data = self.parse_ts_lab_data(csv_file_path)
        # self.raw_ts_lab_data = pd.read_csv(csv_file_path, sep=',')

    def parse_ts_lab_data(self, csv_file_path):
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

        # Объединяем даты, чтобы потом можно было с ними арифметику делать
        ts_lab_data['Дата входа'] = ts_lab_data['Дата входа'] + " " + ts_lab_data['Время входа']
        ts_lab_data['Дата выхода'] = ts_lab_data['Дата выхода'] + " " + ts_lab_data['Время выхода']

        ts_lab_data.drop(columns=["Unnamed: 0", 'Время входа', 'Время выхода'], inplace=True)

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

        ts_lab_data = ts_lab_data.astype({"Позиция": "string",
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
        return ts_lab_data
