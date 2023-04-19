import sqlite3
import pandas as pd
from pandas import DataFrame
import numpy as np

SOURCE_FILENAME = 'test_data.xlsx'
MIN_DATE = '2022-04-01'
MAX_DATE = '2022-04-30'
DB_NAME = 'test_database.sqlite'
TABLE_NAME = 'test_data'


class DataFrameCleaner():
    def __init__(self, dataframe, min_date, max_date) -> None:
        self.dataframe = dataframe
        self.min_date = min_date
        self.max_date = max_date

    def run(self):
        return self.get_clean_dataframe()

    def add_fake_data(self) -> None:
        """
        Добавляет колонку даты с одинаковым значением для каждых двух компаний.
        Даты будут случайные при каждом запуске экземпляра класса.
        """
        for row in range(len(self.dataframe)):
            if not row % 2:
                fake_data = self.dataframe.loc[
                    self.dataframe.index[row], 'date'
                ] = np.random.choice(
                    pd.date_range(self.min_date, self.max_date)
                )
            else:
                self.dataframe.loc[
                    self.dataframe.index[row], 'date'
                ] = fake_data

    def clean_header(self) -> None:
        """
        Подготавливает исходный header, преобразуяв одну строку,
        с сохранением необходимых названий колонок.
        """
        self.dataframe.columns = [
            '_'.join(col)for col in self.dataframe.columns
        ]
        self.dataframe = self.dataframe.rename(columns={
            'id_Unnamed: 0_level_1_Unnamed: 0_level_2': 'id',
            'company_Unnamed: 1_level_1_Unnamed: 1_level_2': 'company',
        })

    def get_clean_dataframe(self) -> DataFrame:
        """
        Создаёт dataframe с правильым header'ом,
        на основе исходного dataframe
        """
        self.clean_header()
        self.add_fake_data()
        clean_columns = ['id', 'company', 'date']
        result_dataframe = pd.DataFrame(columns=clean_columns)

        # Цикл проходит по необработанным колонкам,
        # и разбирает на fact и forecast,
        # учитывая тип ресурсов (resource_type) и данных(data_type)
        for column in self.dataframe:
            if column in clean_columns:
                continue
            temp_dataframe = self.dataframe.groupby(
                clean_columns, as_index=False
            )[column].first().rename(columns={column: column.split('_')[0]})
            temp_dataframe.loc[:, 'resource_type'] = column.split('_')[1]
            temp_dataframe.loc[:, 'data_type'] = column.split('_')[2]
            result_dataframe = pd.concat(
                [result_dataframe, temp_dataframe], axis=0, sort=True
            ).reset_index(drop=True)

        # Объединине получившихся данных fact и forecast по готовым колонкам
        result_dataframe = result_dataframe.groupby(
                    ['id', 'company', 'data_type', 'resource_type', 'date'],
                    as_index=False
                ).sum()
        return result_dataframe

    @staticmethod
    def get_total(dataframe) -> DataFrame:
        """
        Расчитывает тотал для факта и прогноза по датам в один DataFrame
        """
        total = dataframe.groupby(
            ['date'], as_index=False
        ).agg(
            total_fact=('fact', 'sum'),
            total_forecast=('forecast', 'sum')
        )
        return total


def dataframe_to_db(
        dataframe: DataFrame,
        table_name: str,
        db_name: str,
        if_exists: str = 'replace'
) -> None:
    """
    Заносит получившийся dataframe в базу данных,
    заменяя данные, если есть.
    """
    conn = sqlite3.connect(db_name)
    dataframe.to_sql(table_name, conn, if_exists=if_exists, index=False)


if __name__ == '__main__':
    # Создание dataframe на основе данных xls файла
    source_dataframe = pd.read_excel(SOURCE_FILENAME, header=[0, 1, 2])

    # Создание экземпляра обработчика датафрейма и его запуск
    df_cleaner = DataFrameCleaner(
        dataframe=source_dataframe,
        min_date=MIN_DATE,
        max_date=MAX_DATE
        )
    result_dataframe = df_cleaner.run()
    print('DATAFRAME:', result_dataframe, sep='\n')

    # Перенос датафрейма в базу данных
    dataframe_to_db(
        dataframe=result_dataframe,
        db_name=DB_NAME,
        table_name=TABLE_NAME
        )

    # Отображение total по значениям fact и forecast
    print('TOTAL:', df_cleaner.get_total(result_dataframe), sep='\n')
