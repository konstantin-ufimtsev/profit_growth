# ИНН (inn)- 12 символов ИП 10 символов юрлица - integer
# наименование компании (company_name) - varchar
# выручка 2020 (revenue_2020)  - numeric
# расходы 2020 (costs_2020) - numeric
# прибыль 2020 (profit_2020) - numeric
# выручка 2021 (revenue_2021) - numeric
# расходы 2021 (costs_2021) - numeric
# прибыль 2021 (profit_2021) - numeric
# рост выручки (revenue_growth) - numeric
# рост прибыль (profit_growth) - numeric
# электронный адрес (email) - varchar

import psycopg2
import os
import xml.etree.ElementTree as ET
from config import *


def write_db(data):
    # connect to database
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        connection.autocommit = True

        #creating new database
        # with connection.cursor() as cursor:
        #     cursor.execute(
        #         """CREATE TABLE companies_2020(
        #         id serial PRIMARY KEY,
        #         inn VARCHAR(12) NOT NULL,
        #         company_name VARCHAR(100) NOT NULL,
        #         revenue_2020 NUMERIC,
        #         costs_2020 NUMERIC,
        #         profit_2020 NUMERIC);""")

        company_name = data['company_name']
        inn = data['inn']
        revenue_2020 = data['revenue_2020']
        costs_2020 = data['costs_2020']
        profit_2020 = data['profit_2020']
        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO companies_2020 (inn, company_name, revenue_2020, costs_2020, profit_2020) VALUES (%s, %s, %s, %s, %s)',
                (inn, company_name, revenue_2020, costs_2020, profit_2020))


    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL,', _ex)
        print(data)


def get_link() -> list:
    url_2020 = r'C:\Data\2020'
    path_list = []
    for file in os.listdir(url_2020):
        path = f'{url_2020}\{file}'
        path_list.append(path)
    return path_list


def read_file(path_list: list):
    count = 0
    for path in path_list:
        # print(path)
        tree = ET.parse(path)
        root = tree.getroot()
        data_list = []

        for child in root:
            data = {}
            company_name = ''
            inn = ''
            revenue_2020 = 0
            costs_2020 = 0

            for item in child:
                temp_dict = item.attrib
                if temp_dict.get('НаимОрг') != None:
                    company_name = temp_dict.get('НаимОрг')
                if temp_dict.get('ИННЮЛ') != None:
                    inn = temp_dict.get('ИННЮЛ')
                if temp_dict.get('СумДоход') != None:
                    revenue_2020 = float(temp_dict.get('СумДоход'))
                if temp_dict.get('СумРасход') != None:
                    costs_2020 = float(temp_dict.get('СумРасход'))
                profit_2020 = revenue_2020 - costs_2020
                data = {'company_name': company_name,
                        'inn': inn,
                        'revenue_2020': revenue_2020,
                        'costs_2020': costs_2020,
                        'profit_2020': profit_2020
                        }

            if data.get('company_name') == '':
                pass
            else:
                write_db(data)
                count += 1
                #
                print(f'Пишем в базу запись № {count}')


def main():

    read_file(get_link())


if __name__ == '__main__':
    main()
