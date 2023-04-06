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
import asyncio
import asyncpg



async def write_db(data):
    # connect to database
    try:
        connection = await asyncpg.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        connection.autocommit = True

        # #creating new database
        # with connection.cursor() as cursor:
        #     cursor.execute(
        #         """CREATE TABLE companies_2021(
        #         id serial PRIMARY KEY,
        #         inn VARCHAR(12) NOT NULL,
        #         company_name VARCHAR(300) NOT NULL,
        #         revenue_2021 NUMERIC,
        #         costs_2021 NUMERIC,
        #         profit_2021 NUMERIC);""")

        company_name = data['company_name']
        inn = data['inn']
        revenue_2021 = data['revenue_2021']
        costs_2021 = data['costs_2021']
        profit_2021 = data['profit_2021']

        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO companies_2021 (inn, company_name, revenue_2021, costs_2021, profit_2021) VALUES (%s, %s, %s, %s, %s)',
                (inn, company_name, revenue_2021, costs_2021, profit_2021))


    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL,', _ex)
        print(data)


def get_link() -> list:
    url_2021 = r'C:\Data\2021'
    path_list = []
    for file in os.listdir(url_2021):
        path = f'{url_2021}\{file}'
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
            revenue_2021 = 0
            costs_2021 = 0

            for item in child:
                temp_dict = item.attrib
                if temp_dict.get('НаимОрг') != None:
                    company_name = temp_dict.get('НаимОрг')
                if temp_dict.get('ИННЮЛ') != None:
                    inn = temp_dict.get('ИННЮЛ')
                if temp_dict.get('СумДоход') != None:
                    revenue_2021 = float(temp_dict.get('СумДоход'))
                if temp_dict.get('СумРасход') != None:
                    costs_2021 = float(temp_dict.get('СумРасход'))
                profit_2021 = revenue_2021 - costs_2021
                data = {'company_name': company_name,
                        'inn': inn,
                        'revenue_2021': revenue_2021,
                        'costs_2021': costs_2021,
                        'profit_2021': profit_2021
                        }

            if data.get('company_name') == '':
                pass
            else:
                write_db(data)




async def main():
    read_file(get_link())

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
