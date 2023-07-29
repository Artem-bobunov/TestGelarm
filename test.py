from sql_query import select_federal_project_by_id, check_organization_exists, insert_into_federal_projects, \
    check_itogo_exists, insert_into_federal_organizations, insert_into_federal_projects_delayed

import pandas as pd
from datetime import datetime, timedelta
import os
import re
import psycopg2
from prettytable import PrettyTable

LIST_DATA = 'data/'
TRUE_FORMAT = 'форма'
TRUE_MODEL = 'эталон'
DATE_FT = None

table = PrettyTable()
table1 = PrettyTable()
table2 = PrettyTable()

# prj_date:date_C, year_no:2023, year_plan:[j1], year_achieved_cnt:j[2]
# year_achieved_percent:j[3], year_left_cnt:j[4], year_left_percent[5]
# year_delayed_cnt"j[6], year_delayed_percent[7]
table.field_names = ['federal_prj_id', 'federal_org_id', 'prj_date', 'year_no', 'year_plan',
                'year_achieved_cnt', 'year_achieved_percent', 'year_left_cnt', 'year_left_percent',
                'year_delayed_cnt', 'year_delayed_percent',
                'total_delayed_cnt', 'total_delayed_percent',
                'created_at', 'updated_at', 'created_from', 'created_to', 'relevance_dttm']
table1.field_names = ['federal_prj_id', 'federal_org_id', 'prj_date', 'year_no', 'year_plan',
                'year_achieved_cnt', 'year_achieved_percent', 'year_left_cnt', 'year_left_percent',
                'year_delayed_cnt', 'year_delayed_percent',
                'total_delayed_cnt', 'total_delayed_percent',
                'created_at', 'updated_at', 'created_from', 'created_to', 'relevance_dttm']

table2.field_names = ['federal_prj_id', 'federal_org_id', 'prj_date', 'year_no', 'year_plan',
                'year_achieved_cnt', 'year_achieved_percent', 'year_left_cnt', 'year_left_percent',
                'year_delayed_cnt', 'year_delayed_percent',
                'total_delayed_cnt', 'total_delayed_percent',
                'created_at', 'updated_at', 'created_from', 'created_to', 'relevance_dttm']

def list_data(data):
    return os.listdir(data)

def get_week_boundaries(date_str):
    # Преобразование строки с датой в объект datetime
    # date_obj = datetime.strptime(date_str, '%d.%m.%Y')
    date_obj = date_str

    # Определение дня недели (0 - понедельник, 6 - воскресенье)
    day_of_week = date_obj.weekday()

    # Вычисление даты начала недели (понедельник)
    start_of_week = date_obj - timedelta(days=day_of_week)

    # Вычисление даты конца недели (воскресенье)
    end_of_week = start_of_week + timedelta(days=6)

    # Установка времени на начало и конец дня
    created_from = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    created_to = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999)

    return created_from, created_to

def split_FM(string) -> str:
    forma, model = None, None

    if string.islower():
        # Разбиваем название файла чтобы удостоверится что в названии есть слова "форма" и "эталон"
        split_d = string.split()
    else:
        # иначе если название файла находится в верзнем регистре
        # Разбиваем название файла и переводим в нижний регситр
        split_d = string.lower().split()

    for sd in split_d:
        if TRUE_FORMAT in sd:
            forma = sd
        if TRUE_MODEL in sd:
            model = sd

    # Проверяем если вдруг вообще не нашлось этих слов
    if forma == None:
        forma = TRUE_FORMAT
    if model == None:
        model = TRUE_MODEL

    return f"{forma} {model}"

def checinkg_forms_date(string):
    date_pattern = r"\b\d{2,4}[-./]\d{1,2}[-./]\d{2,4}\b"
    dates = re.findall(date_pattern, string)
    return dates

def checking_lower_and_date():
    # Переменная для названия файла
    name_file = None

    # смотрим список файлов
    dt = list_data(LIST_DATA)
    date_format = None
    true_date_fotmat = None
    # В цикле проходимся по каждому названию файла
    for d in dt:
        print(f"Название файла: {d}")

        f_m = split_FM(d)

        # Нужно достать дату из названия файла и проверить ее формат
        # распознаем дату в строке
        split_name_file = checinkg_forms_date(d)
        print(split_name_file)

        # преобразовываем из строки в дату формата в котором она есть, а потом в нужный формат
        true_date_fotmat = datetime.strptime(*split_name_file, '%Y-%m-%d')
        date_format = true_date_fotmat.strftime('%d.%m.%Y')

        DATE_FT = date_format

        # формируем название файла
        name_file = f"{f_m} ({date_format}).xlsx"
        print(f"Правильно сформированное название файла: {name_file}")

        # Переименовываем название файла на допустимое
        os.rename(f'{LIST_DATA}{d}', f'{LIST_DATA}{name_file}')

def parsing():

    dt = list_data(LIST_DATA)

    for i in dt:
        dataframe = pd.read_excel(f"{LIST_DATA}{i}",)

        df = pd.DataFrame(dataframe)

        # в ячейке R1 должна быть указана дата
        R1 = df.columns[17]
        if R1 is not None:
            try:
                search_r1 = datetime.strftime(R1,'%d.%m.%Y')
                print(f'В ячейке R1 содержится дата: {search_r1}')
            except Exception as e:
                print(f'В ячейке R1 не содержтся дата. Ошибка: {e}')

        # iloc: строка, столбец
        # пройти по файлу по столбцу А до записи "итого" - это должно быть последней строкой датасета.

        if df.iloc[48,0] == 'Итого':
            df.drop([50], inplace=True)
            print(f'Оставшееся количество строк: {df.shape[0]}')

        # датасет текущего года - 7 столбцов от D до J.
        df_2023 = df.iloc[4:48,3:10]
        # print(df_2023.shape[0])

        # пройти по файлу по строке 3 от столбца K до записи "итого" - получить количество датасетов по предыдущим годам.
        df_2022 = df.iloc[4:48,10:15]
        # print(df_2022)

        df_2021 = df.iloc[4:48,15:20]
        # print(df_2021)

        # датасет "итого" - 2 столбца от последнего датасета года
        df_itogo = df.iloc[4:48,20:22]
        # print(df_itogo)

        df_AB = df.iloc[4:48,0:2].reset_index(drop=True)
        # df_AB.fillna(method='ffill', inplace=True)
        df_AB_itogo = df.iloc[48:50,0:2].reset_index(drop=True)

        df_AB.index += 1
        df_AB_itogo.index += 1
        # print(df_AB)

        df_date = df.iloc[4:,2].reset_index(drop=True)
        df_date.index += 1
        # print(df_date)

        df_itogo_2023 = df.iloc[48:50,3:10].reset_index(drop=True)
        df_itogo_2022 = df.iloc[48:50,10:15].reset_index(drop=True)
        df_itogo_2021 = df.iloc[48:50,15:20].reset_index(drop=True)
        df_itogo_2023.index += 1
        df_itogo_2022.index += 1
        df_itogo_2021.index += 1
        # print(df_itogo_2023)
        # print(df_itogo_2022)
        # print(df_itogo_2021)

        new_df_2023 = df_2023.copy().reset_index(drop=True)
        new_df_2022 = df_2022.copy().reset_index(drop=True)
        new_df_2021 = df_2021.copy().reset_index(drop=True)
        new_df_itogo = df_itogo.copy().reset_index(drop=True)
        new_df_2023.index += 1
        new_df_2022.index += 1
        new_df_2021.index += 1
        new_df_itogo.index += 1
        # print(new_df_itogo)

        #Делаем автоматически заполняемый справочники
        for ab in df_AB.itertuples(index=True, name=None):
            # print(ab)
            index_ab = ab[1]
            # print(index_ab)
            # если в столбце А есть одна точка ".", то берётся id для значения столбца В, если точек две - то значение federal_prj_id из предыдущей записи
            if str(index_ab).count('.') == 1:
                # Для таблицы федеральные проекты
                # print(f'Название федерального проекта {ab[1]} {ab[2]}')
                # Заполняем в таблице федерального проекта данные
                # insert_into_federal_projects(ab[2],str(index_ab))
                pass

            if str(index_ab).count('.') == 2:
                # Для таблицы федеральные организации
                # print(f'Название федерального проекта {ab[1]} {ab[2]}')
                # insert_into_federal_organizations(ab[2],str(index_ab))
                pass

        # автоматическая запись строк "Итого"
        for ab_itogo in df_AB_itogo.itertuples(index=True, name=None):
            if ab_itogo[1] == 'Итого':
                itogo = ab_itogo[1]
                print('Итого')
                # insert_into_federal_projects(itogo,None)

        # Если точек две то обнулю chechk_dot пока не появится одна точка.
        check_dot = None
        fed_prj_id,fed_org_id = None,None

        created_from, created_to = None,None

        # для каждой строки, начиная с 6, создаётся такое количество записей, которое соответствует количеству датасетов с годами:
        # iloc: строка, столбец
        df_list = [new_df_2023,new_df_2022,new_df_2021]
        df_list_itogo = [df_itogo_2023,df_itogo_2022,df_itogo_2021]
        t = 0
        # проходимся по датафреймам
        for d in df_list:
            # если используется датасет текущего года
            if d is new_df_2023:
                year_no = 2023
            if d is new_df_2022:
                year_no = 2022
            if d is new_df_2021:
                year_no = 2021
            i = 0
            # деаем итератор по строкам
            for j in d.itertuples(index=True, name=None):
                # print(j)
                # Узнаем индекс строки на которой находимся в данный момент
                index_row_d = j[0]

                # print(j, new_df_itogo.iloc[j[0], 0])
                # если в столбце C дата не соответствует дню из ячейки R1 - строку пропускаем (т.е. получится через одну забирать)
                date_C = datetime.strftime(df_date[index_row_d], '%d.%m.%Y')
                if date_C == search_r1:
                    index_j_0 = j[0]
                    # проходимся по датасету столбцов А и В
                    for ab in df_AB.itertuples(index=True, name=None):
                        # print(ab)
                        # print(j,new_df_itogo.iloc[index_j_0,0])
                        # делаем связку где айди датафрема выбранного года равна айди столбца федерального проекта
                        if ab[0] == j[0]:
                            # print(f'Айди датафрема {ab[0]} равен айди столбца {j[0]}')
                            # print(f'{ab[1]}')
                            # Если количество точек равно 1
                            if str(ab[1]).count('.') == 1:
                                # print('Точка одна')
                                federal_prj_id = select_federal_project_by_id(str(ab[1]))
                                fed_prj_id = federal_prj_id[0]

                                # Если точка одна то значит есть, то федерал проекта и фед_орг равен нон
                                fed_org_id = check_dot

                                if year_no == 2023: # Если год текущий, соотвественно сохраняем данные как ниже показано
                                    # print(f'Встретился {year_no} год')
                                    data = [
                                        fed_prj_id, fed_org_id, date_C, year_no, j[1], j[2], j[3], j[4], j[5], j[6], j[7],
                                        None, None, None, None, created_from, created_to, None
                                    ]
                                    table.add_row(data)
                                elif year_no == 2022 or year_no == 2021: # Если год не текущий
                                    # print(f'Встретился {year_no} год')
                                    data = [
                                        fed_prj_id, fed_org_id, date_C, year_no, j[1], j[2], j[3], None, None, j[4], j[5],
                                        new_df_itogo.iloc[index_j_0,0], new_df_itogo.iloc[index_j_0,1], None, None, created_from, created_to, search_r1
                                    ]
                                    table1.add_row(data)
                                # insert_into_federal_projects_delayed(data)

                            if str(ab[1]).count('.') == 2:
                                # print('Точки две')
                                # ищем федеральнаую организацию в БД
                                federal_org_id = check_organization_exists(str(ab[1]))
                                # print(federal_org_id)
                                fed_org_id = federal_org_id[0]
                                # 18
                                if year_no == 2023:  # Если год текущий, соотвественно сохраняем данные как ниже показано
                                    data = [
                                        fed_prj_id, fed_org_id, date_C, year_no, j[1], j[2], j[3], j[4], j[5], j[6],
                                        j[7],None, None, None, None, created_from, created_to, None]
                                    table.add_row(data)
                                elif year_no == 2022 or year_no == 2021:  # Если год не текущий
                                    data = [
                                        fed_prj_id, fed_org_id, date_C, year_no, j[1], j[2], j[3], None, None, j[4],
                                        j[5],new_df_itogo.iloc[index_j_0, 0], new_df_itogo.iloc[index_j_0, 1], None, None, created_from, created_to,search_r1]
                                    table1.add_row(data)
                                # insert_into_federal_projects_delayed(data)
        # создать отдельную запись для строки "итого":
        for di in df_list_itogo:
            if di is df_itogo_2023:
                year_no = 2023
            if di is df_itogo_2022:
                year_no = 2022
            if di is df_itogo_2021:
                year_no = 2021
            for dj in di.itertuples(index=True, name=None):
                index_itogo = dj[0]

                date_C_itogo = datetime.strftime(df_date[index_itogo], '%d.%m.%Y')
                federal_prj_id = check_itogo_exists(itogo)
                # print(federal_prj_id)

                if date_C_itogo == search_r1:
                    if year_no == 2023:
                        data = [
                            federal_prj_id[0], None, date_C_itogo, year_no, dj[1], dj[2], dj[3], dj[4], dj[5], dj[6], dj[7],
                            df.iloc[48, 20], df.iloc[48, 21], None, None, created_from, created_to, search_r1
                        ]
                        table2.add_row(data)
                    else:
                        data = [
                            federal_prj_id[0], None, date_C_itogo, year_no, dj[1], dj[2], dj[3], None, None, dj[4],
                            dj[5], df.iloc[48,20], df.iloc[48,21], None, None, created_from, created_to,
                            search_r1]
                        table2.add_row(data)
                    # insert_into_federal_projects_delayed(data)

        print(table)
        print(table1)
        print(table2)

if __name__ == '__main__':
    #2023-06-16
    # checking_lower_and_date()
    parsing()

