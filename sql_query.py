import psycopg2
from psycopg2 import Error


# Параметры подключения к базе данных
db_params = {
    "host": "localhost",
    "database": "gelarm",
    "user": "postgres",
    "password": "Root123!"
}

def select_federal_project_by_id(id):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # SQL-запрос SELECT
    select_query = """
        SELECT id
        FROM public.federal_projects
        WHERE npp = %s;
    """

    # Значения для параметра в SQL-запросе (должны быть в виде кортежа)
    values = (id,)
    cur.execute(select_query, values)

    # Получаем результат запроса
    result = cur.fetchone()  # Если нужно получить только одну запись

    # Закрываем курсор и соединение
    cur.close()
    conn.close()

    return result

def check_organization_exists(organization_id):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # SQL-запрос для выполнения SELECT
    select_query = """
        SELECT id 
        FROM public.federal_organizations
        WHERE npp = %s;
    """

    # Значения для вставки (должны быть в виде кортежа)
    values = (organization_id,)
    cur.execute(select_query, values)

    # Получаем результаты запроса
    result = cur.fetchone()

    cur.close()
    conn.close()

    # Возвращаем True, если запись с указанным ID существует, и False в противном случае
    return result


def insert_into_federal_projects(name,npp):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # SQL-запрос для вставки данных в таблицу federal_projects
    insert_query = """
        INSERT INTO federal_projects (name,npp) VALUES (%s,%s);
    """

    # Значения для вставки (должны быть в виде кортежа)
    values = (name,npp,)
    cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()


def insert_into_federal_organizations(name,npp):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # SQL-запрос для вставки данных в таблицу federal_organizations
    insert_query = """
        INSERT INTO federal_organizations (name,npp) VALUES (%s,%s);
    """

    # Значения для вставки (должны быть в виде кортежа)
    values = (name,npp,)
    cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()


def insert_into_federal_projects_delayed(values):
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # SQL-запрос для вставки данных в таблицу federal_projects_delayed
        insert_query = """
            INSERT INTO federal_projects_delayed (
                federal_prj_id, federal_org_id, prj_date, year_no, year_plan, 
                year_achieved_cnt, year_achieved_percent, year_left_cnt, year_left_percent, 
                year_delayed_cnt, year_delayed_percent,
                total_delayed_cnt, total_delayed_percent, 
                created_at, updated_at, created_from, created_to, relevance_dttm
            ) 
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """

        # Выполняем запрос
        cur.execute(insert_query, values)

        # Подтверждаем изменения и закрываем соединение
        conn.commit()
        cur.close()
        conn.close()

        print("Запись успешно добавлена в таблицу federal_projects_delayed.")
    except (Exception, Error) as e:
        print("Произошла ошибка:", e)

def check_itogo_exists(name):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # SQL-запрос для выполнения SELECT
    select_query = """
        SELECT id 
        FROM public.federal_projects
        WHERE name = %s;
    """

    # Значения для вставки (должны быть в виде кортежа)
    values = (name,)
    cur.execute(select_query, values)

    # Получаем результаты запроса
    result = cur.fetchone()

    cur.close()
    conn.close()

    # Возвращаем True, если запись с указанным ID существует, и False в противном случае
    return result

