import psycopg2

# Параметры подключения к базе данных
db_params = {
    "host": "localhost",
    "database": "gelarm",
    "user": "postgres",
    "password": "Root123!"
}

# Создаем таблицы и справочники
def create_tables():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Справочник federal_projects
    cur.execute("""
            CREATE TABLE IF NOT EXISTS federal_projects (
                id SERIAL PRIMARY KEY,
                name TEXT,
                npp TEXT
            )
        """)

    # Справочник federal_organizations
    cur.execute("""
            CREATE TABLE IF NOT EXISTS federal_organizations (
                id SERIAL PRIMARY KEY,
                name TEXT,
                npp TEXT
            )
        """)

    # Таблица federal_projects_delayed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS federal_projects_delayed (
            id SERIAL PRIMARY KEY,
            federal_prj_id INT REFERENCES federal_projects(id),
            federal_org_id INT REFERENCES federal_organizations(id),
            prj_date TIMESTAMPTZ,
            year_no INT,
            year_plan INT,
            year_achieved_cnt INT,
            year_achieved_percent FLOAT,
            year_left_cnt INT,
            year_left_percent FLOAT,
            year_delayed_cnt INT,
            year_delayed_percent FLOAT,
            total_delayed_cnt INT,
            total_delayed_percent FLOAT,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            created_from TIMESTAMPTZ,
            created_to TIMESTAMPTZ,
            relevance_dttm TIMESTAMPTZ
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

# Вызываем функцию для создания таблиц и справочников
create_tables()
