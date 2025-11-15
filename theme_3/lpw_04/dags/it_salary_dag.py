"""
DAG для анализа зарплат IT-специалистов в Европе
Вариант задания №21
Источник данных: https://www.kaggle.com/datasets/parulpandey/2020-it-salary-survey-for-eu-region
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os
import kagglehub

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'it_salary_analysis',
    default_args=default_args,
    description='Анализ зарплат IT в Европе - вариант 21',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'it_salary', 'kaggle', 'variant_21']
)

def extract_from_kaggle(**context):
    """Extract: Скачивание датасета с Kaggle и сохранение в правильную папку"""
    import os
    import shutil
    import kagglehub

    print("Начинаем извлечение данных о зарплатах IT в Европе с Kaggle...")
    
    # Используем /tmp — туда точно можно писать
    tmp_dir = '/tmp/kaggle_download'
    os.makedirs(tmp_dir, exist_ok=True)
    
    dataset_name = "parulpandey/2020-it-salary-survey-for-eu-region"
    print(f"Скачиваем датасет: {dataset_name}")
    path = kagglehub.dataset_download(dataset_name)
    
    # Найдём CSV-файл
    csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("CSV-файл не найден в скачанном датасете")
    
    source_file = os.path.join(path, csv_files[0])
    dest_file = os.path.join(tmp_dir, "it_salary_survey.csv")
    
    # Копируем ВНУТРИ /tmp (права есть)
    shutil.copy2(source_file, dest_file)
    print(f"Файл временно сохранён: {dest_file}")
    
    # Передаём путь в XCom
    context['task_instance'].xcom_push(key='data_file_path', value=dest_file)
    return dest_file

def load_raw_to_postgres(**context):
    import pandas as pd
    data_file_path = context['task_instance'].xcom_pull(key='data_file_path', task_ids='extract_from_kaggle')
    print(f"Чтение данных из: {data_file_path}")
    df = pd.read_csv(data_file_path, low_memory=False)
    # ... остальной код без изменений
    print(f"Загружено {len(df)} строк")
    
    # Создаём "сырую" таблицу
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    postgres_hook.run("DROP TABLE IF EXISTS raw_it_salary;")
    
    # Используем текстовые столбцы, т.к. структура датасета может меняться
    create_sql = """
    CREATE TABLE raw_it_salary (
        id SERIAL PRIMARY KEY,
        data JSONB
    );
    """
    postgres_hook.run(create_sql)
    
    # Загружаем каждую строку как JSON
    rows = [(row.to_json(),) for _, row in df.iterrows()]
    postgres_hook.insert_rows('raw_it_salary', rows, target_fields=['data'])
    print("Сырые данные загружены в raw_it_salary")

def transform_and_create_staging(**context):
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    postgres_hook.run("DROP TABLE IF EXISTS stg_it_salary CASCADE;")
    
    # Исправленный SQL с обработкой пробела в названии Position и пустых значений
    transform_sql = """
    CREATE TABLE stg_it_salary AS
SELECT
    TRIM(LOWER(data::jsonb->>'City')) AS city,
    TRIM(COALESCE(data::jsonb->>'Position ', data::jsonb->>'Position')) AS position,
    CASE
        WHEN (data::jsonb->>'Total years of experience') ~ '^[0-9]+' THEN
            (REGEXP_REPLACE(data::jsonb->>'Total years of experience', '[^0-9].*$', '', 'g'))::INTEGER
        ELSE NULL
    END AS experience_years,
    TRIM(data::jsonb->>'Your main technology / programming language') AS main_technology,
    CASE
        WHEN NULLIF(TRIM(data::jsonb->>'Yearly brutto salary (without bonus and stocks) in EUR'), '') IS NOT NULL THEN
            REPLACE(
                REPLACE(
                    TRIM(data::jsonb->>'Yearly brutto salary (without bonus and stocks) in EUR'),
                    ' ',
                    ''
                ),
                ',',
                ''
            )::NUMERIC
        ELSE NULL
    END AS salary_eur
FROM raw_it_salary
WHERE 
    -- Город и должность обязательны
    NULLIF(TRIM(data::jsonb->>'City'), '') IS NOT NULL
    AND NULLIF(TRIM(COALESCE(data::jsonb->>'Position ', data::jsonb->>'Position')), '') IS NOT NULL
    AND NULLIF(TRIM(data::jsonb->>'Your main technology / programming language'), '') IS NOT NULL
    
    -- Опыт и зарплата — опциональны, но если есть — должны быть валидны
    AND (
        (data::jsonb->>'Total years of experience') IS NULL
        OR (data::jsonb->>'Total years of experience') ~ '^[0-9]+'
    )
    AND (
        NULLIF(TRIM(data::jsonb->>'Yearly brutto salary (without bonus and stocks) in EUR'), '') IS NULL
        OR (
            REPLACE(REPLACE(TRIM(data::jsonb->>'Yearly brutto salary (without bonus and stocks) in EUR'), ' ', ''), ',', '') ~ '^[0-9.]+$'
            AND REPLACE(REPLACE(TRIM(data::jsonb->>'Yearly brutto salary (without bonus and stocks) in EUR'), ' ', ''), ',', '')::NUMERIC BETWEEN 1000 AND 500000
        )
    );
    """
    postgres_hook.run(transform_sql)
    print("✅ Стейджинг-таблица stg_it_salary создана с обработкой пробелов и пустых значений")

# Задачи DAG
extract_task = PythonOperator(
    task_id='extract_from_kaggle',
    python_callable=extract_from_kaggle,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_raw_to_postgres',
    python_callable=load_raw_to_postgres,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_create_staging',
    python_callable=transform_and_create_staging,
    dag=dag
)

create_datamart_task = PostgresOperator(
    task_id='create_datamart',
    postgres_conn_id='analytics_postgres',
    sql='datamart_variant_21.sql',
    dag=dag
)

# Зависимости
extract_task >> load_task >> transform_task >> create_datamart_task