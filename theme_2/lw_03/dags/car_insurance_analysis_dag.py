"""
DAG для анализа страховых выплат по брендам автомобилей
Вариант задания №21

Автор: Смляков Руслан
Дата: 2025
"""

from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

# Конфигурация по умолчанию
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

dag = DAG(
    'car_insurance_analysis',
    default_args=default_args,
    description='Анализ средних страховых выплат по брендам автомобилей',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'cars', 'insurance', 'variant_21']
)

# Пути
DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/car_insurance_analysis.db'

# === EXTRACT ===

def extract_cars_data(**context):
    """Извлечение данных об автомобилях из CSV"""
    csv_path = os.path.join(DATA_DIR, 'cars.csv')
    cars_df = pd.read_csv(csv_path)
    context['task_instance'].xcom_push(key='cars_data', value=cars_df.to_dict('records'))
    return f"Загружено {len(cars_df)} автомобилей"

def extract_claims_data(**context):
    """Извлечение данных о страховых случаях из Excel"""
    excel_path = os.path.join(DATA_DIR, 'claims.xlsx')
    claims_df = pd.read_excel(excel_path)
    context['task_instance'].xcom_push(key='claims_data', value=claims_df.to_dict('records'))
    return f"Загружено {len(claims_df)} страховых случаев"

def extract_mileage_data(**context):
    """Извлечение данных о пробеге из JSON (опционально)"""
    json_path = os.path.join(DATA_DIR, 'mileage.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        mileage_data = json.load(f)
    context['task_instance'].xcom_push(key='mileage_data', value=mileage_data)
    return f"Загружено {len(mileage_data)} записей пробега"

# === TRANSFORM ===

def transform_data(**context):
    """Консолидация и расчёт средних выплат по брендам"""
    cars = pd.DataFrame(context['task_instance'].xcom_pull(key='cars_data', task_ids='extract_cars'))
    claims = pd.DataFrame(context['task_instance'].xcom_pull(key='claims_data', task_ids='extract_claims'))
    mileage = pd.DataFrame(context['task_instance'].xcom_pull(key='mileage_data', task_ids='extract_mileage'))

    # Основное объединение: авто + страховые случаи
    merged = pd.merge(cars, claims, on='car_id', how='inner')
    
    # Расчёт средних выплат по брендам
    brand_analysis = merged.groupby('brand')['damage_amount'].agg(
        avg_claim_amount='mean',
        total_claims='count',
        total_payout='sum'
    ).reset_index()
    
    brand_analysis['avg_claim_amount'] = brand_analysis['avg_claim_amount'].round(2)
    brand_analysis['total_payout'] = brand_analysis['total_payout'].round(2)
    
    # Сортировка по средней выплате (по убыванию)
    brand_analysis = brand_analysis.sort_values('avg_claim_amount', ascending=False)
    
    context['task_instance'].xcom_push(key='brand_analysis', value=brand_analysis.to_dict('records'))
    return f"Проанализировано {len(brand_analysis)} брендов"

# === LOAD ===

def load_to_database(**context):
    """Загрузка результатов в SQLite"""
    data = context['task_instance'].xcom_pull(key='brand_analysis', task_ids='transform_data')
    df = pd.DataFrame(data)
    
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql('brand_insurance_analysis', conn, if_exists='replace', index=False)
        print(f"Загружено {len(df)} записей в таблицу brand_insurance_analysis")
    finally:
        conn.close()
    return "Данные успешно сохранены в SQLite"

# === REPORT ===

def generate_report(**context):
    """Генерация текстового отчёта и CSV"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM brand_insurance_analysis ORDER BY avg_claim_amount DESC", conn)
    conn.close()
    
    # Текстовый отчёт
    report = f"""АНАЛИЗ СТРАХОВЫХ ВЫПЛАТ ПО БРЕНДАМ АВТОМОБИЛЕЙ
==================================================

Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Проанализировано брендов: {len(df)}

ТОП брендов с самыми высокими средними выплатами:
"""
    for _, row in df.head().iterrows():
        report += f"- {row['brand']}: {row['avg_claim_amount']:,.2f} руб. (всего случаев: {row['total_claims']})\n"
    
    report += f"\nЛидер по средней выплате: {df.iloc[0]['brand']} — {df.iloc[0]['avg_claim_amount']:,.2f} руб."

    # Сохранение
    report_path = '/opt/airflow/car_insurance_report.txt'
    csv_path = '/opt/airflow/car_insurance_data.csv'
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    # Передача в XCom
    context['task_instance'].xcom_push(key='report', value=report)
    context['task_instance'].xcom_push(key='report_path', value=report_path)
    context['task_instance'].xcom_push(key='csv_path', value=csv_path)
    context['task_instance'].xcom_push(key='top_brand', value=df.iloc[0]['brand'])
    
    return "Отчёт сформирован"

# === EMAIL ===

def send_email_with_attachments(**context):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import smtplib
    import os
    from datetime import datetime

    # Получаем данные
    top_brand = context['task_instance'].xcom_pull(key='top_brand', task_ids='generate_report')
    report_path = '/opt/airflow/car_insurance_report.txt'
    csv_path = '/opt/airflow/car_insurance_data.csv'

    # Создаем сообщение
    msg = MIMEMultipart()
    msg['From'] = 'airflow@example.com'
    msg['To'] = 'test@example.com'
    msg['Subject'] = '🚗 Анализ страховых выплат по брендам автомобилей - Результаты'

    # Формируем HTML-тело письма (идентично скриншоту)
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Анализ страховых выплат</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h2 {{ color: #2e7d32; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .check {{ color: green; }}
            .info {{ margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h2>🎉 Анализ страховых выплат по брендам автомобилей завершен успешно!</h2>

        <div class="info">
            <h3>📊 Информация о выполнении:</h3>
            <ul>
                <li><strong>DAG:</strong> car_insurance_analysis</li>
                <li><strong>Дата выполнения:</strong> {context['ds']}</li>
                <li><strong>Статус:</strong> <span class="check">✅ Все задачи выполнены без ошибок</span></li>
                <li><strong>Результаты:</strong> Сохранены в базе данных SQLite</li>
            </ul>
        </div>

        <div class="info">
            <h3>📈 Краткие результаты анализа:</h3>
            <table>
                <thead>
                    <tr>
                        <th>Бренд</th>
                        <th>Средняя выплата (руб.)</th>
                        <th>Количество случаев</th>
                        <th>Общая сумма выплат (руб.)</th>
                    </tr>
                </thead>
                <tbody>
    """

    # Добавляем строки таблицы из результата
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM brand_insurance_analysis ORDER BY avg_claim_amount DESC", conn)
    conn.close()

    for _, row in df.iterrows():
        html_body += f"""
                    <tr>
                        <td>{row['brand']}</td>
                        <td>{row['avg_claim_amount']:,.2f}</td>
                        <td>{row['total_claims']:,}</td>
                        <td>{row['total_payout']:,.2f}</td>
                    </tr>
        """

    html_body += f"""
                </tbody>
            </table>
        </div>

        <div class="info">
            <h3>📎 Прикрепленные файлы:</h3>
            <ul>
                <li><strong>car_insurance_report.txt</strong> - Подробный текстовый отчет</li>
                <li><strong>car_insurance_data.csv</strong> - Данные в формате CSV</li>
            </ul>
        </div>

        <p><em>Детальный отчет также доступен в логах задачи generate_report в Airflow UI.</em></p>

        <hr>
        <p style="color: #666; font-size: 12px;">
            Это автоматическое уведомление от системы Apache Airflow<br>
            Время отправки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </body>
    </html>
    """

    # Добавляем HTML-содержимое
    msg.attach(MIMEText(html_body, 'html'))

    # Прикрепляем файлы
    files = []
    if os.path.exists(report_path):
        with open(report_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(report_path)}",
        )
        msg.attach(part)
        files.append(os.path.basename(report_path))

    if os.path.exists(csv_path):
        with open(csv_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(csv_path)}",
        )
        msg.attach(part)
        files.append(os.path.basename(csv_path))

    # Отправка через SMTP
    try:
        server = smtplib.SMTP('mailhog', 1025)
        server.sendmail('airflow@example.com', 'test@example.com', msg.as_string())
        server.quit()
        print("📧 Email успешно отправлен с HTML-таблицей и прикрепленными файлами")
        return "Email отправлен"
    except Exception as e:
        print(f"❌ Ошибка при отправке email: {e}")
        raise

# === TASKS ===

extract_cars = PythonOperator(task_id='extract_cars', python_callable=extract_cars_data, dag=dag)
extract_claims = PythonOperator(task_id='extract_claims', python_callable=extract_claims_data, dag=dag)
extract_mileage = PythonOperator(task_id='extract_mileage', python_callable=extract_mileage_data, dag=dag)

transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load = PythonOperator(task_id='load_to_database', python_callable=load_to_database, dag=dag)
report = PythonOperator(task_id='generate_report', python_callable=generate_report, dag=dag)
email = PythonOperator(task_id='send_email_notification', python_callable=send_email_with_attachments, dag=dag)

# === ЗАВИСИМОСТИ ===

[extract_cars, extract_claims, extract_mileage] >> transform
transform >> load >> report >> email