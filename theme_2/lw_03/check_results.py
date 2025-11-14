#!/usr/bin/env python3
"""
Скрипт для проверки результатов анализа страховых выплат по брендам
"""

import sqlite3
import pandas as pd
import os
import subprocess
import sys

DB_PATH = 'car_insurance_analysis.db'
CONTAINER_DB_PATH = '/opt/airflow/car_insurance_analysis.db'

def check_docker_container():
    try:
        result = subprocess.run(['sudo', 'docker', 'ps', '--format', '{{.Names}}'], 
                              capture_output=True, text=True, check=True)
        containers = result.stdout.strip().split('\n')
        scheduler_containers = [c for c in containers if 'scheduler' in c]
        return scheduler_containers[0] if scheduler_containers else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def copy_db_from_container(container_name):
    try:
        print(f"Копируем базу данных из контейнера {container_name}...")
        subprocess.run([
            'sudo', 'docker', 'cp', 
            f'{container_name}:{CONTAINER_DB_PATH}', 
            DB_PATH
        ], check=True)
        print("✅ База данных скопирована")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка: {e}")
        return False

def check_database():
    if not os.path.exists(DB_PATH):
        container = check_docker_container()
        if not container or not copy_db_from_container(container):
            print("Не удалось получить базу данных.")
            return

    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM brand_insurance_analysis ORDER BY avg_claim_amount DESC", conn)
        conn.close()

        print("РЕЗУЛЬТАТЫ АНАЛИЗА СТРАХОВЫХ ВЫПЛАТ ПО БРЕНДАМ")
        print("=" * 70)
        print(df.to_string(index=False))
        print(f"\n🏆 Бренд с самыми высокими средними выплатами: {df.iloc[0]['brand']}")
        print(f"   Средняя выплата: {df.iloc[0]['avg_claim_amount']:,.2f} руб.")

    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    check_database()