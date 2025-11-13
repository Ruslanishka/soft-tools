"""
Генератор тестовых данных для задания 21: Прогнозируемый доход фермерских хозяйств
Создает три файла:
1. fermers.csv - фермерские хозяйства
2. urozhay.xlsx - урожай по хозяйствам
3. ceny.json - цены на культуры
"""

import pandas as pd
import numpy as np
import json
import os
import random

np.random.seed(42)
random.seed(42)

def generate_farms_data():
    """Генерация данных о фермерских хозяйствах (CSV)"""
    crop_types = ["Пшеница", "Кукуруза", "Картофель", "Свекла", "Подсолнечник", "Овёс", "Рожь", "Соя"]
    farms = []
    for i in range(1, 101):  # 100 ферм
        crop = random.choice(crop_types)
        farm = {
            "farm_id": f"F{i:03d}",
            "farm_name": f"Хозяйство '{crop}'-{i}",
            "crop_type": crop
        }
        farms.append(farm)
    
    df = pd.DataFrame(farms)
    df.to_csv('data/fermers.csv', index=False, encoding='utf-8')
    print("✓ Файл fermers.csv создан")
    return farms, crop_types

def generate_harvest_data(farms):
    """Генерация данных об урожае (Excel)"""
    harvests = []
    for farm in farms:
        # Урожай зависит от культуры (в тоннах)
        base_yield = {
            "Пшеница": (30, 60),
            "Кукуруза": (40, 80),
            "Картофель": (150, 300),
            "Свекла": (200, 400),
            "Подсолнечник": (20, 40),
            "Овёс": (25, 50),
            "Рожь": (20, 45),
            "Соя": (35, 70)
        }
        min_y, max_y = base_yield[farm["crop_type"]]
        tonnes = round(np.random.uniform(min_y, max_y), 1)
        harvests.append({
            "farm_id": farm["farm_id"],
            "harvest_tonnes": tonnes
        })
    
    df = pd.DataFrame(harvests)
    df.to_excel('data/urozhay.xlsx', index=False)
    print("✓ Файл urozhay.xlsx создан")
    return harvests

def generate_prices_data(crop_types):
    """Генерация данных о ценах на рынке (JSON)"""
    prices = []
    for crop in crop_types:
        # Цена за тонну в рублях
        base_price = {
            "Пшеница": (12000, 18000),
            "Кукуруза": (10000, 16000),
            "Картофель": (8000, 14000),
            "Свекла": (7000, 12000),
            "Подсолнечник": (25000, 35000),
            "Овёс": (9000, 13000),
            "Рожь": (10000, 15000),
            "Соя": (20000, 30000)
        }
        min_p, max_p = base_price[crop]
        price = round(np.random.uniform(min_p, max_p), 2)
        prices.append({
            "crop_type": crop,
            "price_per_tonne": price
        })
    
    with open('data/ceny.json', 'w', encoding='utf-8') as f:
        json.dump(prices, f, ensure_ascii=False, indent=2)
    print("✓ Файл ceny.json создан")
    return prices

def main():
    print("Генерация тестовых данных для фермерских хозяйств (вариант 21)...")
    print("=" * 60)
    
    if not os.path.exists('data'):
        os.makedirs('data')
        print("✓ Создана папка 'data'")
    
    farms, crop_types = generate_farms_data()
    generate_harvest_data(farms)
    generate_prices_data(crop_types)
    
    print("=" * 60)
    print("Все файлы готовы для анализа в Jupyter!")

if __name__ == "__main__":
    main()