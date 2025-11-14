"""
Генератор тестовых данных для варианта 21: Анализ страховых выплат по брендам автомобилей
Создаёт:
- cars.csv        → car_id, brand, year
- claims.xlsx     → car_id, damage_amount
- mileage.json    → car_id, mileage
"""

import pandas as pd
import numpy as np
import json
import os
import random

np.random.seed(42)
random.seed(42)

def generate_cars_data():
    brands = ["Toyota", "BMW", "Mercedes", "Audi", "Volkswagen", "Ford", "Honda", "Hyundai", "Kia", "Nissan"]
    cars = []
    for i in range(1, 101):  # 100 автомобилей
        brand = random.choice(brands)
        year = random.randint(2015, 2024)
        cars.append({
            "car_id": f"C{i:03d}",
            "brand": brand,
            "year": year
        })
    df = pd.DataFrame(cars)
    df.to_csv("dags/data/cars.csv", index=False, encoding='utf-8')
    print("✅ cars.csv создан")
    return cars

def generate_claims_data(cars):
    claims = []
    for car in cars:
        # У 70% автомобилей есть страховой случай
        if random.random() < 0.7:
            # Средняя выплата зависит от бренда (у премиум-брендов — выше)
            brand_base = {
                "BMW": (150_000, 500_000),
                "Mercedes": (160_000, 550_000),
                "Audi": (140_000, 480_000),
                "Toyota": (80_000, 250_000),
                "Honda": (75_000, 230_000),
                "Ford": (90_000, 280_000),
                "Volkswagen": (100_000, 300_000),
                "Hyundai": (70_000, 220_000),
                "Kia": (65_000, 210_000),
                "Nissan": (85_000, 260_000)
            }
            brand = car["brand"]
            min_amt, max_amt = brand_base.get(brand, (50_000, 200_000))
            damage = round(np.random.uniform(min_amt, max_amt), 2)
            claims.append({
                "car_id": car["car_id"],
                "damage_amount": damage
            })
    df = pd.DataFrame(claims)
    df.to_excel("dags/data/claims.xlsx", index=False)
    print("✅ claims.xlsx создан")
    return claims

def generate_mileage_data(cars):
    mileage_data = []
    for car in cars:
        # Пробег: от 10 000 до 300 000 км
        mileage = random.randint(10_000, 300_000)
        mileage_data.append({
            "car_id": car["car_id"],
            "mileage": mileage
        })
    with open("dags/data/mileage.json", "w", encoding="utf-8") as f:
        json.dump(mileage_data, f, ensure_ascii=False, indent=2)
    print("✅ mileage.json создан")
    return mileage_data

def main():
    print("Генерация данных для варианта 21: Страховые выплаты по брендам...")
    if not os.path.exists("dags/data"):
        os.makedirs("dags/data")
        print("✅ Папка dags/data создана")
    
    cars = generate_cars_data()
    generate_claims_data(cars)
    generate_mileage_data(cars)
    
    print("\n✅ Все файлы готовы для Airflow!")

if __name__ == "__main__":
    main()