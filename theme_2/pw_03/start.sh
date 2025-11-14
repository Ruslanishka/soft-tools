#!/bin/bash

echo "Генерация данных для фермерских хозяйств (вариант 21)..."
python3 data_generator.py

echo "Запуск Jupyter Lab..."
sudo docker compose up -d

echo "Готово!"
echo "Jupyter: http://localhost:8888"./