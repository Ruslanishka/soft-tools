FROM jupyter/scipy-notebook:latest

USER root

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID

# Установка Python библиотек
RUN pip install --no-cache-dir \
    psycopg2-binary \
    sqlalchemy \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    plotly \
    openpyxl \
    xlsxwriter

# Создание рабочих директорий
RUN mkdir -p /home/jovyan/work/data
RUN mkdir -p /home/jovyan/work/notebooks

WORKDIR /home/jovyan/work
