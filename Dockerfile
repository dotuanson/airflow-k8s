FROM apache/airflow:2.8.3

RUN pip install --no-cache-dir 'apache-airflow[mongo]' \
    'apache-airflow[google]' \
    gspread
