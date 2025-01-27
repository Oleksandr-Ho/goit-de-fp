import requests
import os
from pyspark.sql import SparkSession

"""
=== ЕТАП 1: landing to bronze ===
1.1 Завантажити CSV-файли з FTP (athlete_bio.csv, athlete_event_results.csv) 
1.2 Прочитати CSV і зберегти у форматі Parquet у папки: bronze/athlete_bio, bronze/athlete_event_results
"""

# Назви локальних CSV-файлів, що будемо створювати після завантаження з FTP
CSV_BIO = "athlete_bio.csv"
CSV_RESULTS = "athlete_event_results.csv"

# Папки, куди запишемо parquet
BRONZE_DIR_BIO = "bronze/athlete_bio"
BRONZE_DIR_RESULTS = "bronze/athlete_event_results"

def download_data_from_ftp(table_name: str):
    """
    Завантажуємо CSV із FTP-сервера: https://ftp.goit.study/neoversity/<table_name>.csv
    Зберігаємо під назвою <table_name>.csv
    """
    base_url = "https://ftp.goit.study/neoversity/"
    downloading_url = base_url + table_name + ".csv"
    print(f"[landing_to_bronze] Завантажуємо файл із: {downloading_url}")

    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(table_name + ".csv", 'wb') as f:
            f.write(response.content)
        print(f"[landing_to_bronze] Успішно збережено: {table_name}.csv")
    else:
        print(f"[landing_to_bronze] ПОМИЛКА завантаження FTP! Код: {response.status_code}")

def main():
    # [1.1] Завантаження з FTP
    download_data_from_ftp("athlete_bio")
    download_data_from_ftp("athlete_event_results")

    # Ініціалізуємо SparkSession
    spark = SparkSession.builder \
        .appName("landing_to_bronze_oholodetskyi") \
        .getOrCreate()

    # [1.2] Зчитуємо CSV та записуємо у parquet (bronze)
    print("[landing_to_bronze] Читаємо athlete_bio.csv ...")
    df_bio = spark.read.option("header", True).csv(CSV_BIO)
    print("[landing_to_bronze] Приклад даних (df_bio):")
    df_bio.show(5)
    print("[landing_to_bronze] Загальна кількість рядків:", df_bio.count())

    df_bio.write.mode("overwrite").parquet(BRONZE_DIR_BIO)
    print(f"[landing_to_bronze] Записано Parquet у {BRONZE_DIR_BIO}")

    # Те ж для athlete_event_results
    df_results = spark.read.option("header", True).csv(CSV_RESULTS)
    print("[landing_to_bronze] Приклад даних (df_results):")
    df_results.show(5)
    print("[landing_to_bronze] Загальна кількість рядків:", df_results.count())

    df_results.write.mode("overwrite").parquet(BRONZE_DIR_RESULTS)
    print(f"[landing_to_bronze] Записано Parquet у {BRONZE_DIR_RESULTS}")

    spark.stop()
    print("[landing_to_bronze] Етап 1 завершено успішно!")

if __name__ == "__main__":
    main()
