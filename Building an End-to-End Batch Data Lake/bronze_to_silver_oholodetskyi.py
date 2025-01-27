import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# import os
# os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Sanr\\anaconda3\\envs\\pyspark_env\\python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Sanr\\anaconda3\\envs\\pyspark_env\\python.exe'


"""
=== ЕТАП 2: bronze to silver ===
2.1 Зчитати parquet із bronze/
2.2 Виконати чистку тексту у всіх string-колонках
2.3 Дедублікація
2.4 Запис у silver/
"""

# Шляхи до bronze
BRONZE_DIR_BIO = "bronze/athlete_bio"
BRONZE_DIR_RESULTS = "bronze/athlete_event_results"
# Шляхи до silver
SILVER_DIR_BIO = "silver/athlete_bio"
SILVER_DIR_RESULTS = "silver/athlete_event_results"

def clean_text(text: str) -> str:
    """
    Очищає текст від небажаних символів,
    лишаючи a-z, A-Z, 0-9, ., ',', ", ' і пробіли.
    """
    return re.sub(r"[^a-zA-Z0-9,\.\'\"\s]", "", str(text))

def main():
    spark = SparkSession.builder \
        .appName("bronze_to_silver_oholodetskyi") \
        .getOrCreate()

    # [2.1] Зчитуємо parquet із bronze
    print("[bronze_to_silver] Читаємо parquet з bronze...")
    df_bio = spark.read.parquet(BRONZE_DIR_BIO)
    df_results = spark.read.parquet(BRONZE_DIR_RESULTS)

    print("[bronze_to_silver] Приклад даних (bio):")
    df_bio.show(5)
    print("[bronze_to_silver] Приклад даних (results):")
    df_results.show(5)

    # [2.2] Очищення тексту
    clean_text_udf = udf(clean_text, StringType())
    for cname, ctype in df_bio.dtypes:
        if ctype == "string":
            df_bio = df_bio.withColumn(cname, clean_text_udf(col(cname)))
    for cname, ctype in df_results.dtypes:
        if ctype == "string":
            df_results = df_results.withColumn(cname, clean_text_udf(col(cname)))

    # [2.3] Дедублікація
    original_bio_count = df_bio.count()
    original_results_count = df_results.count()
    df_bio_dedup = df_bio.dropDuplicates()
    df_results_dedup = df_results.dropDuplicates()
    print(f"[bronze_to_silver] BIO: {original_bio_count} => {df_bio_dedup.count()} (без дублів)")
    print(f"[bronze_to_silver] RESULTS: {original_results_count} => {df_results_dedup.count()} (без дублів)")

    # [2.4] Запис у silver
    df_bio_dedup.write.mode("overwrite").parquet(SILVER_DIR_BIO)
    df_results_dedup.write.mode("overwrite").parquet(SILVER_DIR_RESULTS)

    print("[bronze_to_silver] Приклад фінального bio:")
    df_bio_dedup.show(5)
    print("[bronze_to_silver] Приклад фінального results:")
    df_results_dedup.show(5)

    spark.stop()
    print("[bronze_to_silver] Етап 2 завершено успішно!")

if __name__ == "__main__":
    main()
