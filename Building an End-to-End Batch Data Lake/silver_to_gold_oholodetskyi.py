from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp

##############################################################################
#  ЕТАП 3: silver to gold
#    3.1 Зчитуємо 2 parquet з silver/
#    3.2 JOIN за ОДНИМ полем (athlete_id)
#    3.3 Групування (sport, medal, sex, country_noc) + avg(weight, height)
#    3.4 Додати колонку timestamp
#    3.5 Запис у gold/avg_stats
##############################################################################

SILVER_DIR_BIO = "silver/athlete_bio"
SILVER_DIR_RESULTS = "silver/athlete_event_results"

GOLD_DIR = "gold/avg_stats"

def main():
    spark = SparkSession.builder \
        .appName("silver_to_gold_oholodetskyi") \
        .getOrCreate()

    # [3.1] Зчитуємо з silver
    df_bio = spark.read.parquet(SILVER_DIR_BIO)
    df_results = spark.read.parquet(SILVER_DIR_RESULTS)

    print("[silver_to_gold] Приклад даних (bio):")
    df_bio.show(5)
    print("[silver_to_gold] Приклад даних (results):")
    df_results.show(5)

    # Перевіряємо, що height/weight у float
    df_bio_casted = df_bio \
        .withColumn("height", col("height").cast("float")) \
        .withColumn("weight", col("weight").cast("float"))

    # [3.2] JOIN лише за athlete_id
    #    Увага: щоб уникнути дублювання колонок country_noc,
    #    беремо "country_noc" з df_results, а "sex", "height", "weight" з df_bio.
    df_joined = df_results.alias("r").join(
        df_bio_casted.alias("b"),
        on="athlete_id",  # Тепер тільки одне поле
        how="inner"
    )

    # Вибираємо необхідні колонки, уникаючи конфліктів:
    # - 'sport', 'medal', 'country_noc' із таблиці results
    # - 'sex', 'height', 'weight' із таблиці bio
    df_joined = df_joined.select(
        col("r.sport").alias("sport"),
        col("r.medal").alias("medal"),
        col("r.country_noc").alias("country_noc"),
        col("b.sex").alias("sex"),
        col("b.height").alias("height"),
        col("b.weight").alias("weight"),
    )

    print("[silver_to_gold] Приклад після JOIN (тільки athlete_id):")
    df_joined.show(5)

    # [3.3] Групування + агрегація (все залишаємо без змін)
    df_agg = df_joined.groupBy(
        "sport", "medal", "sex", "country_noc"
    ).agg(
        avg(col("height")).alias("avg_height"),
        avg(col("weight")).alias("avg_weight")
    )

    # [3.4] Додаємо timestamp
    df_final = df_agg.withColumn("timestamp", current_timestamp())

    print("[silver_to_gold] Приклад після агрегації + timestamp:")
    df_final.show(10)

    # [3.5] Запис у gold
    df_final.write.mode("overwrite").parquet(GOLD_DIR)
    print("[silver_to_gold] Дані збережено у gold/avg_stats")

    spark.stop()
    print("[silver_to_gold] Етап 3 завершено успішно!")

if __name__ == "__main__":
    main()
