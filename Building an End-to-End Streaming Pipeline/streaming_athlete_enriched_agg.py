import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

"""
Цей скрипт реалізує стрімінг-пайплайн для збагачення та агрегування даних (Етапи 1-6).

1) Етап 1: Зчитуємо з MySQL таблиці athlete_bio
2) Етап 2: Фільтруємо атлетів, де зріст/вага некоректні (пусті, нечислові)
3) Етап 3: Зчитуємо потік (stream) із Kafka-топіка oholodetskyi_athlete_event_results
   (дані з JSON конвертуються в DataFrame)
4) Етап 4: З'єднуємо (JOIN) стрімінгові дані з біологічними за athlete_id
5) Етап 5: Обчислюємо середній зріст/вагу групами (sport, medal, sex, country_noc) + додаємо timestamp
6) Етап 6: Вивантажуємо результат у 2 місця:
   a) Kafka-топік "oholodetskyi_athlete_enriched_agg" (і друкуємо у консоль)
   b) MySQL-таблицю "oholodetskyi.athlete_enriched_agg"
"""


# =======================
# КОНФІГИ ДЛЯ MySQL
# =======================
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "neo_data"
MYSQL_USER = "neo_data_admin"
MYSQL_PASS = "Proyahaxuqithab9oplp"
MYSQL_TABLE_BIO = "olympic_dataset.athlete_bio"

# Схема/база та таблиця для запису агрегованих результатів
TARGET_MYSQL_SCHEMA = "oholodetskyi"
TARGET_MYSQL_TABLE = "athlete_enriched_agg"

# =======================
# КОНФІГИ ДЛЯ KAFKA
# =======================
KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASS = "VawEzo1ikLtrA8Ug8THa"

# Вхідний та вихідний топіки
TOPIC_IN = "oholodetskyi_athlete_event_results"
TOPIC_OUT = "oholodetskyi_athlete_enriched_agg"

# ================================================
# Налаштування середовища для Spark Streaming
# ================================================
# Вказуємо через os.environ додаткові опції:
# 1) Зв'язка з Kafka (spark-sql-kafka)
# 2) MySQL-connector

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars mysql-connector-j-8.0.32.jar "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)

# Створюємо SparkSession
spark = SparkSession.builder \
    .appName("StreamingAthleteEnrichedAgg") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# -----------------------------------------------------
# Етап 1: Зчитуємо з MySQL таблиці athlete_bio
# -----------------------------------------------------
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
df_bio = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", MYSQL_TABLE_BIO) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASS) \
    .load()

print("Етап 1: Зчитали дані з таблиці athlete_bio (MySQL). Приклад 10 рядків:")
df_bio.show(10)

# -----------------------------------------------------
# Етап 2: Фільтруємо, де зріст/вага порожні або нечислові
# -----------------------------------------------------

df_bio_filtered = df_bio \
    .filter(
        (col("height").cast("float").isNotNull()) &
        (col("weight").cast("float").isNotNull())
    )

print("Етап 2: Кількість рядків до фільтра:", df_bio.count())
print("Етап 2: Кількість рядків після фільтра:", df_bio_filtered.count())

# -----------------------------------------------------
# Етап 3: Зчитуємо дані з Kafka-топіка (json -> DF)
# -----------------------------------------------------
# Схема JSON, яку ми очікуємо після produce_athlete_event_results
json_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", DoubleType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", DoubleType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", DoubleType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True),
])

# Стрімінгове зчитування
df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_IN) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASS}";') \
    .load()

# Розпарсити (deserialize) значення message.value з JSON у стовпчики
df_parsed = df_kafka_raw \
    .select(
        from_json(
            col("value").cast("string"),
            json_schema
        ).alias("json_parsed")
    ) \
    .select("json_parsed.*")

# -----------------------------------------------------
# Етап 4: JOIN з біоданими (df_bio_filtered) за athlete_id
# -----------------------------------------------------
df_joined = df_parsed.join(
    df_bio_filtered,
    df_parsed.athlete_id == df_bio_filtered.athlete_id,
    "inner"  # або "left", залежно від потреб
)

# Для уникнення колізії імен беремо потрібні колонки
df_joined_clean = df_joined.select(
    df_parsed["sport"],
    df_parsed["medal"],
    df_bio_filtered["sex"],
    df_bio_filtered["country_noc"],
    df_bio_filtered["height"].cast("float").alias("height"),
    df_bio_filtered["weight"].cast("float").alias("weight")
)

# -----------------------------------------------------
# Етап 5: Знайти середній зріст та вагу (groupBy) + timestamp
# -----------------------------------------------------
# Групуємо за (sport, medal, sex, country_noc) і обчислюємо середні величини
from pyspark.sql.functions import avg

df_agg = df_joined_clean.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn("timestamp", current_timestamp())

# -----------------------------------------------------
# Етап 6: Створимо мікробатч (forEachBatch) -> Kafka + MySQL
# -----------------------------------------------------
def foreach_batch_function(batch_df, batch_id):
    print(f"\n==== Обробка мікробатчу: batch_id={batch_id} ====")

    # 6.a) Запис у вихідний Kafka-топік oholodetskyi_athlete_enriched_agg
    #    + Вивід на екран
    #    Спочатку підготуємо колонки 'key' та 'value' для Kafka
    prepared_for_kafka = batch_df \
        .withColumn(
            "value",
            to_json(struct(*batch_df.columns))
        ) \
        .withColumn(
            "key",
            lit("athlete_enriched_agg_key")
        )

    # Виводимо на екран (для наочності) обмежену кількість рядків
    print("---- Дані, що відправляються до Kafka ----")
    prepared_for_kafka.select("value").show(truncate=False)

    prepared_for_kafka \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASS}";') \
        .option("topic", TOPIC_OUT) \
        .save()

    # 6.b) Запис у MySQL таблицю oholodetskyi.athlete_enriched_agg
    target_jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{TARGET_MYSQL_SCHEMA}"
    batch_df.write \
        .format("jdbc") \
        .option("url", target_jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", f"{TARGET_MYSQL_SCHEMA}.{TARGET_MYSQL_TABLE}") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASS) \
        .mode("append") \
        .save()

# Запускаємо стрімінг з outputMode("update") або "complete" (залежить від агрегації)
# Тут "update" підійде, бо ми маємо агрегацію за групами
query = df_agg.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "./checkpoint_streaming_athlete_enriched_agg") \
    .start()

print("=== Стрімінг запущено. Очікуємо дані з Kafka-топіка та виконуємо агрегування... ===")
query.awaitTermination()
