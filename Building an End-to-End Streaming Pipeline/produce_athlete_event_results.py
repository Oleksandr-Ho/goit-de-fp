import uuid
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

"""
Цей скрипт реалізує частину Етапу 3:
- Зчитує дані з MySQL-таблиці olympic_dataset.athlete_event_results
- Відправляє (публікує) їх у Kafka-топік: oholodetskyi_athlete_event_results
"""

# --- КОНФІГУРАЦІЯ MySQL ---
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "neo_data"
MYSQL_USER = "neo_data_admin"
MYSQL_PASS = "Proyahaxuqithab9oplp"
MYSQL_TABLE_EVENT_RESULTS = "olympic_dataset.athlete_event_results"

# --- КОНФІГУРАЦІЯ KAFKA ---
KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASS = "VawEzo1ikLtrA8Ug8THa"
KAFKA_TOPIC = "oholodetskyi_athlete_event_results"

# Етап 3 (частина перша): Зчитування з MySQL
# ------------------------------------------
# Створюємо сесію Spark з підвантаженням jdbc-драйвера
spark = SparkSession.builder \
    .appName("MySQLToKafka") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# Читаємо дані з таблиці athlete_event_results
df_event_results = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", MYSQL_TABLE_EVENT_RESULTS) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASS) \
    .load()

print("Перші 10 рядків з таблиці athlete_event_results:")
df_event_results.show(10)

# Етап 3 (частина друга): Надсилання даних у Kafka-топік
# -----------------------------------------------------
df_for_kafka = df_event_results.select(
    "edition",
    "edition_id",
    "country_noc",
    "sport",
    "event",
    "result_id",
    "athlete",
    "athlete_id",
    "pos",
    "medal",
    "isTeamSport"
)

# Створюємо Kafka Producer з використанням kafka-python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Відправляємо всі рядки DataFrame в топік
rows = df_for_kafka.collect()

for row in rows:
    # перетворюємо pyspark Row -> dict
    message_value = row.asDict()
    # ключ можна зробити з athlete_id або UUID
    message_key = str(uuid.uuid4())
    producer.send(KAFKA_TOPIC, key=message_key, value=message_value)

producer.flush()
producer.close()
print(f"Дані з таблиці '{MYSQL_TABLE_EVENT_RESULTS}' успішно відправлено у Kafka-топік '{KAFKA_TOPIC}'.")

# Завершення
spark.stop()
