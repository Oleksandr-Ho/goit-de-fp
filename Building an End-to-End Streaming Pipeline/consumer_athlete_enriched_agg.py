from kafka import KafkaConsumer
import json

"""
Споживач (Consumer), який читає з топіка oholodetskyi_athlete_enriched_agg
та виводить повідомлення на екран.

Запускається, коли вже працює стрімінг (streaming_athlete_enriched_agg.py),
або принаймні коли якісь дані вже були відправлені у цей топік.
"""

KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASS = "VawEzo1ikLtrA8Ug8THa"
KAFKA_TOPIC_OUT = "oholodetskyi_athlete_enriched_agg"

consumer = KafkaConsumer(
    KAFKA_TOPIC_OUT,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASS,
    auto_offset_reset="earliest",   # щоб читати з початку
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode("utf-8"),
    group_id="oholodetskyi_athlete_enriched_agg_consumer_group"
)

print(f"Підписалися на топік: {KAFKA_TOPIC_OUT}")
print("Очікуємо повідомлень... Натисніть Ctrl+C для завершення.\n")

try:
    for message in consumer:
        # message.value - це JSON-рядок
        print("Отримано повідомлення з Kafka:")
        print(json.dumps(json.loads(message.value), indent=4, ensure_ascii=False))
        print("-" * 50)
except KeyboardInterrupt:
    print("Зупинено користувачем (CTRL+C)")
finally:
    consumer.close()
