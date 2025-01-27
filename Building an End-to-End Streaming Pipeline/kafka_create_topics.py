##############################################################################
# kafka_create_topics.py
#
# Модифікований варіант:
#   1) Видаляє топіки (якщо існують),
#   2) Заново створює їх.
#
##############################################################################
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError

"""
Цей скрипт працює з двома Kafka-топіками:
1) oholodetskyi_athlete_event_results
2) oholodetskyi_athlete_enriched_agg

Порядок дій:
    - Видаляє топіки, якщо вони існують (і видалення дозволене).
    - Створює топіки знову.
"""

# Етап: Підготовка конфігурації Kafka
KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASS = "VawEzo1ikLtrA8Ug8THa"

# Загальні налаштування для з'єднання з Kafka
kafka_admin_config = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": KAFKA_USER,
    "sasl_plain_password": KAFKA_PASS,
}

def delete_topic(topic_name):
    """
    Функція для видалення Kafka-топіка з іменем topic_name,
    якщо він існує і якщо на брокері дозволене видалення топіків
    (delete.topic.enable=true).
    """
    admin_client = KafkaAdminClient(**kafka_admin_config)
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        print(f"Топік '{topic_name}' не існує, пропускаємо видалення.")
        admin_client.close()
        return

    try:
        admin_client.delete_topics([topic_name])
        print(f"Топік '{topic_name}' успішно видалено (запит на видалення надіслано).")
    except UnknownTopicOrPartitionError:
        # Якщо все ж таки брокер вважає, що топіка не існує.
        print(f"Топік '{topic_name}' не існує або вже видалено.")
    except Exception as e:
        print(f"Помилка видалення топіка '{topic_name}': {e}")
    finally:
        admin_client.close()

def create_topic(topic_name):
    """
    Функція для створення Kafka-топіка з іменем topic_name.
    Якщо такий топік уже існує, функція це виведе в консоль.
    """
    admin_client = KafkaAdminClient(**kafka_admin_config)

    topic = NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1
    )

    existing_topics = admin_client.list_topics()
    if topic_name in existing_topics:
        print(f"Топік '{topic_name}' вже існує (не створюємо вдруге).")
        admin_client.close()
        return

    try:
        admin_client.create_topics([topic])
        print(f"Топік '{topic_name}' успішно створено.")
    except Exception as e:
        print(f"Помилка створення топіка '{topic_name}': {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    # Наші два топіки:
    topics_to_recreate = [
        "oholodetskyi_athlete_event_results",
        "oholodetskyi_athlete_enriched_agg"
    ]

    # Спочатку видаляємо їх, потім створюємо заново
    for topic in topics_to_recreate:
        delete_topic(topic)

    for topic in topics_to_recreate:
        create_topic(topic)
