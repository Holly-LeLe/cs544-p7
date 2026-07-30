import os
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from sqlalchemy import create_engine, text

import report_pb2


project = os.environ.get("PROJECT", "p7")
kafka_server = f"{project}-kafka:9092"
mysql_host = f"{project}-mysql-1"

topic_name = "stock_prices"


# Connect to Kafka's administrative interface.
admin = KafkaAdminClient(
    bootstrap_servers=kafka_server,
    client_id="stock-price-admin",
)

# Delete the old topic when it exists.
try:
    admin.delete_topics([topic_name])
    print(f"Deleted old topic: {topic_name}")
    time.sleep(3)
except UnknownTopicOrPartitionError:
    print(f"No old topic named {topic_name}")

# Create a fresh topic with four partitions and one replica.
admin.create_topics(
    new_topics=[
        NewTopic(
            name=topic_name,
            num_partitions=4,
            replication_factor=1,
        )
    ]
)

print(f"Created topic: {topic_name}")


# Connect to Kafka for publishing messages.
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    key_serializer=lambda ticker: ticker.encode("utf-8"),
    value_serializer=lambda report: report.SerializeToString(),
    retries=10,
    acks="all",
)


# Connect to MySQL.
engine = create_engine(
    f"mysql+mysqlconnector://root:abc@{mysql_host}:3306/CS544"
)

last_id = 0

# Continuously look for new rows in MySQL.
while True:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, date, price, ticker
                FROM stock_prices
                WHERE id > :last_id
                ORDER BY id
                """
            ),
            {"last_id": last_id},
        )

        found_new_row = False

        for row in rows:
            found_new_row = True

            report = report_pb2.Report(
                date=str(row.date),
                price=float(row.price),
                ticker=row.ticker,
            )

            producer.send(
                topic_name,
                key=row.ticker,
                value=report,
            )

            last_id = row.id

        if found_new_row:
            producer.flush()
        else:
            time.sleep(0.1)
