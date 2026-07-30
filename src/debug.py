import os

from kafka import KafkaConsumer

import report_pb2


project = os.environ.get("PROJECT", "p7")
kafka_server = f"{project}-kafka:9092"

consumer = KafkaConsumer(
    "stock_prices",
    bootstrap_servers=kafka_server,
    group_id="debug",
)

for message in consumer:
    report = report_pb2.Report()
    report.ParseFromString(message.value)

    print({
        "ticker": report.ticker,
        "date": report.date,
        "price": f"{report.price:.4f}",
        "partition": message.partition,
    })
