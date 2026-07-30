import os
import sys
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from kafka import KafkaConsumer, TopicPartition
from subprocess import check_output

import report_pb2


os.environ["CLASSPATH"] = str(
    check_output(
        [os.environ["HADOOP_HOME"] + "/bin/hdfs",
         "classpath",
         "--glob"]
    ),
    "utf-8",
)

broker = "localhost:9092"
topic_name = "stock_prices"


def main():

    if len(sys.argv) != 2:
        print("Usage: python consumer.py <partition_number>")
        sys.exit(1)

    partition_id = int(sys.argv[1])

    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        enable_auto_commit=False,
        max_poll_records=500,
    )

    topic_partition = TopicPartition(topic_name, partition_id)

    # Manually assign exactly one partition.
    consumer.assign([topic_partition])

    checkpoint_path = f"/src/partition-{partition_id}.json"

    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, "r") as checkpoint_file:
            checkpoint = json.load(checkpoint_file)

        batch_id = checkpoint["batch_id"] + 1
        start_offset = checkpoint["offset"]

        consumer.seek(topic_partition, start_offset)
    else:
        batch_id = 0
        consumer.seek(topic_partition, 0)

    print(
        f"Reading partition {partition_id} "
        f"starting at offset {consumer.position(topic_partition)}"
    )

    # Connect to HDFS and create /data if necessary.
    hdfs = pafs.HadoopFileSystem("boss", 9000)
    hdfs.create_dir("/data", recursive=True)

    while True:
        polled = consumer.poll(timeout_ms=1000)

        records = polled.get(topic_partition, [])

        # Do not create an empty Parquet file.
        if not records:
            continue

        rows = []

        for message in records:
            report = report_pb2.Report()
            report.ParseFromString(message.value)

            rows.append({
                "date": report.date,
                "price": report.price,
                "ticker": report.ticker,
            })

        dataframe = pd.DataFrame(
            rows,
            columns=["date", "price", "ticker"],
        )

        table = pa.Table.from_pandas(
            dataframe,
            preserve_index=False,
        )

        parquet_path = (
            f"/data/partition-{partition_id}-"
            f"batch-{batch_id}.parquet"
        )

        # Overwrite the file if it already exists.
        file_info = hdfs.get_file_info(parquet_path)

        if file_info.type != pafs.FileType.NotFound:
            hdfs.delete_file(parquet_path)

        pq.write_table(
            table,
            parquet_path,
            filesystem=hdfs,
        )

        # position() is the offset of the next message to read.
        current_offset = consumer.position(topic_partition)

        checkpoint = {
            "batch_id": batch_id,
            "offset": current_offset,
        }

        # Write the checkpoint only after Parquet succeeds.
        with open(checkpoint_path, "w") as checkpoint_file:
            json.dump(checkpoint, checkpoint_file, indent=4)

        print(
            f"Wrote {len(rows)} rows to {parquet_path}; "
            f"next offset is {current_offset}"
        )

        batch_id += 1


if __name__ == "__main__":
    main()
