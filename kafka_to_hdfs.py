
import json
import subprocess
from pathlib import Path
from kafka import KafkaConsumer

PROJECT_DIR = Path(__file__).parent
OUTPUT_DIR = PROJECT_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "kafka_hotel_booking_full.jsonl"

TOPIC = "hotel_booking_full"
BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_CMD = r"C:\hadoop-3.5.0\bin\hdfs.cmd"
HDFS_DIR = "/hotel_booking/full_from_kafka"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Dang doc toan bo du lieu tu Kafka...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda data: json.loads(
            data.decode("utf-8")
        ),
        consumer_timeout_ms=10000
    )

    count = 0

    try:
        with OUTPUT_FILE.open("w", encoding="utf-8") as file:
            for message in consumer:
                file.write(
                    json.dumps(message.value, ensure_ascii=False) + "\n"
                )
                count += 1

                if count % 10000 == 0:
                    print(f"Da nhan: {count} ban ghi")

    finally:
        consumer.close()

    print(f"Tong so ban ghi doc tu Kafka: {count}")

    if count == 0:
        print("Khong co du lieu. Chua ghi len HDFS.")
        return

    print("Dang ghi file len HDFS...")

    subprocess.run(
        [HDFS_CMD, "dfs", "-mkdir", "-p", HDFS_DIR],
        check=True
    )

    subprocess.run(
        [HDFS_CMD, "dfs", "-put", "-f",
         str(OUTPUT_FILE), HDFS_DIR],
        check=True
    )

    print(f"Hoan tat! File da duoc ghi len HDFS: {HDFS_DIR}")


if __name__ == "__main__":
    main()
