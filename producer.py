
import csv
import json
from pathlib import Path
from kafka import KafkaProducer

DATA_FILE = Path(__file__).parent / "data" / "hotel_bookings.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "hotel_booking_full"


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Khong tim thay file: {DATA_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda row: json.dumps(
            row, ensure_ascii=False
        ).encode("utf-8"),
        acks="all"
    )

    sent = 0

    try:
        with DATA_FILE.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                producer.send(TOPIC, value=row)
                sent += 1

                if sent % 10000 == 0:
                    print(f"Da gui: {sent} ban ghi")

        producer.flush()
        print(f"Da gui xong {sent} ban ghi vao Kafka.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
