import os
import json
from kafka import KafkaConsumer
from app.database import OracleDBManager

class KafkaPredictionConsumer:
    def __init__(self):
        self.db_manager = OracleDBManager()
        
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "predictions")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "oracle_writer_group")
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=self.group_id,
            auto_offset_reset='earliest'
        )

    def start(self):
        print("Database initialization...")
        self.db_manager.init_database()
        print("Database was successfully initialized.")

        print(f"Consumer started. Listen topic '{self.topic}'...")
        try:
            for message in self.consumer:
                self._process_message(message.value)
        except KeyboardInterrupt:
            print("Consumer остановлен пользователем.")
        finally:
            self.consumer.close()

    def _process_message(self, data: dict):
        text = data.get("text_content")
        rating = data.get("predicted_rating")

        if text is None or rating is None:
            print(f"Format message error: {data}")
            return

        try:
            record_id = self.db_manager.save_prediction(text, rating)
            print(f"Prediction saved. ID: {record_id}, Text: '{text[:20]}...'")
        except Exception as e:
            print(f"Error while saving to db: {e}")

if __name__ == "__main__":
    consumer_service = KafkaPredictionConsumer()
    consumer_service.start()