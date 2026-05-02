import os
import json
from kafka import KafkaProducer

class KafkaProducerManager:
    def __init__(self):
        self.servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "predictions")
        self.producer = None

    def connect(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Kafka Producer подключен к {self.servers}")
    
    def close(self):
        if self.producer:
            self.producer.close()

    def send_prediction(self, text: str, rating: float):
        message = {
            "text_content": text,
            "predicted_rating": rating
        }
        self.producer.send(self.topic, message)
        self.producer.flush() # Гарантируем отправку
