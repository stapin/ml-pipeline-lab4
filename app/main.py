from functools import lru_cache
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager

from app.schemas import ReviewRequest, ReviewResponse
from app.model import Predictor
from app.kafka_producer_manager import KafkaProducerManager


@lru_cache()
def get_predictor() -> Predictor:
    return Predictor()

@lru_cache()
def get_kafka_manager() -> KafkaProducerManager:
    return KafkaProducerManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = get_kafka_manager()
    manager.connect()
    yield
    manager.close()

app = FastAPI(
    title="Amazon Beauty Rating Predictor API",
    description="API для предсказания рейтинга товара по тексту отзыва",
    lifespan=lifespan
)

@app.post("/predict", response_model=ReviewResponse)
def predict_rating(
    request: ReviewRequest,
    predictor: Predictor = Depends(get_predictor),
    kafka_manager: KafkaProducerManager = Depends(get_kafka_manager)
):
    input_dict = request.model_dump() 
    rating = predictor.predict(input_dict)

    kafka_manager.send_prediction(input_dict['full_text'], rating)

    return ReviewResponse(predicted_rating=rating)