from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app, get_kafka_manager

manager = get_kafka_manager()

manager.connect = MagicMock()
manager.close = MagicMock()
manager.send_prediction = MagicMock()

client = TestClient(app)

def test_predict_endpoint_success():
    manager.send_prediction.reset_mock()

    text_input = "Amazing beauty product, highly recommend!"
    response = client.post(
        "/predict",
        json={
            "full_text": text_input
        }
    )
    
    assert response.status_code == 200
    
    data = response.json()
    assert "predicted_rating" in data
    assert isinstance(data["predicted_rating"], (int, float))
    assert 1 <= data["predicted_rating"] <= 5

    manager.send_prediction.assert_called_once_with(
        text_input, data["predicted_rating"]
    )

def test_predict_endpoint_validation_error():
    manager.send_prediction.reset_mock()

    response = client.post(
        "/predict",
        json={
            "text": "Excellent!"
        }
    )
    
    # 422 Unprocessable Entity
    assert response.status_code == 422

    manager.send_prediction.assert_not_called()