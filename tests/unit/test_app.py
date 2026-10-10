"""Unit tests for the Flask health-check application."""

import pytest
from src.app import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


class TestHealthEndpoint:

    def test_returns_200_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "OK"
        assert data["service"] == "UserActivityProcessor"

    def test_returns_json_content_type(self, client):
        response = client.get("/health")
        assert response.content_type == "application/json"
