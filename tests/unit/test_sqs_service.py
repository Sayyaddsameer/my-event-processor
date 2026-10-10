"""Unit tests for the SQS service layer."""

import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from src.services.sqs_service import send_message, receive_messages, delete_message


@pytest.fixture
def mock_sqs_client():
    with patch("src.services.sqs_service._get_sqs_client") as mock_get:
        client = MagicMock()
        mock_get.return_value = client
        yield client


class TestSendMessage:

    def test_success(self, mock_sqs_client):
        mock_sqs_client.send_message.return_value = {"MessageId": "msg-123"}
        result = send_message("http://q/url", '{"key": "value"}')
        assert result["MessageId"] == "msg-123"
        mock_sqs_client.send_message.assert_called_once_with(
            QueueUrl="http://q/url", MessageBody='{"key": "value"}'
        )

    def test_with_attributes(self, mock_sqs_client):
        mock_sqs_client.send_message.return_value = {"MessageId": "msg-456"}
        attrs = {"source": {"StringValue": "test", "DataType": "String"}}
        send_message("http://q/url", "body", attrs)
        mock_sqs_client.send_message.assert_called_once_with(
            QueueUrl="http://q/url", MessageBody="body", MessageAttributes=attrs
        )

    def test_client_error_propagates(self, mock_sqs_client):
        mock_sqs_client.send_message.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Internal"}}, "SendMessage"
        )
        with pytest.raises(ClientError):
            send_message("http://q/url", "body")


class TestReceiveMessages:

    def test_returns_messages(self, mock_sqs_client):
        mock_sqs_client.receive_message.return_value = {
            "Messages": [{"MessageId": "m1", "Body": "b1"}]
        }
        assert len(receive_messages("http://q/url", max_messages=5, wait_time=10)) == 1

    def test_empty_response(self, mock_sqs_client):
        mock_sqs_client.receive_message.return_value = {}
        assert receive_messages("http://q/url") == []


class TestDeleteMessage:

    def test_success(self, mock_sqs_client):
        delete_message("http://q/url", "receipt-handle")
        mock_sqs_client.delete_message.assert_called_once_with(
            QueueUrl="http://q/url", ReceiptHandle="receipt-handle"
        )

    def test_client_error_propagates(self, mock_sqs_client):
        mock_sqs_client.delete_message.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "err"}}, "DeleteMessage"
        )
        with pytest.raises(ClientError):
            delete_message("http://q/url", "rh")
