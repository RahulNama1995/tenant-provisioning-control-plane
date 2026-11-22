from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app import consumer


VALID_BODY = (
    b'{"update_id":"11111111-1111-1111-1111-111111111111",'
    b'"task_id":"22222222-2222-2222-2222-222222222222",'
    b'"status":"done"}'
)


@pytest.fixture
def rabbit_channel():
    return MagicMock()


@pytest.fixture
def rabbit_method():
    return SimpleNamespace(delivery_tag=123)


@pytest.fixture
def rabbit_properties():
    return SimpleNamespace(headers={})


def test_successful_progress_update_is_acknowledged(
    rabbit_channel, rabbit_method, rabbit_properties
):
    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(consumer, "apply_progress") as apply_progress,
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, rabbit_properties, VALID_BODY
        )

    apply_progress.assert_called_once()
    rabbit_channel.basic_ack.assert_called_once_with(123)
    rabbit_channel.basic_nack.assert_not_called()


def test_invalid_json_is_saved_as_dead_letter(rabbit_channel, rabbit_method, rabbit_properties):
    with patch.object(consumer, "save_dead_letter") as save_dead_letter:
        consumer.process_progress_message(
            rabbit_channel,
            rabbit_method,
            rabbit_properties,
            b"{invalid-json",
        )

    save_dead_letter.assert_called_once()
    rabbit_channel.basic_ack.assert_called_once_with(123)


def test_poison_message_is_requeued_if_dead_letter_persistence_fails(
    rabbit_channel, rabbit_method, rabbit_properties
):
    with patch.object(
        consumer, "save_dead_letter", side_effect=RuntimeError("database unavailable")
    ):
        consumer.process_progress_message(
            rabbit_channel,
            rabbit_method,
            rabbit_properties,
            b"{invalid-json",
        )

    rabbit_channel.basic_nack.assert_called_once_with(123, requeue=True)
    rabbit_channel.basic_ack.assert_not_called()


def test_transient_database_error_schedules_retry(rabbit_channel, rabbit_method, rabbit_properties):
    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, rabbit_properties, VALID_BODY
        )

    rabbit_channel.basic_publish.assert_called_once()

    call = rabbit_channel.basic_publish.call_args
    assert call.kwargs["routing_key"] == consumer.PROGRESS_RETRY_QUEUE
    assert call.kwargs["properties"].expiration == str(consumer.BASE_RETRY_DELAY_MS)
    assert call.kwargs["properties"].headers["x-retry-count"] == 1

    rabbit_channel.basic_ack.assert_called_once_with(123)


def test_retry_delay_uses_exponential_backoff(rabbit_channel, rabbit_method):
    properties = SimpleNamespace(headers={"x-retry-count": 1})

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(rabbit_channel, rabbit_method, properties, VALID_BODY)

    published_properties = rabbit_channel.basic_publish.call_args.kwargs["properties"]

    assert published_properties.expiration == str(consumer.BASE_RETRY_DELAY_MS * 2)
    assert published_properties.headers["x-retry-count"] == 2

def test_invalid_retry_header_is_handled_safely(
    rabbit_channel, rabbit_method
):
    properties = SimpleNamespace(
        headers={"x-retry-count": "invalid"}
    )

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError(
                "statement", {}, Exception("DB down")
            ),
        ),
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, properties, VALID_BODY
        )

    # The consumer should not crash or acknowledge a message
    # without successfully scheduling its retry.
    assert (
        rabbit_channel.basic_publish.called
        or rabbit_channel.basic_nack.called
    )

def test_exhausted_retries_are_sent_to_dlq(rabbit_channel, rabbit_method):
    properties = SimpleNamespace(headers={"x-retry-count": consumer.MAX_PROGRESS_RETRIES})

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(rabbit_channel, rabbit_method, properties, VALID_BODY)

    call = rabbit_channel.basic_publish.call_args

    assert call.kwargs["routing_key"] == consumer.PROGRESS_DLQ
    assert "x-final-error" in call.kwargs["properties"].headers

    rabbit_channel.basic_ack.assert_called_once_with(123)


def test_retry_publish_failure_requeues_original_message(
    rabbit_channel, rabbit_method, rabbit_properties
):
    rabbit_channel.basic_publish.side_effect = RuntimeError("broker unavailable")

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, rabbit_properties, VALID_BODY
        )

    rabbit_channel.basic_nack.assert_called_once_with(123, requeue=True)
    rabbit_channel.basic_ack.assert_not_called()


def test_permanent_processing_error_is_saved_as_dead_letter(
    rabbit_channel, rabbit_method, rabbit_properties
):
    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=ValueError("invalid task state"),
        ),
        patch.object(consumer, "save_dead_letter") as save_dead_letter,
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, rabbit_properties, VALID_BODY
        )

    save_dead_letter.assert_called_once()
    rabbit_channel.basic_ack.assert_called_once_with(123)


def test_permanent_error_dead_letter_persistence_failure_requeues(
    rabbit_channel, rabbit_method, rabbit_properties
):
    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=ValueError("invalid task state"),
        ),
        patch.object(
            consumer,
            "save_dead_letter",
            side_effect=RuntimeError("database unavailable"),
        ),
    ):
        consumer.process_progress_message(
            rabbit_channel, rabbit_method, rabbit_properties, VALID_BODY
        )

    rabbit_channel.basic_nack.assert_called_once_with(123, requeue=True)
    rabbit_channel.basic_ack.assert_not_called()


def test_dlq_publish_failure_requeues_message(rabbit_channel, rabbit_method):
    properties = SimpleNamespace(headers={"x-retry-count": consumer.MAX_PROGRESS_RETRIES})
    rabbit_channel.basic_publish.side_effect = RuntimeError("broker unavailable")

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(rabbit_channel, rabbit_method, properties, VALID_BODY)

    rabbit_channel.basic_nack.assert_called_once_with(123, requeue=True)
    rabbit_channel.basic_ack.assert_not_called()


def test_retry_headers_preserve_existing_metadata(rabbit_channel, rabbit_method):
    properties = SimpleNamespace(
        headers={
            "x-retry-count": 1,
            "correlation-id": "correlation-123",
        }
    )

    with (
        patch.object(consumer, "SessionLocal"),
        patch.object(
            consumer,
            "apply_progress",
            side_effect=OperationalError("statement", {}, Exception("DB down")),
        ),
    ):
        consumer.process_progress_message(rabbit_channel, rabbit_method, properties, VALID_BODY)

    published_headers = rabbit_channel.basic_publish.call_args.kwargs["properties"].headers

    assert published_headers["x-retry-count"] == 2
    assert published_headers["correlation-id"] == "correlation-123"


def test_consumer_reconnects_after_connection_failure():

    with (
        patch.object(
            consumer,
            "connection",
            side_effect=[RuntimeError("broker unavailable"), KeyboardInterrupt()],
        ),
        patch.object(consumer.time, "sleep") as sleep,
    ):
        with pytest.raises(KeyboardInterrupt):
            consumer.run_consumer()

    sleep.assert_called_once_with(2)


def test_consumer_configures_channel_and_starts_consuming():
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with (
        patch.object(consumer, "connection", return_value=mock_connection),
        patch.object(consumer, "declare_queues") as declare_queues,
        patch.object(consumer.time, "sleep"),
    ):
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            consumer.run_consumer()

    declare_queues.assert_called_once_with(mock_channel)
    mock_channel.confirm_delivery.assert_called_once()
    mock_channel.basic_qos.assert_called_once_with(prefetch_count=10)

    mock_channel.basic_consume.assert_called_once_with(
        queue=consumer.PROGRESS_QUEUE,
        on_message_callback=consumer.process_progress_message,
    )

    mock_channel.start_consuming.assert_called_once()
