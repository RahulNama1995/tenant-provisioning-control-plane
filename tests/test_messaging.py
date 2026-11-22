from unittest.mock import MagicMock, patch
import pytest
from app import messaging


def test_connection_creates_rabbitmq_connection():
    with (
        patch.object(messaging.pika, "URLParameters") as url_params,
        patch.object(messaging.pika, "BlockingConnection") as blocking_connection,
    ):
        result = messaging.connection()

    url_params.assert_called_once_with(messaging.RABBITMQ_URL)
    blocking_connection.assert_called_once_with(url_params.return_value)
    assert result == blocking_connection.return_value


def test_declare_queues_configures_all_queues():
    channel = MagicMock()

    messaging.declare_queues(channel)

    assert channel.queue_declare.call_count == 5

    declarations = [queue_call.kwargs for queue_call in channel.queue_declare.call_args_list]

    assert declarations[0] == {
        "queue": messaging.TASK_DLQ,
        "durable": True,
    }

    assert declarations[1]["queue"] == messaging.TASK_QUEUE
    assert declarations[1]["durable"] is True
    assert declarations[1]["arguments"]["x-dead-letter-routing-key"] == (messaging.TASK_DLQ)

    assert declarations[2] == {
        "queue": messaging.PROGRESS_QUEUE,
        "durable": True,
    }

    assert declarations[3]["queue"] == messaging.PROGRESS_RETRY_QUEUE
    assert declarations[3]["durable"] is True
    assert declarations[3]["arguments"]["x-dead-letter-routing-key"] == (messaging.PROGRESS_QUEUE)

    assert declarations[4] == {
        "queue": "provisioning.progress.dlq",
        "durable": True,
    }


def test_outbox_publisher_publishes_committed_events():
    outbox_row = MagicMock()
    outbox_row.published_at = None
    outbox_row.payload = {"id": "task-123", "type": "deploy"}

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = [outbox_row]

    channel = MagicMock()
    conn = MagicMock()
    conn.channel.return_value = channel

    with (
        patch.object(messaging, "connection", return_value=conn),
        patch.object(messaging, "declare_queues"),
        patch.object(messaging, "SessionLocal") as session_local,
        patch.object(messaging.time, "sleep", side_effect=KeyboardInterrupt),
    ):
        session_local.return_value.__enter__.return_value = db

        with pytest.raises(KeyboardInterrupt):
            messaging.publish_outbox_forever()

    channel.confirm_delivery.assert_called_once()
    channel.basic_publish.assert_called_once()

    publish_args = channel.basic_publish.call_args.kwargs

    assert publish_args["routing_key"] == messaging.TASK_QUEUE
    assert publish_args["mandatory"] is True
    assert '"task-123"' in publish_args["body"]

    assert outbox_row.published_at is not None
    db.commit.assert_called_once()
    conn.close.assert_called_once()


def test_outbox_publisher_does_not_mark_event_when_publish_fails():
    outbox_row = MagicMock()
    outbox_row.published_at = None
    outbox_row.payload = {"id": "task-123", "type": "deploy"}

    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = [outbox_row]

    channel = MagicMock()
    channel.basic_publish.side_effect = RuntimeError("broker unavailable")

    conn = MagicMock()
    conn.channel.return_value = channel

    with (
        patch.object(messaging, "connection", return_value=conn),
        patch.object(messaging, "declare_queues"),
        patch.object(messaging, "SessionLocal") as session_local,
        patch.object(messaging.time, "sleep", side_effect=KeyboardInterrupt),
    ):
        session_local.return_value.__enter__.return_value = db

        with pytest.raises(KeyboardInterrupt):
            messaging.publish_outbox_forever()

    assert outbox_row.published_at is None
    db.commit.assert_not_called()
    conn.close.assert_called_once()


def test_outbox_publisher_handles_empty_outbox():
    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = []

    channel = MagicMock()
    conn = MagicMock()
    conn.channel.return_value = channel

    with (
        patch.object(messaging, "connection", return_value=conn),
        patch.object(messaging, "declare_queues"),
        patch.object(messaging, "SessionLocal") as session_local,
        patch.object(messaging.time, "sleep", side_effect=KeyboardInterrupt),
    ):
        session_local.return_value.__enter__.return_value = db

        with pytest.raises(KeyboardInterrupt):
            messaging.publish_outbox_forever()

    channel.basic_publish.assert_not_called()
    db.commit.assert_called_once()
    conn.close.assert_called_once()


def test_outbox_publisher_uses_exponential_backoff():
    with (
        patch.object(
            messaging,
            "connection",
            side_effect=RuntimeError("broker unavailable"),
        ),
        patch.object(
            messaging.time,
            "sleep",
            side_effect=[None, KeyboardInterrupt],
        ) as mock_sleep,
    ):
        with pytest.raises(KeyboardInterrupt):
            messaging.publish_outbox_forever()

    retry_delays = [sleep_call.args[0] for sleep_call in mock_sleep.call_args_list]

    assert retry_delays == [1.0, 2.0]
