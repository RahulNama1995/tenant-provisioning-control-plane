import json
from unittest.mock import patch, MagicMock
import pytest
from worker import simulator
from worker.simulator import validate_task_payload, simulate_task_result


def test_validate_valid_task_payload():
    body = json.dumps(
        {
            "id": "task-123",
            "type": "deploy",
        }
    ).encode()

    result = validate_task_payload(body)

    assert result == {
        "id": "task-123",
        "type": "deploy",
    }


@pytest.mark.parametrize("task_type", ["deploy", "update", "destroy"])
def test_validate_supported_task_types(task_type):
    body = json.dumps(
        {
            "id": "task-123",
            "type": task_type,
        }
    ).encode()

    result = validate_task_payload(body)

    assert result["type"] == task_type


def test_validate_rejects_unsupported_task_type():
    body = json.dumps(
        {
            "id": "task-123",
            "type": "restart",
        }
    ).encode()

    with pytest.raises(ValueError, match="Task type must be"):
        validate_task_payload(body)


@pytest.mark.parametrize(
    "body",
    [
        b"{invalid-json",
        b"[]",
        b"null",
        b'{"id": "task-123"}',
        b'{"type": "deploy"}',
        b'{"id": "", "type": "deploy"}',
        b'{"id": "task-123", "type": ""}',
    ],
)
def test_validate_invalid_task_payload(body):
    with pytest.raises((ValueError, json.JSONDecodeError)):
        validate_task_payload(body)


def test_simulate_task_result_success():
    with patch("worker.simulator.random.random", return_value=0.9):
        status, error = simulate_task_result(fail_rate=0.2)

    assert status == "done"
    assert error is None


def test_simulate_task_result_failure():
    with patch("worker.simulator.random.random", return_value=0.1):
        status, error = simulate_task_result(fail_rate=0.2)

    assert status == "failed"
    assert error == "simulated provisioning failure"


def test_simulate_task_result_zero_fail_rate():
    with patch("worker.simulator.random.random", return_value=0.0):
        status, error = simulate_task_result(fail_rate=0.0)

    assert status == "done"
    assert error is None


def test_simulate_task_result_full_fail_rate():
    with patch("worker.simulator.random.random", return_value=0.99):
        status, error = simulate_task_result(fail_rate=1.0)

    assert status == "failed"
    assert error == "simulated provisioning failure"


def test_process_task_message_success():
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 123

    body = json.dumps(
        {
            "id": "task-123",
            "type": "deploy",
        }
    ).encode()

    with (
        patch.object(simulator.time, "sleep"),
        patch.object(
            simulator,
            "simulate_task_result",
            return_value=("done", None),
        ),
    ):
        simulator.process_task_message(channel, method, body, 0.0, 100, 200)

    assert channel.basic_publish.call_count == 2
    channel.basic_ack.assert_called_once_with(123)
    channel.basic_nack.assert_not_called()

    first_event = json.loads(channel.basic_publish.call_args_list[0].kwargs["body"])
    second_event = json.loads(channel.basic_publish.call_args_list[1].kwargs["body"])

    assert first_event["status"] == "in_progress"
    assert second_event["status"] == "done"
    assert first_event["task_id"] == "task-123"


def test_process_task_message_failed_provisioning():
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 456

    body = json.dumps(
        {
            "id": "task-456",
            "type": "deploy",
        }
    ).encode()

    with (
        patch.object(simulator.time, "sleep"),
        patch.object(
            simulator,
            "simulate_task_result",
            return_value=(
                "failed",
                "simulated provisioning failure",
            ),
        ),
    ):
        simulator.process_task_message(channel, method, body, 1.0, 100, 200)

    assert channel.basic_publish.call_count == 2
    channel.basic_ack.assert_called_once_with(456)

    final_event = json.loads(channel.basic_publish.call_args_list[1].kwargs["body"])

    assert final_event["status"] == "failed"
    assert final_event["error"] == "simulated provisioning failure"


def test_process_task_message_sends_invalid_json_to_dlq():
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 789

    simulator.process_task_message(
        channel,
        method,
        b"{invalid-json",
        0.0,
        100,
        200,
    )

    channel.basic_nack.assert_called_once_with(
        789,
        requeue=False,
    )
    channel.basic_ack.assert_not_called()
    channel.basic_publish.assert_not_called()


def test_process_task_message_does_not_ack_when_publish_fails():
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 999

    channel.basic_publish.side_effect = RuntimeError("RabbitMQ unavailable")

    body = json.dumps(
        {
            "id": "task-999",
            "type": "deploy",
        }
    ).encode()

    with pytest.raises(RuntimeError, match="RabbitMQ unavailable"):
        simulator.process_task_message(
            channel,
            method,
            body,
            0.0,
            100,
            200,
        )

    channel.basic_ack.assert_not_called()
    channel.basic_nack.assert_not_called()
    channel.basic_publish.assert_called_once()


def test_worker_reconnects_after_connection_failure():
    with (
        patch(
            "sys.argv",
            ["worker", "--min-delay-ms", "0", "--max-delay-ms", "0"],
        ),
        patch.object(
            simulator.pika,
            "BlockingConnection",
            side_effect=[
                RuntimeError("broker unavailable"),
                KeyboardInterrupt,
            ],
        ),
        patch.object(
            simulator.time,
            "sleep",
        ) as mock_sleep,
    ):
        simulator.main()

    mock_sleep.assert_called_once_with(1.0)


def test_worker_configures_queues_and_starts_consuming():
    channel = MagicMock()
    connection = MagicMock()
    connection.channel.return_value = channel

    # Simulate Ctrl+C after the worker starts consuming.
    channel.start_consuming.side_effect = KeyboardInterrupt

    with (
        patch(
            "sys.argv",
            ["worker", "--min-delay-ms", "0", "--max-delay-ms", "0"],
        ),
        patch.object(
            simulator.pika,
            "BlockingConnection",
            return_value=connection,
        ),
    ):
        simulator.main()

    assert channel.queue_declare.call_count == 3

    declared_queues = [call.kwargs["queue"] for call in channel.queue_declare.call_args_list]

    assert "provisioning.tasks" in declared_queues
    assert "provisioning.tasks.dlq" in declared_queues
    assert "provisioning.progress" in declared_queues

    channel.basic_qos.assert_called_once_with(prefetch_count=1)
    channel.confirm_delivery.assert_called_once()
    channel.basic_consume.assert_called_once()
    channel.start_consuming.assert_called_once()
    connection.close.assert_called_once()

@pytest.mark.parametrize("task_type", [[], {}, 123, None])
def test_validate_rejects_non_string_task_type(task_type):
    body = json.dumps(
        {
            "id": "task-123",
            "type": task_type,
        }
    ).encode()

    with pytest.raises(ValueError, match="Task type must be"):
        validate_task_payload(body)