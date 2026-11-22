import argparse
import json
import logging
import random
import time
import uuid
import pika

from app.config import RABBITMQ_URL, TASK_QUEUE, TASK_DLQ


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def validate_task_payload(body: bytes) -> dict:
    """Parse and validate an incoming task message."""
    task = json.loads(body)

    if not isinstance(task, dict):
        raise ValueError("Task payload must be a JSON object")

    if not task.get("id") or not task.get("type"):
        raise ValueError("Task must contain id and type")

    return task


def simulate_task_result(fail_rate: float) -> tuple[str, str | None]:
    """Return a simulated terminal task status."""
    if random.random() < fail_rate:
        return "failed", "simulated provisioning failure"

    return "done", None


def process_task_message(
    ch,
    method,
    body: bytes,
    fail_rate: float,
    min_delay_ms: int,
    max_delay_ms: int,
):
    """Process one task message and publish its progress updates."""

    try:
        task = validate_task_payload(body)

    except (json.JSONDecodeError, ValueError, TypeError):
        logger.exception("Invalid/poison task message; sending to DLQ")
        ch.basic_nack(method.delivery_tag, requeue=False)
        return

    try:
        logger.info("Processing task %s (%s)", task["id"], task["type"])

        def publish(status, error=None):
            event = {
                "update_id": str(uuid.uuid4()),
                "task_id": task["id"],
                "status": status,
                "error": error,
            }

            ch.basic_publish(
                exchange="",
                routing_key="provisioning.progress",
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
                mandatory=True,
            )

        publish("in_progress")

        time.sleep(random.randint(min_delay_ms, max_delay_ms) / 1000)

        status, error = simulate_task_result(fail_rate)
        publish(status, error)

        ch.basic_ack(method.delivery_tag)
        logger.info("Completed task %s", task["id"])

    except Exception:
        logger.exception("Task processing failed")
        raise


def main():
    parser = argparse.ArgumentParser(description="Simulate asynchronous tenant provisioning")
    parser.add_argument("--fail-rate", type=float, default=0.0)
    parser.add_argument("--min-delay-ms", type=int, default=300)
    parser.add_argument("--max-delay-ms", type=int, default=1200)

    args = parser.parse_args()

    if (
        not 0 <= args.fail_rate <= 1
        or args.min_delay_ms < 0
        or args.max_delay_ms < args.min_delay_ms
    ):
        parser.error("invalid failure rate or delay range")

    retry_delay = 1.0
    max_retry_delay = 30.0

    while True:
        conn = None

        try:
            logger.info("Connecting to RabbitMQ...")

            conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

            channel = conn.channel()

            channel.queue_declare(
                queue=TASK_DLQ,
                durable=True,
            )

            channel.queue_declare(
                queue=TASK_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": TASK_DLQ,
                },
            )
            channel.queue_declare(
                queue="provisioning.progress",
                durable=True,
            )

            channel.basic_qos(prefetch_count=1)
            channel.confirm_delivery()

            def publish(channel, task, status, error=None):
                event = {
                    "update_id": str(uuid.uuid4()),
                    "task_id": task["id"],
                    "status": status,
                    "error": error,
                }

                channel.basic_publish(
                    exchange="",
                    routing_key="provisioning.progress",
                    body=json.dumps(event),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                    ),
                    mandatory=True,
                )

            def callback(ch, method, properties, body):
                process_task_message(
                    ch,
                    method,
                    body,
                    args.fail_rate,
                    args.min_delay_ms,
                    args.max_delay_ms,
                )

            channel.basic_consume(
                queue="provisioning.tasks",
                on_message_callback=callback,
            )

            logger.info(
                "Worker ready (fail_rate=%s, delay=%s-%sms)",
                args.fail_rate,
                args.min_delay_ms,
                args.max_delay_ms,
            )

            # Reset backoff after successfully connecting.
            retry_delay = 1.0

            channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("Worker shutting down")
            break

        except Exception:
            logger.exception(
                "Worker connection or consumption failed. Reconnecting in %.1f seconds...",
                retry_delay,
            )

            time.sleep(retry_delay)
            retry_delay = min(
                retry_delay * 2,
                max_retry_delay,
            )

        finally:
            if conn is not None and conn.is_open:
                try:
                    conn.close()
                except Exception:
                    logger.exception("Error closing RabbitMQ connection")


if __name__ == "__main__":
    main()
