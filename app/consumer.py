import logging
import time

import pika
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.config import (
    BASE_RETRY_DELAY_MS,
    MAX_PROGRESS_RETRIES,
    PROGRESS_QUEUE,
    PROGRESS_RETRY_QUEUE,
    PROGRESS_DLQ,
)
from app.database import SessionLocal
from app.messaging import connection, declare_queues
from app.models import DeadLetter
from app.schemas import ProgressUpdate
from app.service import apply_progress

log = logging.getLogger(__name__)


def save_dead_letter(body, reason):
    with SessionLocal() as db:
        db.add(
            DeadLetter(
                body=body.decode("utf-8", "replace"),
                reason=str(reason)[:500],
            )
        )
        db.commit()


def process_progress_message(ch, method, properties, body):
    # Step 1: Validate the message.
    try:
        payload = ProgressUpdate.model_validate_json(body)

    except (ValidationError, ValueError) as exc:
        log.exception("Invalid/poison progress message")

        try:
            save_dead_letter(body, exc)
            ch.basic_ack(method.delivery_tag)
        except Exception:
            log.exception("Could not persist poison message")
            ch.basic_nack(method.delivery_tag, requeue=True)

        return

    # Step 2: Process the progress update.
    try:
        with SessionLocal() as db:
            apply_progress(
                db,
                payload.update_id,
                payload.task_id,
                payload.status,
            )

        ch.basic_ack(method.delivery_tag)
        return

    except SQLAlchemyError as error:
        log.exception("Transient database error")
        processing_error = error

    except Exception as exc:
        log.exception("Permanent progress processing error")

        try:
            save_dead_letter(body, exc)
            ch.basic_ack(method.delivery_tag)
        except Exception:
            log.exception("Could not persist failed message")
            ch.basic_nack(method.delivery_tag, requeue=True)

        return

    # Step 3: Read the retry count.
    headers = properties.headers or {}
    retry_count = int(headers.get("x-retry-count", 0))

    # Step 4: Exhausted retries go to the progress DLQ.
    if retry_count >= MAX_PROGRESS_RETRIES:
        try:
            ch.basic_publish(
                exchange="",
                routing_key=PROGRESS_DLQ,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers={
                        **headers,
                        "x-final-error": str(processing_error)[:500],
                    },
                ),
                mandatory=True,
            )

            ch.basic_ack(method.delivery_tag)

            log.error(
                "Progress message moved to DLQ after %s retries",
                MAX_PROGRESS_RETRIES,
            )

        except Exception:
            log.exception("Could not publish exhausted message to DLQ")
            ch.basic_nack(method.delivery_tag, requeue=True)

        return

    # Step 5: Calculate exponential backoff.
    next_retry = retry_count + 1
    delay_ms = BASE_RETRY_DELAY_MS * (2**retry_count)

    # Step 6: Publish retry before acknowledging original.
    retry_headers = dict(headers)
    retry_headers["x-retry-count"] = next_retry

    try:
        ch.basic_publish(
            exchange="",
            routing_key=PROGRESS_RETRY_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                expiration=str(delay_ms),
                headers=retry_headers,
            ),
            mandatory=True,
        )

        ch.basic_ack(method.delivery_tag)

        log.warning(
            "Scheduled progress retry %s/%s in %s ms",
            next_retry,
            MAX_PROGRESS_RETRIES,
            delay_ms,
        )

    except Exception:
        log.exception("Could not publish retry message")
        ch.basic_nack(method.delivery_tag, requeue=True)


def run_consumer():
    while True:
        try:
            conn = connection()
            channel = conn.channel()

            declare_queues(channel)
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=10)

            channel.basic_consume(
                queue=PROGRESS_QUEUE,
                on_message_callback=process_progress_message,
            )

            log.info("Progress consumer ready")
            channel.start_consuming()

        except Exception:
            log.exception("Progress consumer disconnected; reconnecting")
            time.sleep(2)
