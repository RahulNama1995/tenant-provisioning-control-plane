import json
import logging
import time

import pika
from sqlalchemy import select

from app.config import RABBITMQ_URL, TASK_QUEUE, TASK_DLQ, PROGRESS_QUEUE, PROGRESS_RETRY_QUEUE
from app.database import SessionLocal
from app.models import Outbox, utcnow

log = logging.getLogger(__name__)


def connection():
    return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))


def declare_queues(channel):
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
        queue=PROGRESS_QUEUE,
        durable=True,
    )

    channel.queue_declare(
        queue=PROGRESS_RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": PROGRESS_QUEUE,
        },
    )

    channel.queue_declare(
        queue="provisioning.progress.dlq",
        durable=True,
    )


def publish_outbox_forever():
    """Poll committed outbox rows; publish confirms before marking them delivered."""
    retry_delay = 1.0

    while True:
        conn = None

        try:
            conn = connection()
            channel = conn.channel()
            declare_queues(channel)
            channel.confirm_delivery()

            with SessionLocal() as db:
                rows = (
                    db.execute(
                        select(Outbox)
                        .where(Outbox.published_at.is_(None))
                        .order_by(Outbox.created_at)
                        .limit(50)
                    )
                    .scalars()
                    .all()
                )

                for row in rows:
                    channel.basic_publish(
                        exchange="",
                        routing_key=TASK_QUEUE,
                        body=json.dumps(row.payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json",
                        ),
                        mandatory=True,
                    )
                    row.published_at = utcnow()

                db.commit()

            retry_delay = 1.0

        except Exception:
            log.exception("Outbox publishing failed; will retry")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)

        else:
            time.sleep(1)

        finally:
            if conn is not None and conn.is_open:
                conn.close()
