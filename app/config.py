import os

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg://control:control@localhost:5432/control_plane"
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
TASK_QUEUE = "provisioning.tasks"
PROGRESS_QUEUE = "provisioning.progress"
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1"))
TASK_DLQ = "provisioning.tasks.dlq"
TASK_EXCHANGE = ""
PROGRESS_RETRY_QUEUE = "provisioning.progress.retry"
MAX_PROGRESS_RETRIES = 3
BASE_RETRY_DELAY_MS = 1000
PROGRESS_DLQ = "provisioning.progress.dlq"
