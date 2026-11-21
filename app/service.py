import uuid
from datetime import datetime, UTC

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.errors import DomainError
from app.models import (
    Outbox,
    ProcessedUpdate,
    Task,
    TaskStatus,
    TaskType,
    Tenant,
    TenantStatus,
)


def _task(db: Session, tenant: Tenant, task_type: TaskType) -> Task:
    """Create a task and its corresponding transactional outbox event."""
    task = Task(
        type=task_type,
        tenant_id=tenant.id,
        status=TaskStatus.accepted,
    )
    db.add(task)
    db.flush()

    db.add(
        Outbox(
            task_id=task.id,
            payload={
                "id": str(task.id),
                "type": task.type.value,
                "tenant_id": str(task.tenant_id),
                "status": task.status.value,
            },
        )
    )

    return task


def create_tenant(db: Session, slug: str, name: str):
    """Create a tenant and its initial deploy task atomically."""
    tenant = Tenant(
        slug=slug,
        name=name,
        status=TenantStatus.provisioning,
        version=1,
    )
    db.add(tenant)

    try:
        db.flush()

        task = _task(db, tenant, TaskType.deploy)

        db.commit()

    except IntegrityError as exc:
        db.rollback()
        raise DomainError(
            "tenant_already_exists",
            "Tenant slug already exists",
            409,
        ) from exc

    db.refresh(tenant)

    return tenant, task


def update_tenant(
    db: Session,
    tenant_id: uuid.UUID,
    name: str,
    version: int,
):
    """Update an active tenant using optimistic locking."""
    tenant = db.execute(
        select(Tenant).where(Tenant.id == tenant_id).with_for_update()
    ).scalar_one_or_none()

    if tenant is None:
        raise DomainError(
            "tenant_not_found",
            "Tenant not found",
            404,
        )

    if tenant.version != version:
        raise DomainError(
            "tenant_version_conflict",
            "Tenant version is stale",
            409,
        )

    if tenant.status != TenantStatus.active:
        raise DomainError(
            "tenant_update_not_allowed",
            f"Cannot update tenant in {tenant.status.value} state",
            409,
        )

    tenant.status = TenantStatus.updating
    tenant.version += 1
    tenant.updated_at = datetime.now(UTC)

    if name is not None:
        tenant.name = name

    task = _task(db, tenant, TaskType.update)

    db.commit()
    db.refresh(tenant)

    return tenant, task


def destroy_tenant(db: Session, tenant_id: uuid.UUID):
    """Start asynchronous destruction of an active or failed tenant."""
    tenant = db.execute(
        select(Tenant).where(Tenant.id == tenant_id).with_for_update()
    ).scalar_one_or_none()

    if tenant is None:
        raise DomainError(
            "tenant_not_found",
            "Tenant not found",
            404,
        )

    if tenant.status not in (
        TenantStatus.active,
        TenantStatus.failed,
    ):
        raise DomainError(
            "tenant_update_not_allowed",
            f"Cannot delete tenant in {tenant.status.value} state",
            409,
        )

    tenant.status = TenantStatus.destroying
    tenant.version += 1
    tenant.updated_at = datetime.now(UTC)

    task = _task(db, tenant, TaskType.destroy)

    db.commit()
    db.refresh(tenant)

    return tenant, task


def apply_progress(
    db: Session,
    update_id: uuid.UUID,
    task_id: uuid.UUID,
    status: str,
):
    """
    Apply a worker progress update safely.

    Guarantees:
    1. Duplicate update IDs are ignored.
    2. Terminal task states cannot regress.
    3. Concurrent deliveries for one task are serialized.
    4. Task, tenant and processed-update changes commit atomically.
    """

    # --------------------------------------------------
    # 1. Lock the task to serialize concurrent deliveries.
    # --------------------------------------------------

    task = db.execute(select(Task).where(Task.id == task_id).with_for_update()).scalar_one_or_none()

    if task is None:
        raise ValueError(f"Unknown task: {task_id}")

    # --------------------------------------------------
    # 2. Check idempotency BEFORE checking task state.
    # --------------------------------------------------

    inserted = db.execute(
        pg_insert(ProcessedUpdate)
        .values(
            id=update_id,
            task_id=task_id,
        )
        .on_conflict_do_nothing(index_elements=[ProcessedUpdate.id])
        .returning(ProcessedUpdate.id)
    )

    inserted_id = inserted.scalar_one_or_none()

    if inserted_id is None:
        db.rollback()
        return "duplicate"

    # --------------------------------------------------
    # 3. Validate the incoming status.
    # --------------------------------------------------

    if status not in (
        TaskStatus.in_progress.value,
        TaskStatus.done.value,
        TaskStatus.failed.value,
    ):
        raise ValueError(f"Invalid task status: {status}")

    # --------------------------------------------------
    # 4. Ignore new events for terminal tasks.
    # --------------------------------------------------

    if task.status in (
        TaskStatus.done,
        TaskStatus.failed,
    ):
        db.commit()
        return "stale"

    # --------------------------------------------------
    # 5. Apply in_progress transition.
    # --------------------------------------------------

    if status == TaskStatus.in_progress.value:
        if task.status == TaskStatus.accepted:
            task.status = TaskStatus.in_progress
            task.updated_at = datetime.now(UTC)

            db.commit()
            return "applied"

        db.commit()
        return "stale"

    # --------------------------------------------------
    # 6. Apply terminal transition.
    # --------------------------------------------------

    new_status = TaskStatus(status)

    task.status = new_status
    task.updated_at = datetime.now(UTC)

    # Lock the tenant while applying its terminal outcome.
    tenant = db.execute(
        select(Tenant).where(Tenant.id == task.tenant_id).with_for_update()
    ).scalar_one()

    if new_status == TaskStatus.failed:
        tenant.status = TenantStatus.failed

    elif task.type in (
        TaskType.deploy,
        TaskType.update,
    ):
        tenant.status = TenantStatus.active

    elif task.type == TaskType.destroy:
        tenant.status = TenantStatus.destroyed

    tenant.version += 1
    tenant.updated_at = datetime.now(UTC)

    # --------------------------------------------------
    # 7. Commit task, tenant and idempotency record together.
    # --------------------------------------------------

    db.commit()

    return "applied"
