import uuid
from datetime import UTC, datetime

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


def _get_tenant_for_update(
    db: Session,
    tenant_id: uuid.UUID,
) -> Tenant:
    """Fetch and lock a tenant row."""
    tenant = db.execute(
        select(Tenant).where(Tenant.id == tenant_id).with_for_update()
    ).scalar_one_or_none()

    if tenant is None:
        raise DomainError(
            "tenant_not_found",
            "Tenant not found",
            404,
        )

    return tenant


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

        constraint_name = getattr(
            getattr(exc.orig, "diag", None),
            "constraint_name",
            None,
        )

        if constraint_name and "slug" in constraint_name:
            raise DomainError(
                "tenant_already_exists",
                "Tenant slug already exists",
                409,
            ) from exc

        raise

    except Exception:
        db.rollback()
        raise

    db.refresh(tenant)
    db.refresh(task)

    return tenant, task


def update_tenant(
    db: Session,
    tenant_id: uuid.UUID,
    name: str,
    version: int,
):
    """Update an active tenant using optimistic locking."""
    try:
        tenant = _get_tenant_for_update(db, tenant_id)

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
        tenant.name = name
        tenant.version += 1
        tenant.updated_at = datetime.now(UTC)

        task = _task(db, tenant, TaskType.update)

        db.commit()
        db.refresh(tenant)
        db.refresh(task)

        return tenant, task

    except Exception:
        db.rollback()
        raise


def destroy_tenant(db: Session, tenant_id: uuid.UUID):
    """Start asynchronous destruction of an active or failed tenant."""
    try:
        tenant = _get_tenant_for_update(db, tenant_id)

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
        db.refresh(task)

        return tenant, task

    except Exception:
        db.rollback()
        raise


def _expected_tenant_status(task_type: TaskType) -> TenantStatus:
    """Return the tenant state expected while a task is running."""
    return {
        TaskType.deploy: TenantStatus.provisioning,
        TaskType.update: TenantStatus.updating,
        TaskType.destroy: TenantStatus.destroying,
    }[task_type]


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
    3. Tenant rows are locked before task rows.
    4. Task, tenant and processed-update changes commit atomically.
    5. Out-of-order progress events cannot regress task state.
    """

    try:
        # Validate the incoming status.
        if status not in (
            TaskStatus.in_progress.value,
            TaskStatus.done.value,
            TaskStatus.failed.value,
        ):
            raise ValueError(f"Invalid task status: {status}")

        # Read the task's tenant ID first. This is a non-locking read.
        # The tenant_id of a task is immutable after creation.
        task_reference = db.execute(
            select(Task.tenant_id).where(Task.id == task_id)
        ).scalar_one_or_none()

        if task_reference is None:
            raise ValueError(f"Unknown task: {task_id}")

        # Use a consistent lock order: tenant first, then task.
        tenant = _get_tenant_for_update(db, task_reference)

        task = db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        ).scalar_one_or_none()

        if task is None:
            raise ValueError(f"Unknown task: {task_id}")

        # Register the event ID atomically.
        # The primary key prevents duplicate event processing.
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

        # Ignore events for tasks that have already reached a terminal state.
        if task.status in (
            TaskStatus.done,
            TaskStatus.failed,
        ):
            db.commit()
            return "stale"

        # Verify that the task and tenant are in the expected lifecycle state.
        expected_status = _expected_tenant_status(task.type)

        if tenant.status != expected_status:
            db.commit()
            return "stale"

        # An in-progress event only advances an accepted task.
        if status == TaskStatus.in_progress.value:
            if task.status == TaskStatus.accepted:
                task.status = TaskStatus.in_progress
                task.updated_at = datetime.now(UTC)

                db.commit()
                return "applied"

            db.commit()
            return "stale"

        # Apply a terminal task transition.
        new_status = TaskStatus(status)

        if task.status not in (
            TaskStatus.accepted,
            TaskStatus.in_progress,
        ):
            db.commit()
            return "stale"

        task.status = new_status
        task.updated_at = datetime.now(UTC)

        # Apply the corresponding tenant transition.
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

        # Commit task, tenant and idempotency record atomically.
        db.commit()

        return "applied"

    except Exception:
        db.rollback()
        raise
