import uuid
from concurrent.futures import ThreadPoolExecutor
import pytest
from sqlalchemy import select

from app.database import SessionLocal
from app.errors import DomainError
from app.models import Outbox, Task, TaskStatus, Tenant, TenantStatus, ProcessedUpdate, TaskType
from app.service import apply_progress, create_tenant, destroy_tenant, update_tenant
from threading import Barrier


def progress(task_id, status):
    with SessionLocal() as db:
        return apply_progress(db, uuid.uuid4(), task_id, status)


def test_deploy_lifecycle_and_outbox():
    with SessionLocal() as db:
        tenant, task = create_tenant(db, "acme-prod", "Acme")
        assert tenant.status == TenantStatus.provisioning
        assert task.status == TaskStatus.accepted
        assert db.query(Outbox).filter_by(task_id=task.id).count() == 1
    assert progress(task.id, "in_progress") == "applied"
    assert progress(task.id, "done") == "applied"
    with SessionLocal() as db:
        assert db.get(Tenant, tenant.id).status == TenantStatus.active
        assert db.get(Task, task.id).status == TaskStatus.done


def test_duplicate_progress_is_idempotent_and_stale_is_ignored():
    with SessionLocal() as db:
        tenant, task = create_tenant(db, "duplicate-safe", "Duplicate")
    update_id = uuid.uuid4()
    with SessionLocal() as db:
        assert apply_progress(db, update_id, task.id, "done") == "applied"
    with SessionLocal() as db:
        saved = db.get(ProcessedUpdate, update_id)
        assert saved is not None
    with SessionLocal() as db:
        assert apply_progress(db, update_id, task.id, "in_progress") == "duplicate"
    with SessionLocal() as db:
        assert apply_progress(db, uuid.uuid4(), task.id, "in_progress") == "stale"
        assert db.get(Task, task.id).status == TaskStatus.done
        assert db.get(Tenant, tenant.id).status == TenantStatus.active


def test_update_requires_active_and_version():
    with SessionLocal() as db:
        tenant, _ = create_tenant(db, "update-check", "Before")

        with pytest.raises(DomainError) as exc_info:
            update_tenant(db, tenant.id, "After", tenant.version)

        assert exc_info.value.code == "tenant_update_not_allowed"


def progress_for_tenant(tenant_id):
    with SessionLocal() as db:
        task = db.execute(select(Task).where(Task.tenant_id == tenant_id)).scalar_one()
        apply_progress(db, uuid.uuid4(), task.id, "done")


def test_delete_from_active_and_destroyed_terminal_state():
    with SessionLocal() as db:
        tenant, deploy = create_tenant(db, "delete-check", "Delete")

    progress_for_tenant(tenant.id)

    with SessionLocal() as db:
        deleting, destroy = destroy_tenant(db, tenant.id)
        assert deleting.status == TenantStatus.destroying


def test_same_version_concurrent_patches_only_one_succeeds():
    with SessionLocal() as db:
        tenant, task = create_tenant(db, "race-check", "Race")
    progress(task.id, "done")
    with SessionLocal() as db:
        version = db.get(Tenant, tenant.id).version

    def patch(name):
        with SessionLocal() as db:
            try:
                update_tenant(db, tenant.id, name, version)
                return "success"
            except DomainError as exc:
                return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(patch, ["First", "Second"]))
    assert results.count("success") == 1
    assert results.count("tenant_version_conflict") == 1


def test_failed_deploy_can_be_destroyed():
    with SessionLocal() as db:
        tenant, deploy = create_tenant(db, "failed-deploy", "Failed deploy")
    progress(deploy.id, "failed")
    with SessionLocal() as db:
        failed = db.get(Tenant, tenant.id)
        assert failed.status == TenantStatus.failed
        deleting, destroy = destroy_tenant(db, tenant.id)
        assert deleting.status == TenantStatus.destroying
    progress(destroy.id, "done")
    with SessionLocal() as db:
        assert db.get(Tenant, tenant.id).status == TenantStatus.destroyed


def test_destroyed_tenant_slug_cannot_be_reused():
    with SessionLocal() as db:
        tenant, task = create_tenant(
            db,
            "unique-slug",
            "Original Tenant",
        )

    # Complete provisioning.
    with SessionLocal() as db:
        apply_progress(
            db,
            uuid.uuid4(),
            task.id,
            "done",
        )

    # Destroy the tenant.
    with SessionLocal() as db:
        destroyed_tenant, destroy_task = destroy_tenant(
            db,
            tenant.id,
        )

    # Complete destruction.
    with SessionLocal() as db:
        apply_progress(
            db,
            uuid.uuid4(),
            destroy_task.id,
            "done",
        )

    # Try to reuse the destroyed tenant's slug.
    with SessionLocal() as db:
        with pytest.raises(DomainError) as exc:
            create_tenant(
                db,
                "unique-slug",
                "New Tenant",
            )

        assert exc.value.code == "tenant_already_exists"


def test_concurrent_tenant_updates_allow_only_one_success():
    # Create and activate tenant.
    with SessionLocal() as db:
        tenant, deploy_task = create_tenant(
            db,
            "concurrent-update",
            "Concurrent Update Test",
        )

    with SessionLocal() as db:
        apply_progress(
            db,
            uuid.uuid4(),
            deploy_task.id,
            "done",
        )

    # Get the current version after activation.
    with SessionLocal() as db:
        current_tenant = db.get(Tenant, tenant.id)
        expected_version = current_tenant.version

    barrier = Barrier(2)

    def update(name):
        with SessionLocal() as db:
            barrier.wait()

            try:
                update_tenant(
                    db,
                    tenant.id,
                    name,
                    expected_version,
                )
                return "success"

            except DomainError as exc:
                return exc.code

    # Execute two updates concurrently.
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                update,
                ["Updated by Client A", "Updated by Client B"],
            )
        )

    # Exactly one request must succeed.
    assert results.count("success") == 1
    assert results.count("tenant_version_conflict") == 1

    # Verify final tenant state.
    with SessionLocal() as db:
        final_tenant = db.get(Tenant, tenant.id)

        assert final_tenant.status == TenantStatus.updating
        assert final_tenant.version == expected_version + 1


def test_out_of_order_progress_does_not_regress_task():
    with SessionLocal() as db:
        tenant, task = create_tenant(
            db,
            "out-of-order",
            "Out Of Order Test",
        )

    # First event: task starts processing.
    with SessionLocal() as db:
        assert (
            apply_progress(
                db,
                uuid.uuid4(),
                task.id,
                "in_progress",
            )
            == "applied"
        )

    # Second event: task completes.
    with SessionLocal() as db:
        assert (
            apply_progress(
                db,
                uuid.uuid4(),
                task.id,
                "done",
            )
            == "applied"
        )

    # Third event: delayed older progress event.
    with SessionLocal() as db:
        assert (
            apply_progress(
                db,
                uuid.uuid4(),
                task.id,
                "in_progress",
            )
            == "stale"
        )

    # Verify terminal state was preserved.
    with SessionLocal() as db:
        saved_task = db.get(Task, task.id)
        saved_tenant = db.get(Tenant, tenant.id)

        assert saved_task.status == TaskStatus.done
        assert saved_tenant.status == TenantStatus.active


def test_worker_failure_marks_task_and_tenant_failed():
    with SessionLocal() as db:
        tenant, task = create_tenant(
            db,
            "worker-failure",
            "Worker Failure Test",
        )

    initial_version = tenant.version

    with SessionLocal() as db:
        result = apply_progress(
            db,
            uuid.uuid4(),
            task.id,
            "failed",
        )
        assert result == "applied"

    with SessionLocal() as db:
        saved_task = db.get(Task, task.id)
        saved_tenant = db.get(Tenant, tenant.id)

        assert saved_task.status == TaskStatus.failed
        assert saved_tenant.status == TenantStatus.failed
        assert saved_tenant.version == initial_version + 1


def test_failed_tenant_can_be_destroyed():
    with SessionLocal() as db:
        tenant, deploy_task = create_tenant(
            db,
            "failed-tenant-delete",
            "Failed Tenant",
        )

    with SessionLocal() as db:
        apply_progress(
            db,
            uuid.uuid4(),
            deploy_task.id,
            "failed",
        )

    with SessionLocal() as db:
        updated_tenant, destroy_task = destroy_tenant(
            db,
            tenant.id,
        )

        assert updated_tenant.status == TenantStatus.destroying
        assert destroy_task.type == TaskType.destroy


def test_concurrent_progress_updates_are_serialized():
    with SessionLocal() as db:
        tenant, task = create_tenant(
            db,
            "concurrent-progress",
            "Concurrent Progress Test",
        )

    barrier = Barrier(2)

    def send_progress(status):
        with SessionLocal() as db:
            barrier.wait()

            return apply_progress(
                db,
                uuid.uuid4(),
                task.id,
                status,
            )

    # Two workers deliver conflicting terminal outcomes.
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                send_progress,
                ["done", "failed"],
            )
        )

    # Exactly one outcome must be applied.
    assert results.count("applied") == 1
    assert results.count("stale") == 1

    # Verify task and tenant consistency.
    with SessionLocal() as db:
        saved_task = db.get(Task, task.id)
        saved_tenant = db.get(Tenant, tenant.id)

        assert saved_task.status in (
            TaskStatus.done,
            TaskStatus.failed,
        )

        if saved_task.status == TaskStatus.done:
            assert saved_tenant.status == TenantStatus.active
        else:
            assert saved_tenant.status == TenantStatus.failed


def test_successful_update_lifecycle():
    with SessionLocal() as db:
        tenant, deploy_task = create_tenant(db, "successful-update", "Original Name")

    progress(deploy_task.id, "done")

    with SessionLocal() as db:
        active_tenant = db.get(Tenant, tenant.id)
        initial_version = active_tenant.version

        updated_tenant, update_task = update_tenant(
            db,
            tenant.id,
            "Updated Name",
            initial_version,
        )

        assert updated_tenant.status == TenantStatus.updating
        assert updated_tenant.name == "Updated Name"
        assert updated_tenant.version == initial_version + 1
        assert update_task.type == TaskType.update
        assert update_task.status == TaskStatus.accepted

    assert progress(update_task.id, "in_progress") == "applied"
    assert progress(update_task.id, "done") == "applied"

    with SessionLocal() as db:
        final_tenant = db.get(Tenant, tenant.id)
        final_task = db.get(Task, update_task.id)

        assert final_tenant.status == TenantStatus.active
        assert final_tenant.version == initial_version + 2
        assert final_task.status == TaskStatus.done


def test_stale_version_does_not_update_tenant():
    with SessionLocal() as db:
        tenant, deploy_task = create_tenant(db, "stale-version", "Original")

    progress(deploy_task.id, "done")

    with SessionLocal() as db:
        active_tenant = db.get(Tenant, tenant.id)
        stale_version = active_tenant.version - 1

        with pytest.raises(DomainError) as exc:
            update_tenant(
                db,
                tenant.id,
                "New Name",
                stale_version,
            )

        assert exc.value.code == "tenant_version_conflict"

    with SessionLocal() as db:
        saved = db.get(Tenant, tenant.id)

        assert saved.name == "Original"
        assert saved.status == TenantStatus.active


def test_unknown_task_progress_is_rejected():
    unknown_task_id = uuid.uuid4()
    update_id = uuid.uuid4()

    with SessionLocal() as db:
        with pytest.raises(ValueError, match="Unknown task"):
            apply_progress(
                db,
                update_id,
                unknown_task_id,
                "done",
            )

    with SessionLocal() as db:
        assert db.get(ProcessedUpdate, update_id) is None


def test_invalid_progress_status_is_rejected():
    with SessionLocal() as db:
        tenant, task = create_tenant(db, "invalid-progress", "Invalid Progress")

    update_id = uuid.uuid4()

    with SessionLocal() as db:
        with pytest.raises(ValueError, match="Invalid task status"):
            apply_progress(
                db,
                update_id,
                task.id,
                "unknown_status",
            )

    with SessionLocal() as db:
        saved_task = db.get(Task, task.id)

        assert saved_task.status == TaskStatus.accepted
        assert db.get(ProcessedUpdate, update_id) is None


def test_progress_is_ignored_when_tenant_state_mismatches():
    with SessionLocal() as db:
        tenant, task = create_tenant(db, "state-mismatch", "State Mismatch")

        tenant.status = TenantStatus.active
        db.commit()

    with SessionLocal() as db:
        result = apply_progress(
            db,
            uuid.uuid4(),
            task.id,
            "done",
        )

        assert result == "stale"

    with SessionLocal() as db:
        saved_task = db.get(Task, task.id)
        saved_tenant = db.get(Tenant, tenant.id)

        assert saved_task.status == TaskStatus.accepted
        assert saved_tenant.status == TenantStatus.active


def test_failed_update_marks_tenant_failed():
    with SessionLocal() as db:
        tenant, deploy_task = create_tenant(db, "failed-update", "Before Update")

    progress(deploy_task.id, "done")

    with SessionLocal() as db:
        active_tenant = db.get(Tenant, tenant.id)

        updated_tenant, update_task = update_tenant(
            db,
            tenant.id,
            "After Update",
            active_tenant.version,
        )

    assert progress(update_task.id, "failed") == "applied"

    with SessionLocal() as db:
        saved_tenant = db.get(Tenant, tenant.id)
        saved_task = db.get(Task, update_task.id)

        assert saved_tenant.status == TenantStatus.failed
        assert saved_task.status == TaskStatus.failed
