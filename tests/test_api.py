from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app


@pytest.fixture
def client(db_session):
    def override_get_db():
        yield db_session

    @asynccontextmanager
    async def test_lifespan(_app):
        yield

    app.dependency_overrides[get_db] = override_get_db

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = test_lifespan

    try:
        with TestClient(
            app,
            raise_server_exceptions=False,
        ) as test_client:
            yield test_client
    finally:
        app.router.lifespan_context = original_lifespan
        app.dependency_overrides.clear()


def test_create_tenant_api(client):
    response = client.post(
        "/tenants",
        json={
            "slug": "api-test-tenant",
            "name": "API Test Tenant",
        },
    )

    assert response.status_code == 201

    data = response.json()

    assert data["id"] is not None
    assert data["slug"] == "api-test-tenant"
    assert data["name"] == "API Test Tenant"
    assert data["status"] == "provisioning"
    assert data["version"] == 1
    assert data["created_at"] is not None
    assert data["updated_at"] is not None


def test_create_duplicate_tenant_returns_conflict(client):
    payload = {
        "slug": "duplicate-api-tenant",
        "name": "Duplicate API Tenant",
    }

    first_response = client.post("/tenants", json=payload)
    assert first_response.status_code == 201

    second_response = client.post("/tenants", json=payload)

    assert second_response.status_code == 409
    assert second_response.json()["error"]["code"] == "tenant_already_exists"


def test_get_tenant_api(client):
    create_response = client.post(
        "/tenants",
        json={
            "slug": "get-api-tenant",
            "name": "Get API Tenant",
        },
    )
    assert create_response.status_code == 201

    tenant_id = create_response.json()["id"]

    response = client.get(f"/tenants/{tenant_id}")

    assert response.status_code == 200
    assert response.json()["id"] == tenant_id
    assert response.json()["slug"] == "get-api-tenant"
    assert response.json()["status"] == "provisioning"


def test_get_nonexistent_tenant_returns_404(client):
    import uuid

    tenant_id = uuid.uuid4()

    response = client.get(f"/tenants/{tenant_id}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "tenant_not_found"


def test_list_tenants_api(client):
    for i in range(3):
        response = client.post(
            "/tenants",
            json={
                "slug": f"list-tenant-{i}",
                "name": f"List Tenant {i}",
            },
        )
        assert response.status_code == 201

    response = client.get("/tenants")

    assert response.status_code == 200
    assert len(response.json()) == 3
    assert all(tenant["status"] == "provisioning" for tenant in response.json())


def test_list_tenants_pagination(client):
    for i in range(5):
        client.post(
            "/tenants",
            json={
                "slug": f"pagination-tenant-{i}",
                "name": f"Pagination Tenant {i}",
            },
        )

    response = client.get("/tenants?limit=2&offset=1")

    assert response.status_code == 200
    assert len(response.json()) == 2

    slugs = {tenant["slug"] for tenant in response.json()}
    assert len(slugs) == 2


def test_patch_tenant_api(client):
    # Create tenant
    response = client.post(
        "/tenants",
        json={
            "slug": "patch-api-tenant",
            "name": "Original Name",
        },
    )
    assert response.status_code == 201

    tenant = response.json()
    tenant_id = tenant["id"]

    # Complete deployment to make tenant active
    from app.service import apply_progress
    from app.database import SessionLocal
    from app.models import Task
    import uuid

    with SessionLocal() as db:
        task = db.query(Task).filter(Task.tenant_id == tenant_id).first()
        assert task is not None

        apply_progress(db, uuid.uuid4(), task.id, "done")

    # Update tenant
    response = client.patch(
        f"/tenants/{tenant_id}",
        json={
            "name": "Updated Name",
            "version": 2,
        },
    )

    assert response.status_code == 200

    updated = response.json()
    assert updated["name"] == "Updated Name"
    assert updated["status"] == "updating"
    assert updated["version"] == 3


def test_patch_tenant_version_conflict(client):
    response = client.post(
        "/tenants",
        json={
            "slug": "version-conflict-tenant",
            "name": "Version Conflict Tenant",
        },
    )
    assert response.status_code == 201

    tenant = response.json()
    tenant_id = tenant["id"]

    # Version 1 is stale because the tenant is still provisioning.
    response = client.patch(
        f"/tenants/{tenant_id}",
        json={
            "name": "Updated Name",
            "version": 1,
        },
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "tenant_update_not_allowed"


def test_patch_active_tenant_with_stale_version(client, db_session):
    import uuid

    from app.models import Task
    from app.service import apply_progress

    # Create tenant
    response = client.post(
        "/tenants",
        json={
            "slug": "stale-version-api",
            "name": "Stale Version API",
        },
    )
    assert response.status_code == 201

    tenant = response.json()
    tenant_id = tenant["id"]

    # Find the deploy task
    task = db_session.query(Task).filter(Task.tenant_id == uuid.UUID(tenant_id)).one()

    # Complete deployment using the same test database session
    result = apply_progress(
        db_session,
        uuid.uuid4(),
        task.id,
        "done",
    )
    assert result != "stale"

    # Tenant is now active at version 2.
    # Send version 1 to simulate a stale client.
    response = client.patch(
        f"/tenants/{tenant_id}",
        json={
            "name": "Stale Update",
            "version": 1,
        },
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "tenant_version_conflict"

    # Verify no mutation occurred
    tenant_response = client.get(f"/tenants/{tenant_id}")
    assert tenant_response.json()["name"] == "Stale Version API"
    assert tenant_response.json()["status"] == "active"
    assert tenant_response.json()["version"] == 2


def test_delete_active_tenant_api(client, db_session):
    import uuid

    from app.models import Task
    from app.service import apply_progress

    # Create tenant
    response = client.post(
        "/tenants",
        json={
            "slug": "delete-api-tenant",
            "name": "Delete API Tenant",
        },
    )
    assert response.status_code == 201

    tenant_id = response.json()["id"]

    # Complete deployment
    task = db_session.query(Task).filter(Task.tenant_id == uuid.UUID(tenant_id)).one()

    apply_progress(db_session, uuid.uuid4(), task.id, "done")

    # Delete active tenant
    response = client.delete(f"/tenants/{tenant_id}")

    assert response.status_code == 200

    deleted = response.json()
    assert deleted["id"] == tenant_id
    assert deleted["status"] == "destroying"
    assert deleted["version"] == 3

    # Tenant remains retrievable during destruction
    response = client.get(f"/tenants/{tenant_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "destroying"


def test_list_tasks_api(client):
    for i in range(2):
        response = client.post(
            "/tenants",
            json={
                "slug": f"task-list-tenant-{i}",
                "name": f"Task List Tenant {i}",
            },
        )
        assert response.status_code == 201

    response = client.get("/tasks")

    assert response.status_code == 200
    assert len(response.json()) == 2

    for task in response.json():
        assert task["type"] == "deploy"
        assert task["status"] == "accepted"
        assert task["tenant_id"] is not None


def test_list_tasks_filtered_by_tenant(client):
    tenant_ids = []

    for i in range(2):
        response = client.post(
            "/tenants",
            json={
                "slug": f"filtered-task-tenant-{i}",
                "name": f"Filtered Task Tenant {i}",
            },
        )
        assert response.status_code == 201
        tenant_ids.append(response.json()["id"])

    response = client.get(
        "/tasks",
        params={"tenant_id": tenant_ids[0]},
    )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["tenant_id"] == tenant_ids[0]


def test_get_task_api(client):
    tenant_response = client.post(
        "/tenants",
        json={
            "slug": "get-task-tenant",
            "name": "Get Task Tenant",
        },
    )
    assert tenant_response.status_code == 201

    tenant_id = tenant_response.json()["id"]

    list_response = client.get(
        "/tasks",
        params={"tenant_id": tenant_id},
    )
    assert list_response.status_code == 200
    assert len(list_response.json()) == 1

    task_id = list_response.json()[0]["id"]

    response = client.get(f"/tasks/{task_id}")

    assert response.status_code == 200
    assert response.json()["id"] == task_id
    assert response.json()["tenant_id"] == tenant_id
    assert response.json()["type"] == "deploy"
    assert response.json()["status"] == "accepted"


def test_get_nonexistent_task_returns_404(client):
    import uuid

    task_id = uuid.uuid4()

    response = client.get(f"/tasks/{task_id}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "task_not_found"


def test_create_tenant_with_empty_name_returns_422(client):
    response = client.post(
        "/tenants",
        json={
            "slug": "empty-name-tenant",
            "name": "",
        },
    )

    assert response.status_code == 422


def test_create_tenant_with_invalid_slug_returns_422(client):
    response = client.post(
        "/tenants",
        json={
            "slug": "Invalid Slug!",
            "name": "Invalid Slug Tenant",
        },
    )

    assert response.status_code == 422


def test_create_tenant_without_name_returns_422(client):
    response = client.post(
        "/tenants",
        json={
            "slug": "missing-name-tenant",
        },
    )

    assert response.status_code == 422


def test_list_tenants_rejects_limit_above_maximum(client):
    response = client.get("/tenants?limit=201")
    assert response.status_code == 422


def test_list_tenants_rejects_zero_limit(client):
    response = client.get("/tenants?limit=0")
    assert response.status_code == 422


def test_list_tenants_rejects_negative_offset(client):
    response = client.get("/tenants?offset=-1")
    assert response.status_code == 422


def test_list_tasks_rejects_invalid_pagination(client):
    response = client.get("/tasks?limit=201")
    assert response.status_code == 422

    response = client.get("/tasks?offset=-1")
    assert response.status_code == 422
