import threading
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing import Annotated

from app.consumer import run_consumer
from app.database import Base, engine, get_db
from app.errors import DomainError, domain_error_handler
from app.messaging import publish_outbox_forever
from app.models import Task, Tenant
from app.schemas import TaskOut, TenantCreate, TenantOut, TenantPatch
from app.service import create_tenant, destroy_tenant, update_tenant

DbSession = Annotated[Session, Depends(get_db)]


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    threading.Thread(target=publish_outbox_forever, daemon=True, name="outbox-publisher").start()
    threading.Thread(target=run_consumer, daemon=True, name="progress-consumer").start()
    yield


app = FastAPI(title="Tenant Provisioning Control Plane", version="1.0.0", lifespan=lifespan)
app.add_exception_handler(DomainError, domain_error_handler)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/tenants", response_model=TenantOut, status_code=201)
def create(payload: TenantCreate, db: DbSession):
    tenant, _ = create_tenant(db, payload.slug, payload.name)
    return tenant


@app.get("/tenants", response_model=list[TenantOut])
def list_tenants(
    db: DbSession,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return (
        db.execute(select(Tenant).order_by(Tenant.created_at.desc()).limit(limit).offset(offset))
        .scalars()
        .all()
    )


@app.get("/tenants/{tenant_id}", response_model=TenantOut)
def get_tenant(tenant_id: uuid.UUID, db: DbSession):
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise DomainError("tenant_not_found", "Tenant not found", 404)
    return tenant


@app.patch("/tenants/{tenant_id}", response_model=TenantOut)
def patch_tenant(tenant_id: uuid.UUID, payload: TenantPatch, db: DbSession):
    tenant, _ = update_tenant(db, tenant_id, payload.name, payload.version)
    return tenant


@app.delete("/tenants/{tenant_id}", response_model=TenantOut)
def delete_tenant(tenant_id: uuid.UUID, db: DbSession):
    tenant, _ = destroy_tenant(db, tenant_id)
    return tenant


@app.get("/tasks", response_model=list[TaskOut])
def list_tasks(
    db: DbSession,
    tenant_id: uuid.UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    stmt = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
    if tenant_id:
        stmt = stmt.where(Task.tenant_id == tenant_id)
    return db.execute(stmt).scalars().all()


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: uuid.UUID, db: DbSession):
    task = db.get(Task, task_id)
    if task is None:
        raise DomainError("task_not_found", "Task not found", 404)
    return task
