import re
import uuid
from datetime import datetime, UTC
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, field_serializer


class TenantCreate(BaseModel):
    slug: str
    name: str = Field(min_length=1, max_length=255)

    @field_validator("slug")
    @classmethod
    def valid_slug(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z][-a-z0-9]{1,26}[a-z0-9]", value):
            raise ValueError("slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
        return value


class TenantPatch(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    version: int = Field(ge=1)


class TenantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    id: uuid.UUID
    slug: str
    name: str
    status: str
    version: int
    created_at: datetime
    updated_at: datetime


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    id: uuid.UUID
    type: str
    tenant_id: uuid.UUID
    status: str
    created_at: datetime
    updated_at: datetime


class ErrorBody(BaseModel):
    error: dict


class ProgressUpdate(BaseModel):
    update_id: uuid.UUID
    task_id: uuid.UUID
    status: Literal["in_progress", "done", "failed"]
    error: str | None = None
