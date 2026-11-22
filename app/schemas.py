import re
import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)


SLUG_PATTERN = r"[a-z][-a-z0-9]{1,26}[a-z0-9]"


class TenantCreate(BaseModel):
    slug: str
    name: str = Field(min_length=1, max_length=255)

    @field_validator("slug", "name")
    @classmethod
    def strip_whitespace(cls, value: str) -> str:
        return value.strip()

    @field_validator("slug")
    @classmethod
    def valid_slug(cls, value: str) -> str:
        if not re.fullmatch(SLUG_PATTERN, value):
            raise ValueError("slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
        return value

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not value:
            raise ValueError("name cannot be empty or whitespace")
        return value


class TenantPatch(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    version: int = Field(ge=1, strict=True)

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name cannot be empty or whitespace")
        return value


class TenantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    slug: str
    name: str
    status: str
    version: int
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    tenant_id: uuid.UUID
    status: str
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ErrorBody(BaseModel):
    error: dict


class ProgressUpdate(BaseModel):
    update_id: uuid.UUID
    task_id: uuid.UUID
    status: Literal["in_progress", "done", "failed"]
    error: str | None = None
