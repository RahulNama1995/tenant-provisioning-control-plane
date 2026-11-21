from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError


class DomainError(Exception):
    def __init__(self, code: str, message: str, status_code: int):
        self.code, self.message, self.status_code = code, message, status_code


async def domain_error_handler(_: Request, exc: DomainError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


async def integrity_error_handler(_: Request, exc: IntegrityError):
    return JSONResponse(
        status_code=409,
        content={
            "error": {"code": "tenant_already_exists", "message": "Tenant slug already exists"}
        },
    )
