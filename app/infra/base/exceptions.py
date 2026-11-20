from asyncpg.exceptions import UniqueViolationError
from sqlalchemy.exc import IntegrityError


class DBError(Exception):
    pass


class DBNotFound(DBError):
    pass


def is_unique_violation(error: IntegrityError, constraint: str) -> bool:
    cause = error.orig.__cause__ if error.orig else None
    return isinstance(cause, UniqueViolationError) and cause.constraint_name == constraint
