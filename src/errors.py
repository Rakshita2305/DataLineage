class DataLineageError(Exception):
    """Base exception for DataLineage."""


class RepositoryNotInitializedError(DataLineageError):
    """Raised when .mydata structure is missing."""


class InvalidVersionError(DataLineageError):
    """Raised when a version identifier is invalid or not found."""


class ValidationError(DataLineageError):
    """Raised for input and schema validation failures."""


class DuplicateVersionError(DataLineageError):
    """Raised when a deterministic version already exists."""
