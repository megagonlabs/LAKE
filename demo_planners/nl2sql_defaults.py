from typing import Any, Dict

from demo_planners.config import (
    DEFAULT_NL2SQL_COLLECTION,
    DEFAULT_NL2SQL_DATABASE,
    DEFAULT_NL2SQL_PROTOCOL,
    DEFAULT_SOURCE_NAME,
)


def apply_default_database_nl2sql_attributes(attributes: Any) -> Dict[str, Any]:
    """Mutate and return NL2SQL database attributes with repo defaults applied."""

    if attributes is None:
        attributes = {}
    elif not isinstance(attributes, dict):
        raise TypeError(f"NL2SQL attributes must be a dict, got {type(attributes).__name__}")

    source = str(attributes.get("source") or "").strip()
    if not source or source == "default":
        attributes["source"] = DEFAULT_SOURCE_NAME

    if not str(attributes.get("protocol") or "").strip():
        attributes["protocol"] = DEFAULT_NL2SQL_PROTOCOL
    if not str(attributes.get("database") or "").strip():
        attributes["database"] = DEFAULT_NL2SQL_DATABASE
    if not str(attributes.get("collection") or "").strip():
        attributes["collection"] = DEFAULT_NL2SQL_COLLECTION

    return attributes
