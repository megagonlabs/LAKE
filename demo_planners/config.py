"""Shared runtime defaults for the L.A.K.E. demo package.

These values are safe local-development defaults only. Real deployments should
override them from the Streamlit UI, CLI flags, or the surrounding Blue
environment configuration.
"""

DEFAULT_SERVICE_URL = "ws://localhost:8001"
DEFAULT_PLATFORM_NAME = "jflavien"
DEFAULT_DATA_REGISTRY_NAME = "default"
DEFAULT_SOURCE_NAME = "postgres_example"
DEFAULT_NL2SQL_PROTOCOL = "postgres"
DEFAULT_NL2SQL_DATABASE = "postgres"
DEFAULT_NL2SQL_COLLECTION = "public"
