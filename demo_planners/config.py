"""Shared runtime defaults for the L.A.K.E. demo package.

These values are safe local-development defaults only. Real deployments should
override them from the Streamlit UI, CLI flags, or the surrounding Blue
environment configuration.
"""

DEFAULT_SERVICE_URL = "ws://localhost:8001"
DEFAULT_PLATFORM_NAME = "default"
DEFAULT_DATA_REGISTRY_NAME = "default"
DEFAULT_SOURCE_NAME = "default"
