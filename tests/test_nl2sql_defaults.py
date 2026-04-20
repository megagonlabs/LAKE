import unittest

from demo_planners.config import (
    DEFAULT_NL2SQL_COLLECTION,
    DEFAULT_NL2SQL_DATABASE,
    DEFAULT_NL2SQL_PROTOCOL,
    DEFAULT_SOURCE_NAME,
)
from demo_planners.nl2sql_defaults import apply_default_database_nl2sql_attributes


class NL2SQLDefaultsTests(unittest.TestCase):
    def test_apply_defaults_replaces_legacy_default_source(self) -> None:
        attributes = {"question": "get jobs", "source": "default"}

        result = apply_default_database_nl2sql_attributes(attributes)

        self.assertIs(result, attributes)
        self.assertEqual(result["source"], DEFAULT_SOURCE_NAME)
        self.assertEqual(result["protocol"], DEFAULT_NL2SQL_PROTOCOL)
        self.assertEqual(result["database"], DEFAULT_NL2SQL_DATABASE)
        self.assertEqual(result["collection"], DEFAULT_NL2SQL_COLLECTION)

    def test_apply_defaults_preserves_explicit_values(self) -> None:
        attributes = {
            "question": "get jobs",
            "source": "custom_source",
            "protocol": "mysql",
            "database": "analytics",
            "collection": "warehouse",
        }

        result = apply_default_database_nl2sql_attributes(attributes)

        self.assertEqual(result["source"], "custom_source")
        self.assertEqual(result["protocol"], "mysql")
        self.assertEqual(result["database"], "analytics")
        self.assertEqual(result["collection"], "warehouse")
