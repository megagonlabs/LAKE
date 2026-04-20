import unittest

from streamlit_app_support import normalize_payload


class StreamlitAppSupportTests(unittest.TestCase):
    def test_normalize_payload_handles_none(self) -> None:
        payload = normalize_payload(
            None,
            question="What jobs are available?",
            method="iterative_planning",
            tools=["COUNT", "NL2SQL"],
            service_url="ws://localhost:8001",
            method_labels={"iterative_planning": "Iterative Planning"},
        )

        self.assertEqual(payload["error"], "Pipeline returned no payload.")
        self.assertEqual(payload["method_label"], "Iterative Planning")
        self.assertEqual(payload["tools"], ["COUNT", "NL2SQL"])
        self.assertEqual(payload["steps"], [])
        self.assertIsNone(payload["final_answer"])

    def test_normalize_payload_backfills_and_sanitizes_fields(self) -> None:
        payload = normalize_payload(
            {
                "question": "Saved question",
                "method": "cascade_planning",
                "tools": ("JOIN_2", 7),
                "steps": "not-a-list",
                "error": None,
            },
            question="Fallback question",
            method="iterative_planning",
            tools=["COUNT"],
            service_url="ws://blue.example/ws",
            method_labels={"cascade_planning": "Cascade Planning"},
        )

        self.assertEqual(payload["question"], "Saved question")
        self.assertEqual(payload["method"], "cascade_planning")
        self.assertEqual(payload["method_label"], "Cascade Planning")
        self.assertEqual(payload["tools"], ["JOIN_2", "7"])
        self.assertEqual(payload["service_url"], "ws://blue.example/ws")
        self.assertEqual(payload["steps"], [])
        self.assertEqual(payload["error"], "")
