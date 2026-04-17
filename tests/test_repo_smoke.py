from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class RepoSmokeTests(unittest.TestCase):
    def test_python_sources_compile(self) -> None:
        for path in REPO_ROOT.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            compile(source, str(path), "exec")

    def test_readme_has_release_sections(self) -> None:
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        expected_sections = [
            "## Installation",
            "## Run",
            "## Configuration Notes",
            "## Testing",
            "## License",
            "## Citation",
            "## Contact",
            "## Disclosure",
            "## Datasets",
            "## Megagon Components",
            "## Open Source Software (OSS) Components",
        ]
        for section in expected_sections:
            self.assertIn(section, readme)

    def test_docs_scaffold_exists(self) -> None:
        expected_docs = [
            REPO_ROOT / "docs" / "README.md",
            REPO_ROOT / "docs" / "code-structure.md",
            REPO_ROOT / "docs" / "testing.md",
            REPO_ROOT / "LICENSE.txt",
            REPO_ROOT / "NOTICE",
        ]
        for path in expected_docs:
            self.assertTrue(path.is_file(), f"Missing expected file: {path}")
