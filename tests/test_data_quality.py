"""Static checks for the exported notebook; execution requires Databricks."""
import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class DataQualityNotebookTests(unittest.TestCase):
    def test_quality_assertions_are_present(self):
        source = (ROOT / "notebooks" / "04_data_quality.py").read_text()
        tree = ast.parse(source)
        messages = {
            node.msg.value for node in ast.walk(tree)
            if isinstance(node, ast.Assert) and isinstance(node.msg, ast.Constant)
        }
        self.assertEqual(messages, {
            "Duplicate order IDs found",
            "Missing order IDs found",
            "Order items without matching orders",
        })


if __name__ == "__main__":
    unittest.main()
