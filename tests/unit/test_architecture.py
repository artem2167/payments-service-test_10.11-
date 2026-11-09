import ast
import sys
from importlib.util import resolve_name
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[2] / "app"


def imports(path: Path):
    package = ".".join(path.parent.relative_to(APP.parent).parts)
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = resolve_name("." * node.level + (node.module or ""), package)
            yield from (f"{module}.{alias.name}" for alias in node.names)


@pytest.mark.parametrize(
    "layer,internal,external",
    [
        ("domain", {"domain"}, set()),
        ("application", {"application", "domain"}, set()),
        ("api", {"api", "application", "domain", "schemas"}, {"fastapi", "pydantic"}),
        (
            "messaging",
            {"messaging", "application", "domain"},
            {"aio_pika", "faststream", "pamqp", "pydantic"},
        ),
    ],
)
def test_dependencies_do_not_cross_layer_boundaries(layer, internal, external):
    violations = []
    for path in (APP / layer).rglob("*.py"):
        for module in imports(path):
            parts = module.split(".")
            if parts[0] == "app":
                allowed = len(parts) > 1 and parts[1] in internal
            else:
                allowed = parts[0] in sys.stdlib_module_names or parts[0] in external
            if not allowed:
                violations.append(f"{path.relative_to(APP)} imports {module}")
    assert not violations, "\n".join(violations)
