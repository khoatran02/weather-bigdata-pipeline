import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_committed_dashboards_match_the_generator():
    path = ROOT / "scripts/build_dashboards.py"
    spec = importlib.util.spec_from_file_location("build_dashboards", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for build in (module.realtime, module.history, module.operations):
        board = build()
        committed = ROOT / f"docker/grafana/dashboards/{board['uid']}.json"
        assert board == json.loads(committed.read_text(encoding="utf-8"))
        assert len({p["id"] for p in board["panels"]}) == len(board["panels"])
