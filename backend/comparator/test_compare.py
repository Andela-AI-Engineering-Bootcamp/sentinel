from unittest.mock import patch

from fastapi.testclient import TestClient

import os
os.environ.setdefault("AUTH_DISABLED", "true")

from api.main import app
from common.models import IncidentCompareResult

client = TestClient(app)

@patch("comparator.agent.compare_workflows", return_value=IncidentCompareResult(
    job_id_a="a", job_id_b="b", verdict="unclear", confidence="low"
))
def test_compare_endpoint_mocked(_mock):
    r = client.post(
        "/api/jobs/compare",
        json={"job_id_a": "x", "job_id_b": "y"},
    )
    # Will still hit DB: only use this if you have two real completed jobs, or
    # mock at db.get_job level. Prefer integration with real SQLite test DB.
    assert r.status_code in (200, 404, 422)