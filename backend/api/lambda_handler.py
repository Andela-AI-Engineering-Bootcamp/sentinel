"""Lambda adapter for Sentinel API."""

from mangum import Mangum

from api.main import app


handler = Mangum(app)
