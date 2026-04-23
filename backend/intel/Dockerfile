FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Install Node.js
RUN apt-get update && apt-get install -y \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Python package manager
RUN pip install uv

# Copy project files
COPY intel/pyproject.toml intel/uv.lock ./
RUN uv sync --frozen --no-install-project

COPY intel/ /app/
COPY common/ /app/common/

ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]