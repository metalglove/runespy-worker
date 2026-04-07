FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_NO_CACHE=1

WORKDIR /app

# Project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install dependencies (creates and populates .venv)
RUN uv sync --no-dev

# Web UI listens on 8080 (from webui.main)
EXPOSE 8080

# Run the web UI module
CMD ["uv", "run", "python", "-m", "runespy_worker.webui"]