FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src
COPY data /app/data
COPY scripts /app/scripts

RUN uv sync --frozen \
    && uv run playwright install --with-deps firefox

EXPOSE 8001

CMD ["uv", "run", "python", "-m", "src.main"]
