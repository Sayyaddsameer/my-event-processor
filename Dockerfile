# ---------------------------------------------------------------------------
# Stage 1 -- install Python dependencies into an isolated layer
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 2 -- lean runtime image
# ---------------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Bring only the installed packages from the builder
COPY --from=builder /usr/local/lib/python3.11/site-packages \
                    /usr/local/lib/python3.11/site-packages

COPY src/ src/

ENV PYTHONPATH=/app
EXPOSE 8000

CMD ["python", "-m", "src.app"]
