FROM python:3.12-alpine

RUN apk add --no-cache restic

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY app.py migration.py migration_data.py operations.py configuration.py recovery.py snapshot_configuration.py restic_process.py journal.py ./
COPY templates/ templates/

EXPOSE 8080

CMD ["/app/.venv/bin/hypercorn", "-b", "0.0.0.0:8080", "app:app"]
