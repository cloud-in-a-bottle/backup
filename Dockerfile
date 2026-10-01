FROM python:3.12-alpine

RUN apk add --no-cache ca-certificates
COPY --from=restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510 /usr/bin/restic /usr/local/bin/restic
RUN restic version

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY app.py migration.py migration_data.py operations.py configuration.py recovery.py snapshot_configuration.py restic_process.py journal.py ./
COPY templates/ templates/

EXPOSE 8080

CMD ["/app/.venv/bin/hypercorn", "-b", "0.0.0.0:8080", "app:app"]
