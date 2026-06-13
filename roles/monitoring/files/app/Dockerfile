# Argus — pure-stdlib Python service monitor, no pip deps.
FROM python:3.12-slim

WORKDIR /app
# openssh-client: host CPU/mem is collected via a lightweight SSH call to the
# Mongo box (Mongo's API can't report host metrics).
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/*
COPY app.py mongo.py ollama.py elastic.py kafka.py store.py hoststat.py ./
COPY static/ ./static/

# Drop privileges — the monitor needs outbound network, its listen port, and
# write access to /app/data (the SQLite file, a host-mounted volume). The uid is
# pinned so the host data dir can be chowned to match (see the monitoring role).
RUN useradd --system --no-create-home --uid 5000 monitor \
    && mkdir -p /app/data && chown monitor /app/data
USER monitor

EXPOSE 9200
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9200/healthz',timeout=4).status==200 else 1)"

CMD ["python", "app.py"]
