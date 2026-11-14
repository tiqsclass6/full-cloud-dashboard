# Hermes Agent as the interactive on-call assistant (docs/ai-ops.md); Hermes supports Python 3.11-3.13.
FROM python:3.13.15-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HERMES_HOME=/data/hermes HOME=/data
WORKDIR /opt/hermes
# Debian security updates on top of the pinned base image (OpenSSL, PCRE2, ... between base rebuilds).
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
# Every package, transitive ones included, is pinned and hash-checked.
RUN pip install --require-hashes --no-deps -r requirements.txt && pip uninstall -y -q pip
USER 10001
ENTRYPOINT ["hermes"]
CMD ["--help"]
