# ops-agent, ops-control and the AgentCore Runtime agent: one image, three modes.
FROM python:3.14.7-slim-trixie AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /opt/ai-ops
# Debian security updates on top of the pinned base image (OpenSSL, PCRE2, ... between base rebuilds).
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
# Every package, transitive ones included, is pinned and hash-checked.
RUN pip install --require-hashes --no-deps -r requirements.txt
COPY ops_agent/ ops_agent/
RUN python -m compileall -q ops_agent

FROM base AS test
RUN pip install pytest==9.1.1
COPY tests/ tests/
COPY pyproject.toml .
CMD ["python", "-m", "pytest"]

FROM base
# pip is not needed at run time; removing it removes its CVEs from the image.
RUN pip uninstall -y -q pip
USER 10001
EXPOSE 8080
# Mode from the Deployment's args, else OPS_AGENT_MODE: AgentCore Runtime starts the image without arguments.
ENTRYPOINT ["python", "-m", "ops_agent"]
