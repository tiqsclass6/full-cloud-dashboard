# MCP tool server. Build from this directory with the runbooks as a second build context:
#   docker build --build-context runbooks=../../docs/runbooks -t lakehouse/mcp-platform-tools:dev .
FROM python:3.14.7-slim-trixie AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /opt/mcp-platform-tools
# Debian security updates on top of the pinned base image (OpenSSL, PCRE2, ... between base rebuilds).
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
# Every package, transitive ones included, is pinned and hash-checked.
RUN pip install --require-hashes --no-deps -r requirements.txt
COPY platform_tools/ platform_tools/
COPY --from=runbooks . /opt/runbooks/
RUN python -m compileall -q platform_tools

# Unit tests run against the same interpreter and locked packages: ./run-tests.sh
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
ENTRYPOINT ["python", "-m", "platform_tools.server"]
