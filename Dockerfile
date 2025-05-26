# syntax=docker/dockerfile:1

FROM python:3.13.3-slim-bookworm

# Set environment variables to prevent interactive prompts during apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies for pip, and core ODBC libraries
# These are the Debian/Ubuntu equivalents for the packages you need
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc-dev \
    gnupg \
    curl \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN set -eux; \
    # The correct way to get the Debian version (e.g., "12" for Bookworm)
    DEBIAN_VERSION=$(lsb_release -rs); \
    DEBIAN_CODENAME=$(lsb_release -cs); \
    \
    # Add Microsoft GPG key to trusted keys for apt
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg; \
    echo "deb [arch=amd64,armhf,arm64] https://packages.microsoft.com/debian/${DEBIAN_VERSION}/prod ${DEBIAN_CODENAME} main" > /etc/apt/sources.list.d/mssql-release.list; \
    \
    apt-get update; \
    \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18; \
    \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src /app/src
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "src.data-api.main:app", "--host", "0.0.0.0", "--port", "8000"]



