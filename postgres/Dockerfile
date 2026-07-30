# Use the official Postgres image as a base
FROM postgres:13

WORKDIR /code
COPY . /code

# Use root for package installation
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    postgresql-server-dev-13 \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    wget

# Create a virtual environment for the Python dependencies
RUN python3 -m venv /code/venv && \
    /code/venv/bin/pip install --upgrade pip

# Download, build, and install multicorn2. Point its Makefile at the venv pip
# via PIP: the Python package step runs `pip install --upgrade 'pip>=23'`,
# which fails on postgres:13's system pip (no RECORD metadata to uninstall).
# The native extension is still built and installed via PGXS.
RUN wget https://github.com/pgsql-io/multicorn2/archive/refs/tags/v2.5.tar.gz && \
    tar -xvf v2.5.tar.gz && \
    cd multicorn2-2.5 && \
    make && \
    make install PIP=/code/venv/bin/pip

# Install shillelagh and its dependencies into the same virtual environment
RUN /code/venv/bin/pip install -e '.[all]'

# Set environment variable for PostgreSQL to use the virtual environment
ENV PATH="/code/venv/bin:$PATH"

# Switch back to the default postgres user
USER postgres
