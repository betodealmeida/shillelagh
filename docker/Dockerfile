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

# Download, build, and install multicorn2
RUN wget https://github.com/pgsql-io/multicorn2/archive/refs/tags/v2.5.tar.gz && \
    tar -xvf v2.5.tar.gz && \
    cd multicorn2-2.5 && \
    make && \
    make install


# Create a virtual environment and install dependencies
RUN python3 -m venv /code/venv && \
    /code/venv/bin/pip install --upgrade pip && \
    /code/venv/bin/pip install -e '.[all]' && \
    /code/venv/bin/pip install 'multicorn @ git+https://github.com/pgsql-io/multicorn2.git@v2.5'

# Set environment variable for PostgreSQL to use the virtual environment
ENV PATH="/code/venv/bin:$PATH"

# Switch back to the default postgres user
USER postgres
