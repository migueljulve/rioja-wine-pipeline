# Use the official lightweight Python 3.12 image
FROM python:3.12-slim

# Install 'uv' from the official binary to manage dependencies much faster than pip
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the internal working directory for all subsequent commands
WORKDIR /app

# Configure Environment Variables:
# 1. Add the virtual environment created by uv to the system PATH
# 2. Set UV to 'copy' mode for better compatibility with Docker volumes
# 3. Force Python to output logs immediately (no buffering) to see real-time progress
ENV PATH="/app/.venv/bin:$PATH"
ENV UV_LINK_MODE=copy
ENV PYTHONUNBUFFERED=1

# Copy only dependency files first to optimize Docker layer caching
# This means we don't re-install libraries every time we change the Python script
COPY pyproject.toml uv.lock ./

# Install project dependencies based on the lockfile (ensures identical versions)
RUN uv sync --frozen --no-cache

# Create a dedicated directory inside the container for Google Cloud credentials
# This acts as the 'mount point' for the volume we define in docker-compose
RUN mkdir -p /app/keys

# Copy the ingestion script into the container
COPY data_ingestion.py .

# Define the command to run when the container starts
ENTRYPOINT ["uv", "run", "python", "data_ingestion.py"]