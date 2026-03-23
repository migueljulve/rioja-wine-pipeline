FROM python:3.12-slim

# 1. Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

# 2. Setup Environment
ENV PATH="/app/.venv/bin:$PATH"
ENV UV_LINK_MODE=copy

# 3. Copy only dependency files
# Make sure .python-version, pyproject.toml, and uv.lock exist!
COPY pyproject.toml uv.lock .python-version ./

# 4. Install dependencies (creates the .venv)
RUN uv sync --frozen --no-cache

# 5. Copy the Rioja data and script
COPY rioja_data/ ./rioja_data/
COPY data_ingestion.py .

# 6. Run the script
ENTRYPOINT ["python", "data_ingestion.py"]