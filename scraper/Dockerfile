FROM python:3.12-slim

# Install Poetry
RUN pip install --no-cache-dir poetry==1.8.3

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml poetry.lock* ./

# Install dependencies without creating a venv (we're in a container)
RUN poetry config virtualenvs.create false \
 && poetry install --only main --no-interaction --no-ansi

# Copy source code
COPY . .

CMD ["python", "scrape.py"]
