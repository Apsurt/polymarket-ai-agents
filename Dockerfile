FROM python:3.10-slim-bookworm

WORKDIR /app

# Copy only poetry config files first to leverage Docker cache
COPY pyproject.toml poetry.lock* ./

# Install poetry and dependencies
RUN pip install poetry
# Install project dependencies, using --only main for production dependencies
RUN poetry install --no-root --only main

# Copy the rest of your application code
COPY . .

# Set environment variable for PYTHONPATH
ENV PYTHONPATH=/app

# Expose ports if running FastAPI directly from the image
EXPOSE 8000

# No default CMD, as it will be overridden by docker-compose for each service
