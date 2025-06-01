FROM python:3.12-slim-bookworm

# Create a non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy only poetry config files first to leverage Docker cache
COPY pyproject.toml ./

# Install poetry and dependencies
RUN pip install --no-cache-dir poetry

# Configure poetry to not create virtual environment (we're in a container)
RUN poetry config virtualenvs.create false

# Install project dependencies
RUN poetry install --no-root --no-dev

# Copy the rest of your application code
COPY . .

# Change ownership of the app directory to the non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set environment variable for PYTHONPATH
ENV PYTHONPATH=/app

# Expose ports if running FastAPI directly from the image
EXPOSE 8000

# No default CMD, as it will be overridden by docker-compose for each service
