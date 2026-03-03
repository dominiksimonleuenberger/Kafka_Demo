FROM python:3.11-slim

# Python output sent straight to container logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# copy all project files
COPY pyproject.toml .
COPY src/ ./src/

# install project dependencies from pyproject.toml
RUN pip install --no-cache-dir .

# default command (overridden per service in docker-compose)
CMD ["python"]
