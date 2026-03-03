FROM python:3.11-slim

WORKDIR /app

# install project dependencies from pyproject.toml
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# copy producer and consumer application code
COPY producer.py consumer.py ./

# default command (overridden per service in docker-compose)
CMD ["python"]
