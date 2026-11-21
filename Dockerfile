FROM python:3.12-slim
RUN apt-get update \
&& apt-get install -y --no-install-recommends make \
&& rm -rf /var/lib/apt/lists/*
WORKDIR /srv
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt pyproject.toml Makefile ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY worker ./worker
COPY tests ./tests
COPY scripts ./scripts
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]