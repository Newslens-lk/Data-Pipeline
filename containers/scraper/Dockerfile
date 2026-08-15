FROM python:3.11-slim

# Chromium for selenium-based scrapers (AdaDeranaSinhalaLk)
RUN apt-get update && \
    apt-get install -y --no-install-recommends chromium chromium-driver git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Clone lk_news repo (not pip-installable, no setup.py)
RUN git clone --depth 1 https://github.com/nuuuwan/lk_news.git /app/lk_news_repo
ENV PYTHONPATH="/app/lk_news_repo/src:${PYTHONPATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run.py .

ENTRYPOINT ["python", "run.py"]
