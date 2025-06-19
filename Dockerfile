FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install playwright
RUN playwright install chromium
RUN apt-get update && apt-get install -y \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2

COPY . .

ENV FLASK_APP=main.py
ENV TZ=Asia/Ho_Chi_Minh


EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
