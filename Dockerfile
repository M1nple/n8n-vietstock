FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP=main.py
ENV TZ=Asia/Ho_Chi_Minh


EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
