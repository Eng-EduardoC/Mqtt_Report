FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt reportlab requests
COPY . /app
CMD ["python", "main.py"]
