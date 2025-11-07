# ==========================================================
# Dockerfile - App MQTT + Flask + ReportLab + WhatsApp
# ==========================================================

FROM python:3.12-slim

# Define diretório de trabalho
WORKDIR /app

# Instala dependências do sistema (para evitar erros com PDF/reportlab)
RUN apt-get update && apt-get install -y \
    libfreetype6-dev libjpeg62-turbo-dev libpng-dev && \
    rm -rf /var/lib/apt/lists/*

# Instala dependências Python necessárias
RUN pip install --no-cache-dir \
    paho-mqtt \
    reportlab \
    requests \
    flask

# Copia o código do app
COPY . /app

# Expõe a porta do Flask (caso precise receber webhooks)
EXPOSE 5000

# Comando padrão de execução
CMD ["python", "main.py"]
