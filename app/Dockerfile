# ==========================================================
# Dockerfile - App MQTT + Flask + ReportLab + WhatsApp
# ==========================================================

FROM python:3.12-slim

# Define diretório de trabalho
WORKDIR /app

# Instala dependências do sistema (para o ReportLab gerar PDFs corretamente)
RUN apt-get update && apt-get install -y \
    libfreetype6-dev libjpeg62-turbo-dev libpng-dev && \
    rm -rf /var/lib/apt/lists/*

# Instala dependências Python necessárias
RUN pip install --no-cache-dir \
    paho-mqtt \
    reportlab \
    requests \
    flask

# Copia o código-fonte (mas ignora o config.json)
COPY . /app
# Remove config.json da imagem (para evitar conflito com o volume externo)
RUN rm -f /app/config.json

# Expõe a porta do Flask
EXPOSE 5000

# Comando padrão
CMD ["python", "main.py"]
