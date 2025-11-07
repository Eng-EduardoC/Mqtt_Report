import os
import json
import time
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import paho.mqtt.client as mqtt
import requests

# ==================================================
# Configurações
# ==================================================
MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "Agrodigital")
MQTT_PASS = os.getenv("MQTT_PASS", "Eng2025@")
MQTT_TOPICS = os.getenv("MQTT_TOPICS", "silos/#")

WHATSAPP_INSTANCE_ID = os.getenv("WHATSAPP_INSTANCE_ID", "")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_TO = os.getenv("WHATSAPP_TO", "")

PDF_DIR = "/data/relatorios"
os.makedirs(PDF_DIR, exist_ok=True)

# ==================================================
# Funções auxiliares
# ==================================================
def gerar_pdf(payload, caminho_pdf):
    """Gera um PDF com as temperaturas recebidas via MQTT"""
    c = canvas.Canvas(caminho_pdf, pagesize=A4)
    width, height = A4

    ts = payload.get("ts", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    temps = payload.get("d", {}).get("temperature", [])

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 50, f"Relatório de Temperaturas - {ts}")

    c.setFont("Helvetica", 10)
    ypos = height - 90
    c.drawString(50, ypos, "Leituras de Temperatura (°C)")
    ypos -= 20

    for i, t in enumerate(temps, start=1):
        c.drawString(50, ypos, f"Sensor {i:02d}: {t:.2f} °C")
        ypos -= 15
        if ypos < 50:
            c.showPage()
            c.setFont("Helvetica", 10)
            ypos = height - 50

    c.showPage()
    c.save()


def enviar_pdf_whatsapp(caminho_pdf, legenda="Relatório MQTT"):
    if not (WHATSAPP_INSTANCE_ID and WHATSAPP_TOKEN and WHATSAPP_TO):
        print("⚠️ Credenciais WhatsApp não configuradas.")
        return

    # 🔧 URL corrigida: token via GET + campo 'document'
    url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE_ID}/messages/document?token={WHATSAPP_TOKEN}"

    # Abre o arquivo PDF
    files = {"document": open(caminho_pdf, "rb")}
    data = {
        "to": WHATSAPP_TO,
        "filename": os.path.basename(caminho_pdf),
        "caption": legenda,
    }

    try:
        resp = requests.post(url, data=data, files=files, timeout=30)
        print("📨 Enviado via WhatsApp:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Erro ao enviar via WhatsApp:", e)



# ==================================================
# MQTT
# ==================================================
client = mqtt.Client()

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("✅ Conectado ao broker MQTT.")
    for topic in MQTT_TOPICS.split(","):
        client.subscribe(topic.strip())
        print(f"📡 Assinado tópico: {topic.strip()}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"📥 Mensagem recebida em {msg.topic}: {payload}")

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        caminho_pdf = os.path.join(PDF_DIR, f"relatorio_{ts}.pdf")
        gerar_pdf(payload, caminho_pdf)
        enviar_pdf_whatsapp(caminho_pdf)

    except Exception as e:
        print("❌ Erro ao processar mensagem:", e)

client.on_connect = on_connect
client.on_message = on_message

if MQTT_USER and MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

print(f"🔗 Conectando ao broker {MQTT_HOST}:{MQTT_PORT} ...")
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()
