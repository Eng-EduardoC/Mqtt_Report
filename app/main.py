# ============================================================
# main.py – App MQTT → Relatório PDF único por obra → WhatsApp
# ============================================================

import os
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
import io

import paho.mqtt.client as mqtt
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from flask import Flask, request


# ============================================================
# 1. Carregamento de configuração (config.json + variáveis de ambiente)
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

def carregar_config():
    """Lê config.json no mesmo diretório deste arquivo."""
    cfg_path = BASE_DIR / "config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = carregar_config()

# ----- MQTT -----
mqtt_cfg = CONFIG.get("mqtt", {})
MQTT_HOST = os.getenv("MQTT_HOST", mqtt_cfg.get("host", "mqtt-broker"))
MQTT_PORT = int(os.getenv("MQTT_PORT", mqtt_cfg.get("port", 1883)))
MQTT_USER = os.getenv("MQTT_USER", mqtt_cfg.get("user", ""))
MQTT_PASS = os.getenv("MQTT_PASS", mqtt_cfg.get("pass", ""))

# ----- WasenderAPI -----
WASENDER_API_KEY = os.getenv("WASENDER_API_KEY", "")

# ----- Relatório -----
rel_cfg = CONFIG.get("relatorio", {})
RELATORIO_TIMEOUT = int(rel_cfg.get("timeout_segundos", 180))

# ----- Clientes / Obras -----
CLIENTES = CONFIG.get("clientes", [])
OBRA_CONFIG = {c["obra"]: c for c in CLIENTES}


# ============================================================
# 2. Estruturas de memória
# ============================================================

leituras_obra = {}      # {obra: {silo: {"temperaturas": [...], "ts": "..."}}}
ultima_leitura = {}     # {obra: timestamp_última_mensagem}
leituras_lock = threading.Lock()


# ============================================================
# 3. Utilitários
# ============================================================

def agora_utc_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def agora_legivel():
    """Retorna data e hora legível para nomes de arquivos."""
    return datetime.now().strftime("%d-%m-%Y_%H")

def normalizar_topico(topico: str) -> str:
    return topico.strip()


# ============================================================
# 4. Funções de cor e desenho do relatório
# ============================================================

def cor_por_temp(temp: int):
    """Mapeia temperatura para cor térmica."""
    if temp <= 9:
        return colors.HexColor("#00BFFF")  # Azul claro
    elif temp <= 19:
        return colors.HexColor("#32CD32")  # Verde
    elif temp <= 29:
        return colors.HexColor("#FFD700")  # Amarelo
    elif temp <= 40:
        return colors.HexColor("#FF4500")  # Vermelho
    elif temp <= 60:
        return colors.HexColor("#8B4513")  # Vermelho
    else:
        return colors.HexColor("#9B9B9B")  # Cinza


def gerar_relatorio_silo(c, descricao, config, temperaturas, logo_path="logo.png"):
    """Gera uma página térmica centralizada e organizada."""
    largura, altura = A4
    c.setFillColor(colors.white)
    c.rect(0, 0, largura, altura, fill=1, stroke=0)

    # --- Cabeçalho ---
    if os.path.exists(logo_path):
        c.drawImage(logo_path, 50, altura - 100, width=80, height=60, preserveAspectRatio=True)

    c.setFont("Helvetica-Bold", 18)
    c.setFillColor(colors.black)
    c.drawString(150, altura - 60, f"Relatório Térmico - {descricao}")

    c.setFont("Helvetica", 11)
    c.drawString(150, altura - 80, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    # --- Matriz térmica ---
    total_cabos = len(config)
    max_sensores = max(config)
    tamanho_celula = 25

    largura_matriz = total_cabos * tamanho_celula
    altura_matriz = max_sensores * tamanho_celula

    inicio_x = (largura - largura_matriz) / 2
    inicio_y = (altura - altura_matriz) / 2

    idx_temp = 0

    for col, sensores in enumerate(config):
        for linha in range(sensores):
            if idx_temp >= len(temperaturas):
                break
            temp = int(temperaturas[idx_temp])
            cor = cor_por_temp(temp)
            x = inicio_x + col * tamanho_celula
            y = inicio_y + linha * tamanho_celula

            c.setFillColor(cor)
            c.rect(x, y, tamanho_celula, tamanho_celula, fill=1, stroke=0)
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 7)
            c.drawCentredString(x + tamanho_celula / 2, y + tamanho_celula / 2 - 3, str(temp))
            idx_temp += 1

    # --- Rótulos e legenda ---
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(colors.black)

    for i in range(max_sensores):
        label = f"S{i+1:02}"
        y_label = inicio_y + i * tamanho_celula + tamanho_celula / 2 - 3
        c.drawRightString(inicio_x - 10, y_label, label)

    for i in range(total_cabos):
        label = f"CB{i+1:02}"
        x_label = inicio_x + i * tamanho_celula + tamanho_celula / 2
        c.drawCentredString(x_label, inicio_y + altura_matriz + 12, label)

    legenda_itens = [
        ("#00BFFF", "Azul – Ótimo"),
        ("#32CD32", "Verde – Bom"),
        ("#FFD700", "Amarelo – Alerta"),
        ("#FF4500", "Vermelho – Ruim"),
        ("#8B4513", "Marrom – Péssimo")
    ]

    legenda_y = inicio_y - 70
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(largura / 2, legenda_y + 30, "Legenda de cores")

    bloco_larg = 90
    espacamento_x = 10
    total_largura_legenda = len(legenda_itens) * (bloco_larg + espacamento_x)
    inicio_legenda_x = (largura - total_largura_legenda) / 2

    for i, (cor_hex, texto) in enumerate(legenda_itens):
        x_leg = inicio_legenda_x + i * (bloco_larg + espacamento_x)
        c.setFillColor(colors.HexColor(cor_hex))
        c.rect(x_leg, legenda_y, 12, 12, fill=1, stroke=0)
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 8)
        c.drawString(x_leg + 16, legenda_y + 3, texto)

    c.showPage()


# ============================================================
# 5. Envio via WasenderAPI (upload + envio)
# ============================================================

def upload_pdf_wasender(buffer_pdf: io.BytesIO, nome_arquivo: str) -> str:
    """Faz upload do PDF e retorna o link público (conforme API Wasender /api/upload)."""
    url = "https://wasenderapi.com/api/upload"
    headers = {"Content-Type": "application/pdf"}

    try:
        buffer_pdf.seek(0)
        resp = requests.post(url, headers=headers, data=buffer_pdf.getvalue(), timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(f"❌ Falha no upload: {data}")

        public_url = data.get("publicUrl")
        print(f"✅ Upload concluído. URL pública: {public_url}")
        return public_url

    except Exception as e:
        print("❌ Erro no upload do PDF:", e)
        if 'resp' in locals():
            print("🧩 Resposta da API:", resp.text)
        return None


def enviar_pdf_wasender(buffer_pdf: io.BytesIO, nome_arquivo: str, legenda: str, numero_destino: str):
    """Faz upload do PDF e envia o documento via WasenderAPI."""
    link = upload_pdf_wasender(buffer_pdf, nome_arquivo)
    if not link:
        print("⚠️ Upload falhou, não foi possível enviar o PDF.")
        return

    url = "https://wasenderapi.com/api/send-message"
    headers = {
        "Authorization": f"Bearer {WASENDER_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {"to": numero_destino, "text": legenda, "documentUrl": link}

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=60)
        print("📨 PDF enviado via WasenderAPI:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Erro ao enviar PDF via WasenderAPI:", e)


def enviar_texto_wasender(numero_destino: str, texto: str):
    """Envia mensagem simples de texto via WasenderAPI."""
    url = "https://wasenderapi.com/api/send-message"
    headers = {
        "Authorization": f"Bearer {WASENDER_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {"to": numero_destino, "text": texto}

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        print("💬 Mensagem enviada via WasenderAPI:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Erro ao enviar mensagem de texto:", e)


def gerar_e_enviar_relatorio_obra(obra: str):
    """Gera PDF em memória e envia via WhatsApp (sem salvar em disco)."""
    cliente = OBRA_CONFIG.get(obra)
    if not cliente:
        print(f"⚠️ Obra {obra} não encontrada em CONFIG.")
        return

    dados = leituras_obra.get(obra, {})
    if not dados:
        print(f"⚠️ Nenhum dado recebido para {obra}.")
        return

    nome_arquivo = f"Relatorio_{obra.replace(' ', '_').title()}_{agora_legivel()}.pdf"
    legenda = f"📊 Relatório de Temperatura - {obra.replace('_', ' ').title()}"
    numero = cliente.get("numero")

    buffer_pdf = io.BytesIO()
    c = canvas.Canvas(buffer_pdf, pagesize=A4)
    c.setTitle(f"Relatório Térmico - {obra.replace('_', ' ').title()}")

    for silo in cliente["unidades"]:
        nome = silo["nome"]
        descricao = silo.get("descricao", nome)
        config = silo.get("config", [])
        info = dados.get(nome)
        if not info:
            print(f"⚠️ Sem dados para {obra}/{nome}, pulando...")
            continue
        temperaturas = [int(t) for t in info.get("temperaturas", [])]
        gerar_relatorio_silo(c, descricao, config, temperaturas)

    c.save()
    buffer_pdf.seek(0)

    print(f"📄 PDF da obra {obra} gerado em memória.")
    enviar_pdf_wasender(buffer_pdf, nome_arquivo, legenda, numero)

    with leituras_lock:
        leituras_obra.pop(obra, None)
        ultima_leitura.pop(obra, None)


# ============================================================
# 6. Thread de monitoramento (timeout por obra)
# ============================================================

def monitorar_agrupamento(stop_event: threading.Event):
    print(f"⏱️ Monitor de agrupamento iniciado. Timeout: {RELATORIO_TIMEOUT}s")
    while not stop_event.is_set():
        agora = time.time()
        obras_para_fechar = []
        with leituras_lock:
            for obra, t_ultimo in ultima_leitura.items():
                if agora - t_ultimo >= RELATORIO_TIMEOUT:
                    obras_para_fechar.append(obra)
        for obra in obras_para_fechar:
            try:
                print(f"🧾 Tempo limite atingido para {obra}, gerando relatório...")
                gerar_e_enviar_relatorio_obra(obra)
            except Exception as e:
                print(f"❌ Erro ao gerar/enviar relatório da obra {obra}: {e}")
        stop_event.wait(30)


# ============================================================
# 7. MQTT – conexão e callbacks
# ============================================================

TOPICOS_PERMITIDOS = set()
for cliente in CLIENTES:
    obra = cliente["obra"]
    for silo in cliente.get("unidades", []):
        nome_silo = silo["nome"]
        TOPICOS_PERMITIDOS.add(f"temperaturas/{obra}/{nome_silo}")

print("📡 Tópicos assinados:")
for t in TOPICOS_PERMITIDOS:
    print("  -", t)

client = mqtt.Client()

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Conectado ao broker MQTT.")
        for topico in TOPICOS_PERMITIDOS:
            client.subscribe(topico)
            print(f"📡 Assinado: {topico}")
    else:
        print(f"❌ Falha na conexão (rc={rc})")

def on_message(client, userdata, msg):
    try:
        topico = normalizar_topico(msg.topic)
        if topico not in TOPICOS_PERMITIDOS:
            print(f"⚠️ Mensagem ignorada: {topico}")
            return

        dados = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        temperaturas = dados.get("d", {}).get("temperature", [])
        ts = dados.get("ts", agora_utc_iso())
        if not isinstance(temperaturas, list) or not temperaturas:
            print(f"⚠️ Payload sem temperaturas em {topico}: {dados}")
            return

        _, obra, silo = topico.split("/")
        print(f"📥 {obra}/{silo}: {len(temperaturas)} temps, ts={ts}")

        with leituras_lock:
            if obra not in leituras_obra:
                leituras_obra[obra] = {}
            leituras_obra[obra][silo] = {"temperaturas": [int(t) for t in temperaturas], "ts": ts}
            ultima_leitura[obra] = time.time()

    except Exception as e:
        print("❌ Erro ao processar mensagem MQTT:", e)

client.on_connect = on_connect
client.on_message = on_message
if MQTT_USER and MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)


# ============================================================
# 8. Webhook WhatsApp (WasenderAPI)
# ============================================================

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def receber_whatsapp():
    """Recebe mensagens do WhatsApp via WasenderAPI Webhook"""
    data = request.json or {}
    print("📩 Webhook WhatsApp recebido:", data)

    msg = data.get("data", {}).get("body", "").strip().lower()
    chat_id = data.get("data", {}).get("chatId")

    if "iniciar leitura" in msg:
        resposta = (
            "✅ O sistema de termometria iniciou a leitura.\n"
            "⏳ Aguarde aproximadamente *10 minutos*.\n"
            "📄 O relatório será enviado automaticamente."
        )
        enviar_texto_wasender(chat_id, resposta)

        topico_comando = "silos/fazenda_jk/comando"
        client.publish(topico_comando, "iniciar_leitura")
        print(f"🚀 Publicado comando MQTT em {topico_comando}")

    else:
        resposta = (
            "🤖 Comando não reconhecido.\n"
            "Envie *Iniciar Leitura* para começar o processo de termometria."
        )
        enviar_texto_wasender(chat_id, resposta)

    return {"status": "ok"}


# ============================================================
# 9. Ponto de entrada
# ============================================================

def main():
    print(f"🔗 Conectando ao broker {MQTT_HOST}:{MQTT_PORT} ...")
    client.connect(MQTT_HOST, MQTT_PORT, 60)

    stop_event = threading.Event()
    threading.Thread(target=monitorar_agrupamento, args=(stop_event,), daemon=True).start()
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=5000), daemon=True).start()

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("🛑 Encerrando aplicação...")
        stop_event.set()


if __name__ == "__main__":
    main()
