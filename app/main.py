# ============================================================
# main.py – App MQTT → Relatório PDF único por obra → WhatsApp
# ============================================================

import os
import json
import time
import base64
import threading
from datetime import datetime, timezone
from pathlib import Path
import io

import requests
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas

from flask import Flask, request

#importa do utils de pdf
from pdf_utils import (
    texto_por_valor,
    cor_por_valor,
    gerar_relatorio_silo,
    configurar_base_dir
)

#importa do mqtt_manager
from mqtt.mqtt_manager import configurar_mqtt, iniciar_mqtt


# ============================================================
# 1. Carregamento de configuração
# ============================================================

client = None

BASE_DIR = Path(__file__).resolve().parent

configurar_base_dir(BASE_DIR)

def carregar_config():
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

# ----- WhatsApp / UltraMsg -----
wa_cfg = CONFIG.get("whatsapp", {})
WHATSAPP_INSTANCE_ID = os.getenv("WHATSAPP_INSTANCE_ID", wa_cfg.get("instance_id", ""))
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", wa_cfg.get("token", ""))

# ----- Relatório -----
rel_cfg = CONFIG.get("relatorio", {})
RELATORIO_TIMEOUT = int(rel_cfg.get("timeout_segundos", 180))

# ----- Clientes / Obras -----
CLIENTES = CONFIG.get("clientes", [])
OBRA_CONFIG = {c["obra"]: c for c in CLIENTES}


# ============================================================
# 3. Estruturas de memória
# ============================================================

leituras_obra = {}      # {obra: {silo: {"temperaturas": [...], "ts": "..."}}}
ultima_leitura = {}     # {obra: timestamp_última_mensagem}
leituras_lock = threading.Lock()


# ============================================================
# 4. Utilitários
# ============================================================

def agora_utc_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def agora_legivel():
    return datetime.now().strftime("%d-%m-%Y_%H")


def normalizar_topico(topico: str) -> str:
    return topico.strip()

# ============================================================
# 6. Gerar e enviar relatório consolidado
# ============================================================

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

    # Tenta pegar o ts do primeiro silo (para nomear o arquivo com base na coleta)
    primeiro_ts = None
    for s in dados.values():
        if s.get("ts"):
            primeiro_ts = s["ts"]
            break

    if primeiro_ts:
        try:
            datahora_relatorio = datetime.strptime(primeiro_ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            datahora_relatorio = datetime.now()
    else:
        datahora_relatorio = datetime.now()

    nome_arquivo = f"Relatorio_{obra.replace(' ', '_').title()}_{datahora_relatorio.strftime('%d-%m-%Y_%H-%M')}.pdf"

    legenda = f"📊 Relatório de Temperatura - {obra.replace('_', ' ').title()}"
    numeros_destino = cliente.get("numeros", [])

    buffer_pdf = io.BytesIO()
    c = canvas.Canvas(buffer_pdf, pagesize=landscape(A4))
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
        arcos = silo.get("arcos", None)

        ts_str = info.get("ts")
        try:
            datahora = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S") if ts_str else datetime.now()
        except ValueError:
            print(f"⚠️ TS inválido em {obra}/{nome}: {ts_str}")
            datahora = datetime.now()

        gerar_relatorio_silo(c, descricao, config, temperaturas, arcos, datahora=datahora)

    c.save()
    buffer_pdf.seek(0)

    print(f"📄 PDF da obra {obra} gerado em memória (não salvo em disco).")

    # Enviar para todos os números configurados
    numeros_destino = cliente.get("numeros", [])

    if not numeros_destino:
        print(f"⚠️ Nenhum número configurado para obra {obra}.")
    else:
        for numero in numeros_destino:
            print(f"📨 Enviando PDF para {numero}...")
            enviar_pdf_whatsapp_memoria(buffer_pdf, nome_arquivo, legenda, numero)

    # Limpar buffers após envio
    with leituras_lock:
        leituras_obra.pop(obra, None)
        ultima_leitura.pop(obra, None)



def enviar_pdf_whatsapp_memoria(buffer_pdf: io.BytesIO, nome_arquivo: str, legenda: str, numero_destino: str):
    """
    Envia PDF diretamente da memória via API UltraMsg.
    - Verifica credenciais.
    - Checa limite de tamanho (PDF e Base64).
    - Faz retry automático em caso de timeout.
    """
    if not (WHATSAPP_INSTANCE_ID and WHATSAPP_TOKEN and numero_destino):
        print("⚠️ Credenciais WhatsApp ou número não configurados.")
        return

    # ==================================================
    # 📏 Verifica tamanho do arquivo antes do envio
    # ==================================================
    pdf_bytes = buffer_pdf.getvalue()
    tamanho_mb = len(pdf_bytes) / (1024 * 1024)
    print(f"📄 Gerado PDF: {tamanho_mb:.2f} MB")

    if len(pdf_bytes) > 7.5 * 1024 * 1024:
        print("⚠️ PDF muito grande (>7.5 MB). Envio cancelado para evitar erro no UltraMsg.")
        return

    # Codifica em Base64
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    base64_len = len(pdf_b64)
    print(f"📦 Base64 length: {base64_len:,} caracteres")

    if base64_len > 10_000_000:
        print("⚠️ Base64 length excede o limite (~10.000.000). Envio cancelado.")
        return

    # ==================================================
    # Envio via UltraMsg
    # ==================================================
    url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE_ID}/messages/document"
    data = {
        "token": WHATSAPP_TOKEN,
        "to": numero_destino,
        "filename": nome_arquivo,
        "document": pdf_b64,
        "caption": legenda,
    }

    tentativas = 3
    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.post(url, data=data, timeout=(8, 25))
            print(f"📨 Tentativa {tentativa}: {resp.status_code}")
            if resp.status_code == 200:
                print("✅ Enviado com sucesso via WhatsApp!")
                break
            elif resp.status_code >= 500:
                print(f"⚠️ Erro servidor ({resp.status_code}), aguardando 10 s...")
                time.sleep(10)
            else:
                print("❌ Falha não recuperável:", resp.text)
                break
        except requests.exceptions.ReadTimeout:
            print(f"⚠️ Timeout na tentativa {tentativa}, aguardando 10 s...")
            time.sleep(10)
        except Exception as e:
            print(f"❌ Erro ao enviar na tentativa {tentativa}:", e)
            time.sleep(10)




# ============================================================
# 7. Thread de monitoramento (timeout por obra)
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
# 8. MQTT – conexão e callbacks
# ============================================================

def processar_leitura_mqtt(obra, silo, temperaturas, ts):
    with leituras_lock:
        if obra not in leituras_obra:
            leituras_obra[obra] = {}

        leituras_obra[obra][silo] = {
            "temperaturas": temperaturas,
            "ts": ts
        }

        ultima_leitura[obra] = time.time()

TOPICOS_PERMITIDOS = {
    f"temperaturas/{c['obra']}/{s['nome']}"
    for c in CLIENTES for s in c.get("unidades", [])
}

print("📡 Tópicos assinados:")
for t in TOPICOS_PERMITIDOS:
    print("  -", t)


configurar_mqtt(
    host=MQTT_HOST,
    port=MQTT_PORT,
    user=MQTT_USER,
    password=MQTT_PASS,
    topicos=TOPICOS_PERMITIDOS,
    callback_leitura=processar_leitura_mqtt
)


# ============================================================
#  🔄 BOT WHATSAPP INTEGRADO (RECEBE COMANDO E ENVIA MQTT)
# ============================================================

app = Flask(__name__)


@app.route("/webhook", methods=["POST"])
def receber_whatsapp():
    """Recebe mensagens do WhatsApp via UltraMsg Webhook"""
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
        enviar_pdf_whatsapp_mensagem(chat_id, resposta)

        topico_comando = "silos/fazenda_jk/comando"
        if client:
            client.publish(topico_comando, "iniciar_leitura")
        else:
            print("⚠️ MQTT ainda não inicializado, não foi possível publicar.")

        print(f"🚀 Publicado comando MQTT em {topico_comando}")
    else:
        resposta = (
            "🤖 Comando não reconhecido.\n"
            "Envie *Iniciar Leitura* para começar o processo de termometria."
        )
        enviar_pdf_whatsapp_mensagem(chat_id, resposta)

    return {"status": "ok"}


def enviar_pdf_whatsapp_mensagem(numero_destino, texto):
    """Envia mensagem simples de texto via UltraMsg"""
    if not (WHATSAPP_INSTANCE_ID and WHATSAPP_TOKEN and numero_destino):
        print("⚠️ Credenciais WhatsApp ou número não configurados.")
        return

    url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE_ID}/messages/chat"
    data = {
        "token": WHATSAPP_TOKEN,
        "to": numero_destino,
        "body": texto,
    }
    try:
        resp = requests.post(url, data=data, timeout=30)
        print("📨 Mensagem enviada:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Erro ao enviar mensagem:", e)


# ============================================================
# 9. Ponto de entrada
# ============================================================

def main():

    global client
    client = iniciar_mqtt()

    stop_event = threading.Event()
    t = threading.Thread(target=monitorar_agrupamento, args=(stop_event,), daemon=True)
    t.start()

    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=5000),
        daemon=True
    )
    flask_thread.start()

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("🛑 Encerrando aplicação...")
        stop_event.set()


if __name__ == "__main__":
    main()
