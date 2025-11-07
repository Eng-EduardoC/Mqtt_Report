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

import paho.mqtt.client as mqtt
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors


# ============================================================
# 1. Carregamento de configuração (config.json + variáveis de ambiente)
# ============================================================

BASE_DIR = Path(__file__).resolve().parent


def carregar_config():
    """
    Lê config.json no mesmo diretório deste arquivo.
    Exemplo:

    {
      "mqtt": { "host": "mqtt-broker", "port": 1883, "user": "", "pass": "" },
      "whatsapp": { "instance_id": "instance148636", "token": "xxxxx" },
      "relatorio": { "timeout_segundos": 180 },
      "clientes": [
        {
          "obra": "fazenda_jk",
          "numero": "+5584999999999",
          "silos": [
            { "nome": "silo_01", "descricao": "Silo 01 - Fazenda JK", "config": [9,9,9,9,9,10] },
            { "nome": "silo_02", "descricao": "Silo 02 - Fazenda JK", "config": [6,6,6,6,6] }
          ]
        }
      ]
    }
    """
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
# 2. Diretórios de saída
# ============================================================

DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RELATORIOS_DIR = DATA_DIR / "relatorios"
RELATORIOS_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Diretório de relatórios: {RELATORIOS_DIR}")


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
    """Retorna data e hora em formato legível para nomes de arquivos."""
    return datetime.now().strftime("%d-%m-%Y_%H")


def normalizar_topico(topico: str) -> str:
    return topico.strip()


# ============================================================
# 5. Funções de cor e desenho do relatório
# ============================================================

def cor_por_temp(temp: int):
    """Mapeia a temperatura para cor térmica (visual tipo legenda colorida)."""
    if temp <= 9:
        return colors.HexColor("#00BFFF")  # Azul claro / Ciano
    elif temp <= 19:
        return colors.HexColor("#32CD32")  # Verde
    elif temp <= 29:
        return colors.HexColor("#FFD700")  # Amarelo
    elif temp <= 40:
        return colors.HexColor("#FF4500")  # Vermelho
    else:
        return colors.HexColor("#8B4513")  # Marrom



def gerar_relatorio_silo(c, descricao, config, temperaturas, logo_path="logo.png"):
    """
    Gera uma página térmica centralizada e bonita.
    Cabeçalho + matriz + legenda organizada.
    """
    largura, altura = A4
    c.setFillColor(colors.white)
    c.rect(0, 0, largura, altura, fill=1, stroke=0)

    # --- CABEÇALHO ---
    if os.path.exists(logo_path):
        c.drawImage(logo_path, 50, altura - 100, width=80, height=60, preserveAspectRatio=True)

    c.setFont("Helvetica-Bold", 18)
    c.setFillColor(colors.black)
    c.drawString(150, altura - 60, f"Relatório Térmico - {descricao}")

    c.setFont("Helvetica", 11)
    c.drawString(150, altura - 80, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    # --- PARÂMETROS DA MATRIZ ---
    total_cabos = len(config)
    max_sensores = max(config)
    tamanho_celula = 25
    espacamento = 0

    largura_matriz = total_cabos * tamanho_celula
    altura_matriz = max_sensores * tamanho_celula

    inicio_x = (largura - largura_matriz) / 2
    inicio_y = (altura - altura_matriz) / 2

    idx_temp = 0

    # --- DESENHO DA MATRIZ ---
    for col, sensores in enumerate(config):
        for linha in range(sensores):
            if idx_temp >= len(temperaturas):
                break

            temp = int(temperaturas[idx_temp])
            cor = cor_por_temp(temp)

            # y sobe (S1 embaixo)
            x = inicio_x + col * tamanho_celula
            y = inicio_y + linha * tamanho_celula

            c.setFillColor(cor)
            c.rect(x, y, tamanho_celula, tamanho_celula, fill=1, stroke=0)

            # valor
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 7)
            c.drawCentredString(x + tamanho_celula / 2, y + tamanho_celula / 2 - 3, str(temp))

            idx_temp += 1

    # --- EIXOS ---
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(colors.black)

    # Sensores (S01, S02...) à esquerda
    for i in range(max_sensores):
        label = f"S{i+1:02}"
        y_label = inicio_y + i * tamanho_celula + tamanho_celula / 2 - 3
        c.drawRightString(inicio_x - 10, y_label, label)

    # Cabos (CB01, CB02...) acima
    for i in range(total_cabos):
        label = f"CB{i+1:02}"
        x_label = inicio_x + i * tamanho_celula + tamanho_celula / 2
        c.drawCentredString(x_label, inicio_y + altura_matriz + 12, label)

    # --- LEGENDA ---
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

    # desenhar blocos lado a lado
    bloco_larg = 90
    bloco_alt = 14
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
# 6. Gerar e enviar relatório consolidado
# ============================================================

def enviar_pdf_whatsapp(caminho_pdf: Path, legenda: str, numero_destino: str):
    """Envia PDF em base64 via API UltraMsg."""
    if not (WHATSAPP_INSTANCE_ID and WHATSAPP_TOKEN and numero_destino):
        print("⚠️ Credenciais WhatsApp ou número não configurados.")
        return

    url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE_ID}/messages/document"
    with caminho_pdf.open("rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode("utf-8")

    data = {
        "token": WHATSAPP_TOKEN,
        "to": numero_destino,
        "filename": caminho_pdf.name,
        "document": pdf_b64,
        "caption": legenda,
    }

    try:
        resp = requests.post(url, data=data, timeout=60)
        print("📨 Enviado via WhatsApp:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Erro ao enviar via WhatsApp:", e)


def gerar_e_enviar_relatorio_obra(obra: str):
    """Gera PDF com 1 página por silo e envia via WhatsApp."""
    cliente = OBRA_CONFIG.get(obra)
    if not cliente:
        print(f"⚠️ Obra {obra} não encontrada em CONFIG.")
        return

    dados = leituras_obra.get(obra, {})
    if not dados:
        print(f"⚠️ Nenhum dado recebido para {obra}.")
        return

    nome_arquivo = f"Relatorio_{obra.replace(' ', '_').title()}_{agora_legivel()}.pdf"
    caminho_pdf = RELATORIOS_DIR / nome_arquivo

    c = canvas.Canvas(str(caminho_pdf), pagesize=A4)
    c.setTitle(f"Relatório Térmico - {obra.replace('_', ' ').title()}")
    c.setAuthor("AgroDigital Engenharia")
    c.setSubject(f"Monitoramento térmico consolidado - {obra.replace('_', ' ').title()}")
    c.setKeywords("Relatório térmico, termometria, silos, AgroDigital")

    for silo in cliente["silos"]:
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
    print(f"📄 PDF consolidado da obra {obra} gerado: {caminho_pdf}")

    numero = cliente.get("numero")
    legenda = f"📊 Relatório de Temperatura - {obra.replace('_', ' ').title()}"
    enviar_pdf_whatsapp(caminho_pdf, legenda, numero)

    # Limpa dados após envio
    with leituras_lock:
        leituras_obra.pop(obra, None)
        ultima_leitura.pop(obra, None)


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

TOPICOS_PERMITIDOS = set()
for cliente in CLIENTES:
    obra = cliente["obra"]
    for silo in cliente.get("silos", []):
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
            print(f"⚠️ Mensagem ignorada (tópico não configurado): {topico}")
            return

        dados = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        temperaturas = dados.get("d", {}).get("temperature", [])
        ts = dados.get("ts", agora_utc_iso())
        if not isinstance(temperaturas, list) or not temperaturas:
            print(f"⚠️ Payload sem temperaturas em {topico}: {dados}")
            return

        partes = topico.split("/")
        if len(partes) < 3:
            print(f"⚠️ Tópico inválido: {topico}")
            return
        _, obra, silo = partes[0], partes[1], partes[2]

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
# 9. Ponto de entrada
# ============================================================

def main():
    print(f"🔗 Conectando ao broker {MQTT_HOST}:{MQTT_PORT} ...")
    client.connect(MQTT_HOST, MQTT_PORT, 60)

    stop_event = threading.Event()
    threading.Thread(target=monitorar_agrupamento, args=(stop_event,), daemon=True).start()

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("🛑 Encerrando aplicação...")
        stop_event.set()


if __name__ == "__main__":
    main()
