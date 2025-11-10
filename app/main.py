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

import paho.mqtt.client as mqtt
import requests
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.lib import colors

from flask import Flask, request


# ============================================================
# 1. Carregamento de configuração
# ============================================================

BASE_DIR = Path(__file__).resolve().parent


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
# 5. Funções de cor e desenho do relatório
# ============================================================

def texto_por_valor(v: int) -> str:
    """
    Converte o valor recebido em texto para exibição na célula:
    - 85–89 → -5 a -1 (faixa negativa)
    - 92–99 → códigos SR, NE, ...
    - demais → o próprio número.
    """
    if 85 <= v <= 89:
        return str(v - 90)  # 85→-5, 89→-1

    mapa_erros = {
        92: "SR",
        93: "NE",
        94: "TN",
        95: "SE",
        96: "SI",
        97: "TA",
        98: "SC",
        99: ".",
    }
    return mapa_erros.get(v, str(v))


def cor_por_valor(v: int):
    """
    Define a cor conforme a faixa:
      0–9       → Azul
      10–19     → Verde
      20–29     → Amarelo
      30–60     → Vermelho
      85–89     → Azul (negativos -5 a -1)
      92–99     → Cinza (erro)
      outros    → Cinza claro (fora de faixa)
    """
    if 0 <= v <= 9 or 85 <= v <= 89:
        return colors.HexColor("#00BFFF")  # Azul claro / Ciano
    elif 10 <= v <= 19:
        return colors.HexColor("#32CD32")  # Verde
    elif 20 <= v <= 29:
        return colors.HexColor("#FFD700")  # Amarelo
    elif 30 <= v <= 60:
        return colors.HexColor("#FF4500")  # Vermelho
    elif 92 <= v <= 99:
        return colors.HexColor("#A9A9A9")  # Cinza (erro)
    else:
        return colors.HexColor("#C0C0C0")  # Fora da faixa / inválido


def gerar_relatorio_silo(c, descricao, config, temperaturas):
    """
    Gera o relatório térmico de um silo, em modo paisagem,
    com até 2 linhas de cabos por página, e célula dimensionada de forma dinâmica.
    """
    # Constrói matriz de colunas a partir do vetor linear de temperaturas
    colunas = []
    idx = 0
    for nsens in config:
        col_vals = []
        for _ in range(nsens):
            if idx >= len(temperaturas):
                break
            col_vals.append(int(temperaturas[idx]))
            idx += 1
        colunas.append(col_vals)

    total_cabos = len(colunas)
    if total_cabos == 0:
        return

    max_sensores = max((len(col) for col in colunas), default=0)
    if max_sensores == 0:
        return

    largura, altura = landscape(A4)

    # Regras de layout
    MAX_CABOS_POR_LINHA = 36
    MAX_LINHAS_CABOS_POR_PAGINA = 2
    CABOS_POR_PAGINA = MAX_CABOS_POR_LINHA * MAX_LINHAS_CABOS_POR_PAGINA

    # Margens e espaçamentos
    MARGEM_X = 80
    MARGEM_TOPO = 110
    MARGEM_RODAPE = 70
    GAP_ENTRE_LINHAS_CABOS = 40

    # Paginação por grupos de cabos
    primeiro_page = True
    for inicio_pag in range(0, total_cabos, CABOS_POR_PAGINA):
        # Se não é a primeira página deste silo, avança página
        if not primeiro_page:
            c.showPage()
        primeiro_page = False

        c.setPageSize(landscape(A4))
        c.setFillColor(colors.white)
        c.rect(0, 0, largura, altura, fill=1, stroke=0)

        # Cabeçalho moderno
        CABECALHO_ALTURA = 60  # reduz fundo
        MARGEM_INFERIOR_CABECALHO = 20  # espaço em branco entre o fundo e o início da matriz
        cor_fundo_cabecalho = colors.HexColor("#F0F4F8")  # cinza claro azulado
        c.setFillColor(cor_fundo_cabecalho)
        c.rect(0, altura - CABECALHO_ALTURA, largura, CABECALHO_ALTURA, fill=1, stroke=0)

        # Logo (ajuste o caminho conforme necessário)
        logo_path = os.path.join(BASE_DIR, "assets", "logo eletromaass.png")
        if os.path.exists(logo_path):
            logo_altura = 40
            logo_largura = 80
            y_logo = altura - (CABECALHO_ALTURA / 2) - (logo_altura / 2)
            c.drawImage(
                logo_path, 
                40, y_logo, 
                width=logo_largura, 
                height=logo_altura, 
                preserveAspectRatio=True, 
                mask='auto'  # <- importante para o fundo transparente
            )

        # Texto central (título e data)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 18)
        titulo = f"Relatório Térmico - {descricao}"
        data_str = datetime.now().strftime("%d/%m/%Y %H:%M")

        # Centraliza texto
        text_width = c.stringWidth(titulo, "Helvetica-Bold", 18)
        data_width = c.stringWidth(data_str, "Helvetica", 11)
        centro_x = largura / 2

        # === Texto centralizado dentro do fundo do cabeçalho ===
        centro_y_cabecalho = altura - (CABECALHO_ALTURA / 2)

        # Título centralizado horizontal e verticalmente
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(centro_x, centro_y_cabecalho + 6, f"Relatório Térmico - {descricao}")

        # Data logo abaixo (um pouco menor)
        c.setFont("Helvetica", 11)
        c.drawCentredString(centro_x, centro_y_cabecalho - 12, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

        # Define os cabos desta página
        fim_pag = min(inicio_pag + CABOS_POR_PAGINA, total_cabos)
        indices_pag = list(range(inicio_pag, fim_pag))

        # Divide em até 2 linhas de cabos
        indices_linha1 = indices_pag[:MAX_CABOS_POR_LINHA]
        indices_linha2 = indices_pag[MAX_CABOS_POR_LINHA:]
        linhas_indices = [indices_linha1]
        if indices_linha2:
            linhas_indices.append(indices_linha2)

        num_linhas_cabos = len(linhas_indices)
        max_cabos_em_uma_linha = max(len(l) for l in linhas_indices)

        # Área útil vertical para a matriz
        altura_disponivel = altura - MARGEM_TOPO - MARGEM_RODAPE
        largura_disponivel = largura - 2 * MARGEM_X

        # Cálculo do tamanho da célula
        # Altura: max_sensores linhas * nº de blocos de cabos + gaps
        altura_matrizes = max_sensores * num_linhas_cabos
        altura_total_com_gaps = altura_matrizes + (num_linhas_cabos - 1) * (GAP_ENTRE_LINHAS_CABOS / 10)

        # Cálculo aproximado de altura por célula
        tamanho_celula_h = (altura_disponivel - (num_linhas_cabos - 1) * GAP_ENTRE_LINHAS_CABOS) / (max_sensores * num_linhas_cabos)
        tamanho_celula_w = largura_disponivel / max_cabos_em_uma_linha

        tamanho_celula = min(tamanho_celula_h, tamanho_celula_w, 25)
        tamanho_celula = max(tamanho_celula, 8)  # limite mínimo

        # Altura total real das matrizes (com gaps)
        altura_total_matrizes = max_sensores * tamanho_celula * num_linhas_cabos + (num_linhas_cabos - 1) * GAP_ENTRE_LINHAS_CABOS

        # Y inicial para centralizar verticalmente
        inicio_y_global = ((altura - altura_total_matrizes) / 2) - MARGEM_INFERIOR_CABECALHO

        # Desenho das matrizes (uma ou duas "fileiras" de cabos)
        c.setLineWidth(0.5)
        for idx_linha, indices_cabos in enumerate(reversed(linhas_indices)):
            y_base = inicio_y_global + idx_linha * (max_sensores * tamanho_celula + GAP_ENTRE_LINHAS_CABOS)
            num_cabos_linha = len(indices_cabos)
            largura_matriz = num_cabos_linha * tamanho_celula
            x_inicio = (largura - largura_matriz) / 2

            # Células
            c.setFont("Helvetica-Bold", 6)
            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                col = colunas[idx_cabo]
                x_cabo = x_inicio + pos_cabo * tamanho_celula
                for linha_sensor, v in enumerate(col):
                    y_cel = y_base + linha_sensor * tamanho_celula
                    cor = cor_por_valor(v)
                    texto = texto_por_valor(v)

                    c.setFillColor(cor)
                    c.rect(x_cabo, y_cel, tamanho_celula, tamanho_celula, fill=1, stroke=0)

                    c.setFillColor(colors.black)
                    c.drawCentredString(
                        x_cabo + tamanho_celula / 2,
                        y_cel + tamanho_celula / 2 - 2,
                        texto
                    )

            # Eixo de sensores (Sxx) à esquerda dessa fileira
            c.setFont("Helvetica-Bold", 7)
            c.setFillColor(colors.black)
            for i in range(max_sensores):
                y_label = y_base + i * tamanho_celula + tamanho_celula / 2 - 3
                c.drawRightString(x_inicio - 8, y_label, f"S{i+1:02}")

            # Eixo de cabos (CBxx) acima da fileira – tamanho dinâmico
            fonte_cabos = max(4, min(8, tamanho_celula * 0.35))
            c.setFont("Helvetica-Bold", fonte_cabos)

            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                x_label = x_inicio + pos_cabo * tamanho_celula + tamanho_celula / 2
                y_label_cabo = y_base + max_sensores * tamanho_celula + (tamanho_celula * 0.3)
                c.drawCentredString(x_label, y_label_cabo, f"C{idx_cabo + 1:02}")


        # Linha divisória entre as duas fileiras, se existirem duas
        if num_linhas_cabos == 2:
            y_meio = inicio_y_global + max_sensores * tamanho_celula + GAP_ENTRE_LINHAS_CABOS / 2
            c.setStrokeColor(colors.lightgrey)
            c.setLineWidth(0.5)
            c.line(MARGEM_X / 2, y_meio, largura - MARGEM_X / 2, y_meio)

        # Legenda (apenas cores normais, sem erro/fora de faixa)
        legenda = [
            ("#00BFFF", "Azul – Ótimo"),
            ("#32CD32", "Verde – Bom"),
            ("#FFD700", "Amarelo – Alerta"),
            ("#FF4500", "Vermelho – Crítico"),
        ]
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(colors.black)
        c.drawCentredString(largura / 2, 45, "Legenda de Cores")

        total_largura_legenda = len(legenda) * 120
        inicio_legenda_x = (largura - total_largura_legenda) / 2
        for i, (cor_hex, texto) in enumerate(legenda):
            x_leg = inicio_legenda_x + i * 120
            c.setFillColor(colors.HexColor(cor_hex))
            c.rect(x_leg, 25, 12, 12, fill=1, stroke=0)
            c.setFillColor(colors.black)
            c.setFont("Helvetica", 7)
            c.drawString(x_leg + 16, 27, texto)

    # Ao final deste silo, deixa uma página em branco para o próximo silo (mesmo comportamento antigo)
    c.showPage()


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

    nome_arquivo = f"Relatorio_{obra.replace(' ', '_').title()}_{agora_legivel()}.pdf"
    legenda = f"📊 Relatório de Temperatura - {obra.replace('_', ' ').title()}"
    numero = cliente.get("numero")

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
        gerar_relatorio_silo(c, descricao, config, temperaturas)

    c.save()
    buffer_pdf.seek(0)

    print(f"📄 PDF da obra {obra} gerado em memória (não salvo em disco).")
    enviar_pdf_whatsapp_memoria(buffer_pdf, nome_arquivo, legenda, numero)

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

    try:
        resp = requests.post(url, data=data, timeout=(10, 180))
        print("📨 Enviado via WhatsApp:", resp.status_code, resp.text)
    except requests.exceptions.ReadTimeout:
        print("⚠️ Timeout no envio. Tentando novamente em 10 s...")
        time.sleep(10)
        try:
            resp = requests.post(url, data=data, timeout=(10, 180))
            print("📨 Reenvio concluído:", resp.status_code, resp.text)
        except Exception as e2:
            print("❌ Falha também na segunda tentativa:", e2)
    except Exception as e:
        print("❌ Erro ao enviar via WhatsApp:", e)



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

TOPICOS_PERMITIDOS = {
    f"temperaturas/{c['obra']}/{s['nome']}"
    for c in CLIENTES for s in c.get("unidades", [])
}

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
            leituras_obra[obra][silo] = {
                "temperaturas": [int(t) for t in temperaturas],
                "ts": ts
            }
            ultima_leitura[obra] = time.time()

    except Exception as e:
        print("❌ Erro ao processar mensagem MQTT:", e)


client.on_connect = on_connect
client.on_message = on_message
if MQTT_USER and MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)


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
        client.publish(topico_comando, "iniciar_leitura")
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
    print(f"🔗 Conectando ao broker {MQTT_HOST}:{MQTT_PORT} ...")
    client.connect(MQTT_HOST, MQTT_PORT, 60)

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
