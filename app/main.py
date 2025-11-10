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


def cor_por_valor(v_bruto: int):
    """
    v_bruto: valor recebido do MQTT (0–99).

    Mapeamento:
      0–60   -> temperatura 0–60°C
      85–89  -> -5 a -1°C (faixa negativa, mesma cor da parte fria)
      92–99  -> erro (cinza)
      resto  -> clamped para [-5, 60] e entra no gradiente

    Gradiente (no espaço da temperatura real):
      -5  -> Azul (frio extremo)
       0  -> Verde (bom)
      20  -> Amarelo (alerta)
      40  -> Vermelho (crítico)
      60  -> Cinza (muito quente)
    """

    # 1) Erros (92–99) -> cinza escuro fixo
    if 92 <= v_bruto <= 99:
        return colors.HexColor("#A9A9A9")

    # 2) Converte para temperatura física
    if 85 <= v_bruto <= 89:
        temp = v_bruto - 90   # 85→-5 ... 89→-1
    else:
        temp = v_bruto        # 0–60 já é a própria temperatura

    # 3) Limita para faixa [-5, 60]
    if temp < -5:
        temp = -5
    if temp > 60:
        temp = 60

    # 4) Gradiente por pontos de controle
    pontos = [
        (-5, "#00E5FF"),  # Azul
        (0,  "#00FF00"),  # Verde
        (20, "#FFFF00"),  # Amarelo
        (40, "#FF4500"),  # Vermelho
        (60, "#A0A0A0"),  # Cinza quente
    ]

    def hex_to_rgb(hex_color: str):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    for i in range(len(pontos) - 1):
        v1, c1 = pontos[i]
        v2, c2 = pontos[i + 1]
        if v1 <= temp <= v2:
            t = (temp - v1) / (v2 - v1)
            r1, g1, b1 = hex_to_rgb(c1)
            r2, g2, b2 = hex_to_rgb(c2)
            r = int(r1 + (r2 - r1) * t)
            g = int(g1 + (g2 - g1) * t)
            b = int(b1 + (b2 - b1) * t)
            return colors.Color(r / 255, g / 255, b / 255)

    # fallback (não deve cair aqui)
    return colors.HexColor("#C0C0C0")



def gerar_relatorio_silo(c, descricao, config, temperaturas, arcos=None):
    """
    Gera o relatório térmico de um silo, em modo paisagem,
    com até 2 linhas de cabos por página, e célula dimensionada de forma dinâmica.
    """
    # === Construção da matriz de colunas ===
    colunas = []
    idx = 0
    for nsens in config:
        col = []
        for _ in range(nsens):
            if idx >= len(temperaturas):
                break
            col.append(int(temperaturas[idx]))
            idx += 1
        colunas.append(col)

    total_cabos = len(colunas)
    if total_cabos == 0:
        return
    max_sensores = max((len(c) for c in colunas), default=0)
    if max_sensores == 0:
        return

    largura, altura = landscape(A4)

    # === Regras de layout ===
    MAX_CABOS_POR_LINHA = 36
    MAX_LINHAS_POR_PAGINA = 2
    CABOS_POR_PAGINA = MAX_CABOS_POR_LINHA * MAX_LINHAS_POR_PAGINA
    MARGEM_X, MARGEM_TOPO, MARGEM_RODAPE = 80, 110, 70
    GAP_ENTRE_LINHAS = 40

    primeira = True
    for inicio_pag in range(0, total_cabos, CABOS_POR_PAGINA):
        if not primeira:
            c.showPage()
        primeira = False

        c.setPageSize(landscape(A4))
        c.setFillColor(colors.white)
        c.rect(0, 0, largura, altura, fill=1, stroke=0)

        # === Cabeçalho moderno ===
        CABECALHO_ALTURA = 60
        MARGEM_INFERIOR_CABECALHO = 20
        cor_fundo_cab = colors.HexColor("#F0F4F8")
        c.setFillColor(cor_fundo_cab)
        c.rect(0, altura - CABECALHO_ALTURA, largura, CABECALHO_ALTURA, fill=1, stroke=0)

        # Logo alinhada à esquerda, centrada verticalmente no cabeçalho
        logo_path = os.path.join(BASE_DIR, "assets", "logo eletromaass.png")
        if os.path.exists(logo_path):
            logo_alt, logo_larg = 40, 80
            y_logo = altura - (CABECALHO_ALTURA / 2) - (logo_alt / 2)
            c.drawImage(
                logo_path, 40, y_logo,
                width=logo_larg, height=logo_alt,
                preserveAspectRatio=True, mask='auto'
            )

        # Texto centralizado (título e data)
        centro_x = largura / 2
        centro_y_cab = altura - (CABECALHO_ALTURA / 2)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(centro_x, centro_y_cab + 6, f"Relatório Térmico - {descricao}")
        c.setFont("Helvetica", 11)
        c.drawCentredString(centro_x, centro_y_cab - 12, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

        # === Cabos da página ===
        fim_pag = min(inicio_pag + CABOS_POR_PAGINA, total_cabos)
        indices_pag = list(range(inicio_pag, fim_pag))
        indices_linha1 = indices_pag[:MAX_CABOS_POR_LINHA]
        indices_linha2 = indices_pag[MAX_CABOS_POR_LINHA:]
        linhas_indices = [indices_linha1] + ([indices_linha2] if indices_linha2 else [])
        num_linhas = len(linhas_indices)
        max_cabos_linha = max(len(l) for l in linhas_indices)

        altura_disp = altura - MARGEM_TOPO - MARGEM_RODAPE
        largura_disp = largura - 2 * MARGEM_X
        tam_h = (altura_disp - (num_linhas - 1) * GAP_ENTRE_LINHAS) / (max_sensores * num_linhas)
        tam_w = largura_disp / max_cabos_linha
        tam = min(tam_h, tam_w, 25)
        tam = max(tam, 8)

        altura_total = max_sensores * tam * num_linhas + (num_linhas - 1) * GAP_ENTRE_LINHAS
        inicio_y_global = ((altura - altura_total) / 2) - MARGEM_INFERIOR_CABECALHO

        # --- Divisão por arcos (calculada 1x por página) ---
        arcos_indices = []
        if arcos:
            inicio_arco = 0
            for qtd in arcos:
                fim_arco = inicio_arco + qtd
                arcos_indices.append(list(range(inicio_arco, fim_arco)))
                inicio_arco = fim_arco

        # === Desenho ===
        c.setLineWidth(0.5)
        for idx_linha, indices_cabos in enumerate(reversed(linhas_indices)):
            y_base = inicio_y_global + idx_linha * (max_sensores * tam + GAP_ENTRE_LINHAS)
            num_cabos = len(indices_cabos)
            largura_mat = num_cabos * tam
            x_inicio = (largura - largura_mat) / 2

            c.setFont("Helvetica-Bold", 6)
            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                col = colunas[idx_cabo]
                x_cabo = x_inicio + pos_cabo * tam
                for linha_sensor, v in enumerate(col):
                    y_cel = y_base + linha_sensor * tam
                    cor = cor_por_valor(v)
                    texto = texto_por_valor(v)
                    c.setFillColor(cor)
                    c.rect(x_cabo, y_cel, tam, tam, fill=1, stroke=0)
                    c.setFillColor(colors.black)
                    c.drawCentredString(x_cabo + tam / 2, y_cel + tam / 2 - 2, texto)

            # Sensores à esquerda
            c.setFont("Helvetica-Bold", 7)
            for i in range(max_sensores):
                y_label = y_base + i * tam + tam / 2 - 3
                c.drawRightString(x_inicio - 8, y_label, f"S{i+1:02}")

            # Cabos abaixo
            c.setFont("Helvetica-Bold", max(4, min(8, tam * 0.35)))
            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                x_label = x_inicio + pos_cabo * tam + tam / 2
                y_label = y_base + max_sensores * tam + (tam * 0.3)
                c.drawCentredString(x_label, y_label, f"C{idx_cabo + 1:02}")

            # Etiquetas de arcos nesta linha
            if arcos_indices:
                c.setFont("Helvetica-Bold", 8)
                for num_arco, grupo in enumerate(arcos_indices, start=1):
                    cabos_linha = [idx for idx in indices_cabos if idx in grupo]
                    if not cabos_linha:
                        continue
                    primeiro_idx = cabos_linha[0]
                    ultimo_idx = cabos_linha[-1]
                    pos_p = indices_cabos.index(primeiro_idx)
                    pos_u = indices_cabos.index(ultimo_idx)
                    x_p = x_inicio + pos_p * tam
                    x_u = x_inicio + (pos_u + 1) * tam
                    x_centro = (x_p + x_u) / 2
                    y_arco = y_base + max_sensores * tam + (tam * 1.3)
                    c.drawCentredString(x_centro, y_arco, f"A{num_arco:02}")

        # Linha divisória se houver duas fileiras
        if num_linhas == 2:
            y_meio = inicio_y_global + max_sensores * tam + GAP_ENTRE_LINHAS / 2
            c.setStrokeColor(colors.lightgrey)
            c.line(MARGEM_X / 2, y_meio, largura - MARGEM_X / 2, y_meio)

        # === Legenda de gradiente ===
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(colors.black)
        c.drawCentredString(largura / 2, 50, "Escala de Temperatura")
        barra_larg, barra_alt = 400, 14
        x_ini_barra = (largura - barra_larg) / 2
        y_barra = 30
        num_passos = 100
        for i in range(num_passos):
            frac = i / (num_passos - 1)
            v = -5 + frac * 65
            cor = cor_por_valor(v)
            x = x_ini_barra + i * (barra_larg / num_passos)
            c.setFillColor(cor)
            c.rect(x, y_barra, barra_larg / num_passos, barra_alt, fill=1, stroke=0)

        c.setFillColor(colors.black)
        c.setFont("Helvetica", 7)
        c.drawString(x_ini_barra - 25, y_barra + 3, "Frio")
        c.drawCentredString(largura / 2, y_barra + 3, "Normal")
        c.drawRightString(x_ini_barra + barra_larg + 25, y_barra + 3, "Crítico")

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
        arcos = silo.get("arcos", None)
        gerar_relatorio_silo(c, descricao, config, temperaturas, arcos)

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
