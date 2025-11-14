# ============================================================
# mqtt_manager.py – Gerenciamento da conexão MQTT
# ============================================================

import json
import time
import paho.mqtt.client as mqtt


# Variáveis internas do módulo
_client = None
TOPICOS_PERMITIDOS = set()
CALLBACK_PROCESSAR_LEITURA = None
CALLBACK_LOG = print

MQTT_HOST = None
MQTT_PORT = None
MQTT_USER = None
MQTT_PASS = None


# ============================================================
# Configuração inicial vinda do main
# ============================================================

def configurar_mqtt(host, port, user, password, topicos, callback_leitura, log_callback=print):
    """
    host, port, user, pass → credenciais do broker
    topicos → lista/set de strings
    callback_leitura(obra, silo, temperaturas, ts) → função externa para repassar mensagens
    log_callback → função para logs (padrão: print)
    """
    global MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS
    global TOPICOS_PERMITIDOS, CALLBACK_PROCESSAR_LEITURA, CALLBACK_LOG

    MQTT_HOST = host
    MQTT_PORT = port
    MQTT_USER = user
    MQTT_PASS = password

    TOPICOS_PERMITIDOS = set(topicos)
    CALLBACK_PROCESSAR_LEITURA = callback_leitura
    CALLBACK_LOG = log_callback


# ============================================================
# Callbacks MQTT
# ============================================================

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        CALLBACK_LOG("✅ Conectado ao broker MQTT.")
        for topico in TOPICOS_PERMITIDOS:
            client.subscribe(topico)
            CALLBACK_LOG(f"📡 Assinado: {topico}")
    else:
        CALLBACK_LOG(f"❌ Falha na conexão (rc={rc})")


def on_message(client, userdata, msg):
    try:
        topico = msg.topic.strip()

        if topico not in TOPICOS_PERMITIDOS:
            CALLBACK_LOG(f"⚠️ Tópico ignorado: {topico}")
            return

        # Decodificar JSON
        dados = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        temperaturas = dados.get("d", {}).get("temperature", [])
        ts = dados.get("ts", "")

        if not isinstance(temperaturas, list) or not temperaturas:
            CALLBACK_LOG(f"⚠️ Sem temperaturas no payload: {dados}")
            return

        # Extrair obra e silo do tópico: temperaturas/{obra}/{silo}
        partes = topico.split("/")
        if len(partes) < 3:
            CALLBACK_LOG(f"⚠️ Tópico inválido: {topico}")
            return

        _, obra, silo = partes[0], partes[1], partes[2]

        # Log simples
        CALLBACK_LOG(f"📥 MQTT {obra}/{silo}: {len(temperaturas)} temperaturas")

        # Enviar leitura para o main
        if CALLBACK_PROCESSAR_LEITURA:
            CALLBACK_PROCESSAR_LEITURA(
                obra=obra,
                silo=silo,
                temperaturas=[int(t) for t in temperaturas],
                ts=ts
            )

    except Exception as e:
        CALLBACK_LOG(f"❌ Erro no on_message: {e}")


# ============================================================
# Funções principais
# ============================================================

def iniciar_mqtt():
    """
    Cria e inicia o cliente MQTT configurado.
    Retorna o objeto client para o main controlar o loop.
    """
    global _client

    if MQTT_HOST is None:
        raise RuntimeError("⚠️ MQTT não configurado! Chame configurar_mqtt() antes.")

    _client = mqtt.Client()

    if MQTT_USER and MQTT_PASS:
        _client.username_pw_set(MQTT_USER, MQTT_PASS)

    _client.on_connect = on_connect
    _client.on_message = on_message

    CALLBACK_LOG(f"🔗 Conectando ao broker {MQTT_HOST}:{MQTT_PORT} ...")
    _client.connect(MQTT_HOST, MQTT_PORT, 60)

    return _client
