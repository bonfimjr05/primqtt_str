"""
Assinante MQTT com Mosquitto
Disciplina: Sistemas de Tempo Real — UFBA

Subscreve o tópico sensor/s1 e imprime cada mensagem recebida
com o timestamp de recebimento.
"""

import paho.mqtt.client as mqtt
import time
import json

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPICO      = "sensor/s1"

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[OK] Conectado. Aguardando mensagens em '{TOPICO}'...")
        # subscrição feita dentro do on_connect garante que ela
        # é refeita automaticamente em caso de reconexão
        client.subscribe(TOPICO)
    else:
        print(f"[ERRO] Código de conexão: {rc}")

def on_message(client, userdata, msg):
    ts_recebido = time.monotonic_ns()
    try:
        payload = json.loads(msg.payload.decode())
        ts_pub  = payload.get("ts", 0)
        if ts_pub:
            # ts do ESP32 está em microssegundos, converte para ns para comparar
            latencia_ms = (ts_recebido - ts_pub * 1000) / 1_000_000
            print(f"[MSG] node={payload.get('node')} seq={payload.get('seq')} "
                  f"ts={ts_pub} us latência={latencia_ms:.3f} ms")
        else:
            print(f"[MSG] tópico={msg.topic} payload={msg.payload.decode()}")
    except json.JSONDecodeError:
        print(f"[MSG] tópico={msg.topic} payload={msg.payload.decode()}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT)

print(f"Conectando ao broker {BROKER_HOST}:{BROKER_PORT}...")

# loop_forever mantém a conexão e processa eventos indefinidamente
# Ctrl+C para encerrar
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\nAssinante encerrado.")