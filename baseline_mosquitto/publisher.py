"""
Publicador MQTT com Mosquitto
Disciplina: Sistemas de Tempo Real — UFBA

Publica mensagens periódicas no tópico sensor/s1
e mede o tempo entre publicação e confirmação de entrega.
"""

import paho.mqtt.client as mqtt
import time
import json

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC       = "sensor/s1"
INTERVALO_S = 1.0        # publica a cada 1 segundo
N_MENSAGENS = 10

# callback chamado quando a conexão com o broker é estabelecida
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[OK] Conectado ao broker {BROKER_HOST}:{BROKER_PORT}")
    else:
        print(f"[ERRO] Falha na conexão, código: {rc}")

# callback chamado quando o broker confirma o recebimento da mensagem
def on_publish(client, userdata, mid, reason_code=None, properties=None):
    ts_confirmacao = time.monotonic_ns()
    ts_envio = userdata.get(mid)
    if ts_envio:
        latencia_ms = (ts_confirmacao - ts_envio) / 1_000_000
        print(f"  [ACK] msg_id={mid} latência_ack={latencia_ms:.3f} ms")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.user_data_set({})   # dicionário para guardar timestamps por msg_id
client.on_connect = on_connect
client.on_publish = on_publish

client.connect(BROKER_HOST, BROKER_PORT)
client.loop_start()

print(f"Publicando {N_MENSAGENS} mensagens em '{TOPIC}'...\n")

for i in range(N_MENSAGENS):
    payload = json.dumps({
        "seq" : i,
        "node": "s1",
        "dado": f"temp={20 + i * 0.1:.1f}",
        "ts"  : time.monotonic_ns(),
    })

    ts_antes = time.monotonic_ns()
    info = client.publish(TOPIC, payload, qos=0)

    # guarda o timestamp indexado pelo msg_id para o on_publish calcular
    client.user_data_set({**client._userdata, info.mid: ts_antes})

    print(f"[PUB] seq={i} payload={payload}")
    time.sleep(INTERVALO_S)

client.loop_stop()
client.disconnect()
print("\nPublicador encerrado.")