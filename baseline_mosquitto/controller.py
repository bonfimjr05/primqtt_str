"""
Controlador MQTT — Baseline TCP com Mosquitto
Disciplina: Sistemas de Tempo Real — UFBA

Subscreve os tópicos de dados de todos os módulos IO,
responde com echo reply no tópico de comando correspondente
e registra os timestamps para cálculo de RTT.

Tópicos esperados:
  sensor/s1, sensor/s2, sensor/s3  — dados dos módulos IO
  cmd/s1, cmd/s2, cmd/s3           — comandos para os módulos
"""

import paho.mqtt.client as mqtt
import json
import time
import csv
import os

BROKER_HOST  = "localhost"
BROKER_PORT  = 1883
NODES        = ["s1", "s2", "s3"]
DATA_TOPICS  = [f"sensor/{n}" for n in NODES]
CMD_TOPICS   = [f"cmd/{n}"    for n in NODES]
DEADLINE_MS  = 50.0   # deadline de 50 ms

# arquivo CSV para registrar as trocas
CSV_FILE = "rtt_baseline_tcp.csv"
csv_file  = open(CSV_FILE, "w", newline="")
csv_writer = csv.DictWriter(csv_file, fieldnames=[
    "wall_time", "node", "seq", "pub_ts_us",
    "tb_ns", "tc_ns", "rtt_ms", "deadline_miss"
])
csv_writer.writeheader()

contadores = {n: {"recebidas": 0, "misses": 0} for n in NODES}

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[OK] Controlador conectado ao broker.")
        for topic in DATA_TOPICS:
            client.subscribe(topic)
            print(f"     Subscrito em: {topic}")
    else:
        print(f"[ERRO] Código: {rc}")

def on_message(client, userdata, msg):
    tb_ns = time.monotonic_ns()   # TB: momento em que o controlador recebe

    try:
        payload = json.loads(msg.payload.decode())
    except json.JSONDecodeError:
        print(f"[ERRO] Payload inválido: {msg.payload}")
        return

    node   = payload.get("node", "??")
    seq    = payload.get("seq", -1)
    pub_ts = payload.get("ts", 0)   # TA em microssegundos (clock do ESP32)
    
    # monta o echo reply com os timestamps necessários para RTT
    tc_ns = time.monotonic_ns()     # TC: momento em que o controlador responde
    proc_us = (tc_ns - tb_ns) / 1000  # tempo de processamento em us

    echo  = json.dumps({
        "node"   : node,
        "seq"    : seq,
        "orig_ts": pub_ts,          # TA original do publicador
        "tb_ns"  : tb_ns,           # TB: recebimento no controlador
        "tc_ns"  : tc_ns,           # TC: envio do echo
    })

    cmd_topic = f"cmd/{node}"
    client.publish(cmd_topic, echo, qos=0)

    contadores[node]["recebidas"] += 1
    print(f"[CTRL] node={node} seq={seq} -> echo em {cmd_topic}")

    # registra no CSV
    csv_writer.writerow({
        "wall_time"    : round(time.time(), 6),
        "node"         : node,
        "seq"          : seq,
        "pub_ts_us"    : pub_ts,
        "tb_ns"        : tb_ns,
        "tc_ns"        : tc_ns,
        "rtt_ms"       : proc_us,        # calculado no publicador
        "deadline_miss": "",
    })
    csv_file.flush()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT)

print(f"Controlador iniciado. Deadline: {DEADLINE_MS} ms")
print(f"Métricas em: {CSV_FILE}")
print("Ctrl+C para encerrar.\n")

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\n--- Resumo ---")
    for node, c in contadores.items():
        print(f"  {node}: {c['recebidas']} mensagens recebidas")
    csv_file.close()
    print(f"CSV salvo: {CSV_FILE}")