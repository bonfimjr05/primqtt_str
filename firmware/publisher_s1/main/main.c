/*
 * PrioMQTT — Publicador ESP32 com ciclo de RTT
 * Etapa 2: Baseline TCP com Mosquitto
 * Disciplina: Sistemas de Tempo Real — UFBA
 *
 * Fluxo:
 *   1. Conecta no Wi-Fi
 *   2. Conecta no Mosquitto via TCP
 *   3. Subscreve cmd/s1 para receber echo reply do controlador
 *   4. Publica em sensor/s1 a cada 1 segundo
 *   5. Ao receber echo, calcula RTT e loga se houve deadline miss
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "mqtt_client.h"
#include "esp_timer.h"

/* -------------------------------------------------------
 * Configurações — ajuste para sua rede e PC
 * ------------------------------------------------------- */
#define WIFI_SSID        "teste"              ///< SSID
#define WIFI_PASSWORD    "12345678"           ///< Senha
#define BROKER_IP        "192.168.1.8"
#define BROKER_PORT      1883
#define TOPIC_PUB        "sensor/s1"
#define TOPIC_CMD        "cmd/s1"
#define NODE_ID          "s1"
#define PUB_PERIOD_MS    1000
#define DEADLINE_US      50000   /* 50 ms em microssegundos */

static const char *TAG = "PRIOMQTT";

/* -------------------------------------------------------
 * Event group para sincronização Wi-Fi e MQTT
 * ------------------------------------------------------- */
static EventGroupHandle_t s_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define MQTT_CONNECTED_BIT  BIT1

static esp_mqtt_client_handle_t mqtt_client = NULL;

/*
 * Timestamp do último publish em microssegundos.
 * Gravado imediatamente antes de chamar esp_mqtt_client_publish
 * para minimizar o erro de medição do RTT.
 * Acesso protegido por ser escrito só na pub_task e lido
 * só no handler de MQTT_EVENT_DATA, que roda na mesma task
 * interna do cliente MQTT.
 */
static volatile int64_t ts_ultimo_pub = 0;

/* Contadores para resumo final */
static uint32_t total_pub   = 0;
static uint32_t total_miss  = 0;
static int64_t  rtt_soma_us = 0;
static int64_t  rtt_max_us  = 0;

/* -------------------------------------------------------
 * Handlers de eventos Wi-Fi
 * ------------------------------------------------------- */
static void wifi_event_handler(void *arg, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    if (base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();

    } else if (base == WIFI_EVENT &&
               event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi desconectado. Reconectando...");
        xEventGroupClearBits(s_event_group, WIFI_CONNECTED_BIT);
        esp_wifi_connect();

    } else if (base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *ev = (ip_event_got_ip_t *) event_data;
        ESP_LOGI(TAG, "IP obtido: " IPSTR, IP2STR(&ev->ip_info.ip));
        xEventGroupSetBits(s_event_group, WIFI_CONNECTED_BIT);
    }
}

/* -------------------------------------------------------
 * Inicialização do Wi-Fi em modo estação (STA)
 * ------------------------------------------------------- */
static void wifi_init(void)
{
    s_event_group = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

    wifi_config_t wifi_config = {
        .sta = {
            .ssid     = WIFI_SSID,
            .password = WIFI_PASSWORD,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "Aguardando conexao Wi-Fi...");
    xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                        pdFALSE, pdTRUE, portMAX_DELAY);
    ESP_LOGI(TAG, "Wi-Fi conectado.");
}

/* -------------------------------------------------------
 * Handler de eventos MQTT
 * ------------------------------------------------------- */
static void mqtt_event_handler(void *arg, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event =
        (esp_mqtt_event_handle_t) event_data;

    switch ((esp_mqtt_event_id_t) event_id) {

    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "Conectado ao broker MQTT.");
        /* subscreve o topico de comando para receber echo reply */
        esp_mqtt_client_subscribe(mqtt_client, TOPIC_CMD, 0);
        ESP_LOGI(TAG, "Subscrito em %s", TOPIC_CMD);
        xEventGroupSetBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "Desconectado do broker MQTT.");
        xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DATA:
        /*
         * Echo reply recebido do controlador.
         *
         * TD = esp_timer_get_time() agora
         * TA = ts_ultimo_pub (gravado antes do publish)
         *
         * RTT bruto = TD - TA
         *
         * Nota: esse RTT inclui o tempo de processamento do
         * controlador (TC - TB). Para descontar precisariamos
         * dos timestamps do controlador no payload, o que sera
         * implementado nas etapas seguintes com o broker Python.
         * Por ora o RTT bruto e suficiente para validar o ciclo.
         */
        {
            int64_t td     = esp_timer_get_time();
            int64_t rtt_us = td - ts_ultimo_pub;

            total_pub++;
            rtt_soma_us += rtt_us;
            if (rtt_us > rtt_max_us) rtt_max_us = rtt_us;

            char topico[64] = {0};
            int  tlen = event->topic_len < 63 ? event->topic_len : 63;
            memcpy(topico, event->topic, tlen);

            if (rtt_us > DEADLINE_US) {
                total_miss++;
                ESP_LOGW(TAG,
                    "MISS | topic=%s rtt=%lld us (%.1f ms) "
                    "misses=%lu/%lu",
                    topico,
                    (long long) rtt_us,
                    rtt_us / 1000.0f,
                    (unsigned long) total_miss,
                    (unsigned long) total_pub);
            } else {
                ESP_LOGI(TAG,
                    "RTT  | topic=%s rtt=%lld us (%.1f ms)",
                    topico,
                    (long long) rtt_us,
                    rtt_us / 1000.0f);
            }
        }
        break;

    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "Erro MQTT.");
        break;

    default:
        break;
    }
}

/* -------------------------------------------------------
 * Inicializacao do cliente MQTT
 * ------------------------------------------------------- */
static void mqtt_init(void)
{
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.hostname  = BROKER_IP,
        .broker.address.port      = BROKER_PORT,
        .broker.address.transport = MQTT_TRANSPORT_OVER_TCP,
    };

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(
        mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);

    ESP_LOGI(TAG, "Aguardando conexao MQTT...");
    xEventGroupWaitBits(s_event_group, MQTT_CONNECTED_BIT,
                        pdFALSE, pdTRUE, portMAX_DELAY);
}

/* -------------------------------------------------------
 * Tarefa de publicacao periodica
 *
 * Prioridade 5: acima das tarefas de sistema do Wi-Fi
 * (prio 3) e do cliente MQTT (prio 5 interna), mas
 * ajustavel conforme o experimento evoluir.
 *
 * Payload JSON:
 *   node : identificador deste ESP32
 *   seq  : numero de sequencia (detecta perdas)
 *   ts   : timestamp em us desde o boot (esp_timer_get_time)
 *   p    : prioridade (preparado para o PrioMQTT — por ora 1)
 * ------------------------------------------------------- */
static void pub_task(void *pvParameters)
{
    char     payload[192];
    uint32_t seq = 0;

    while (1) {
        xEventGroupWaitBits(s_event_group, MQTT_CONNECTED_BIT,
                            pdFALSE, pdTRUE, portMAX_DELAY);

        /*
         * Grava TA imediatamente antes do publish.
         * Qualquer codigo entre essa linha e o publish
         * adiciona erro ao RTT medido.
         */
        ts_ultimo_pub = esp_timer_get_time();

        snprintf(payload, sizeof(payload),
                 "{"
                 "\"node\":\"%s\","
                 "\"seq\":%lu,"
                 "\"ts\":%lld,"
                 "\"p\":1"
                 "}",
                 NODE_ID,
                 (unsigned long) seq,
                 (long long) ts_ultimo_pub);

        int msg_id = esp_mqtt_client_publish(
            mqtt_client,
            TOPIC_PUB,
            payload,
            0,   /* len=0: usa strlen automaticamente */
            0,   /* QoS 0: sem confirmacao, menor latencia */
            0    /* retain=0 */
        );

        if (msg_id >= 0) {
            ESP_LOGI(TAG, "PUB | seq=%lu ts=%lld us",
                     (unsigned long) seq,
                     (long long) ts_ultimo_pub);
        } else {
            ESP_LOGE(TAG, "Falha ao publicar seq=%lu",
                     (unsigned long) seq);
        }

        seq++;
        vTaskDelay(pdMS_TO_TICKS(PUB_PERIOD_MS));
    }
}

/* -------------------------------------------------------
 * Tarefa de estatisticas periodicas
 *
 * Imprime resumo a cada 10 segundos para acompanhar
 * o experimento sem precisar contar mensagens manualmente.
 * ------------------------------------------------------- */
static void stats_task(void *pvParameters)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));
        if (total_pub > 0) {
            float avg_ms  = (rtt_soma_us / total_pub) / 1000.0f;
            float max_ms  = rtt_max_us / 1000.0f;
            float miss_pct = (total_miss * 100.0f) / total_pub;
            ESP_LOGI(TAG,
                "--- STATS | pub=%lu miss=%lu (%.1f%%) "
                "rtt_avg=%.2fms rtt_max=%.2fms ---",
                (unsigned long) total_pub,
                (unsigned long) total_miss,
                miss_pct,
                avg_ms,
                max_ms);
        }
    }
}

/* -------------------------------------------------------
 * Ponto de entrada
 * ------------------------------------------------------- */
void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_LOGI(TAG, "=== PrioMQTT Publisher | node=%s ===", NODE_ID);
    ESP_LOGI(TAG, "Broker: %s:%d", BROKER_IP, BROKER_PORT);
    ESP_LOGI(TAG, "Topico pub: %s | cmd: %s", TOPIC_PUB, TOPIC_CMD);
    ESP_LOGI(TAG, "Deadline: %d us (%d ms)",
             DEADLINE_US, DEADLINE_US / 1000);

    wifi_init();
    mqtt_init();

    xTaskCreate(pub_task,   "pub_task",   4096, NULL, 5, NULL);
    xTaskCreate(stats_task, "stats_task", 2048, NULL, 2, NULL);
}