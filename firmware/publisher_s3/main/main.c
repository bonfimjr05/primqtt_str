/*
 * PrioMQTT: No S3 (gerador de carga de fundo)
 * Etapa 3: Multiplos ESP32 no Modo 1 (TCP)
 * Disciplina: Sistemas de Tempo Real — UFBA
 *
 * Diferenca em relacao ao firmware de S1/S2:
 *   - Periodo de publicacao configuravel via LOAD_RATE_HZ
 *   - Representa trafego de baixa prioridade / nao critico
 *   - Usado para estressar a fila do broker e observar o
 *     impacto sobre o RTT de S1 e S2
 * LOAD_RATE_HZ para 5, 10 e 20.
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
 * Configuracoes 
 * ------------------------------------------------------- */
#define WIFI_SSID        ".:Bahiatelecom:. Ray"              ///< SSID
#define WIFI_PASSWORD    "BF81505179"           ///< Senha
#define BROKER_IP        "192.168.1.24"//"10.141.135.69" ///IP DO PC
#define BROKER_PORT      1883
#define TOPIC_PUB        "sensor/s3"
#define TOPIC_CMD        "cmd/s3"
#define NODE_ID          "s3"

/*
 * Taxa de carga de fundo em Hz.
 * Altere este valor para 5, 10 ou 20 entre as execucoes
 * do experimento, conforme a condicao de carga avaliada.
 */
#define LOAD_RATE_HZ     5
#define PUB_PERIOD_MS    (1000 / LOAD_RATE_HZ)

/*
 * S3 e trafego de baixa prioridade: deadline mais alto 200 ms 
 */
#define DEADLINE_US      200000

static const char *TAG = "PRIOMQTT_S3";

static EventGroupHandle_t s_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define MQTT_CONNECTED_BIT  BIT1

static esp_mqtt_client_handle_t mqtt_client = NULL;
static volatile int64_t ts_ultimo_pub = 0;

static uint32_t total_pub   = 0;
static uint32_t total_miss  = 0;
static int64_t  rtt_soma_us = 0;
static int64_t  rtt_max_us  = 0;

/* -------------------------------------------------------
 * Wi-Fi
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
 * MQTT
 * ------------------------------------------------------- */
static void mqtt_event_handler(void *arg, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event =
        (esp_mqtt_event_handle_t) event_data;

    switch ((esp_mqtt_event_id_t) event_id) {

    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "Conectado ao broker MQTT.");
        esp_mqtt_client_subscribe(mqtt_client, TOPIC_CMD, 0);
        ESP_LOGI(TAG, "Subscrito em %s", TOPIC_CMD);
        xEventGroupSetBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "Desconectado do broker MQTT.");
        xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DATA:
        {
            int64_t td     = esp_timer_get_time();
            int64_t rtt_us = td - ts_ultimo_pub;

            total_pub++;
            rtt_soma_us += rtt_us;
            if (rtt_us > rtt_max_us) rtt_max_us = rtt_us;

            if (rtt_us > DEADLINE_US) {
                total_miss++;
            }

            ESP_LOGD(TAG, "RTT = %lld us", (long long) rtt_us);
        }
        break;

    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "Erro MQTT.");
        break;

    default:
        break;
    }
}

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
 * Tarefa de publicacao, carga de fundo
 *
 * Prioridade FreeRTOS 3 (mais baixa que S1/S2, que usam 5).
 * ------------------------------------------------------- */
static void pub_task(void *pvParameters)
{
    char     payload[192];
    uint32_t seq = 0;

    ESP_LOGI(TAG, "Taxa de carga configurada: %d Hz (periodo %d ms)",
             LOAD_RATE_HZ, PUB_PERIOD_MS);

    while (1) {
        xEventGroupWaitBits(s_event_group, MQTT_CONNECTED_BIT,
                            pdFALSE, pdTRUE, portMAX_DELAY);

        ts_ultimo_pub = esp_timer_get_time();

        snprintf(payload, sizeof(payload),
                 "{"
                 "\"node\":\"%s\","
                 "\"seq\":%lu,"
                 "\"ts\":%lld,"
                 "\"p\":5"
                 "}",
                 NODE_ID,
                 (unsigned long) seq,
                 (long long) ts_ultimo_pub);

        int msg_id = esp_mqtt_client_publish(
            mqtt_client, TOPIC_PUB, payload, 0, 0, 0);

        if (msg_id < 0) {
            ESP_LOGE(TAG, "Falha ao publicar seq=%lu",
                     (unsigned long) seq);
        }

        
        if (seq % 50 == 0) {
            ESP_LOGI(TAG, "PUB | seq=%lu (carga %d Hz)",
                     (unsigned long) seq, LOAD_RATE_HZ);
        }

        seq++;
        vTaskDelay(pdMS_TO_TICKS(PUB_PERIOD_MS));
    }
}

static void stats_task(void *pvParameters)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));
        if (total_pub > 0) {
            float avg_ms   = (rtt_soma_us / total_pub) / 1000.0f;
            float max_ms   = rtt_max_us / 1000.0f;
            float miss_pct = (total_miss * 100.0f) / total_pub;
            ESP_LOGI(TAG,
                "--- STATS S3 | pub=%lu miss=%lu (%.1f%%) "
                "rtt_avg=%.2fms rtt_max=%.2fms taxa=%dHz ---",
                (unsigned long) total_pub,
                (unsigned long) total_miss,
                miss_pct, avg_ms, max_ms, LOAD_RATE_HZ);
        }
    }
}

void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_LOGI(TAG, "=== PrioMQTT S3 (carga de fundo) ===");
    ESP_LOGI(TAG, "Broker: %s:%d", BROKER_IP, BROKER_PORT);
    ESP_LOGI(TAG, "Taxa: %d Hz", LOAD_RATE_HZ);

    wifi_init();
    mqtt_init();

    /* Prioridade FreeRTOS 3 — menor que S1/S2 (prio 5) */
    xTaskCreate(pub_task,   "pub_task_s3",   4096, NULL, 3, NULL);
    xTaskCreate(stats_task, "stats_task_s3", 2048, NULL, 1, NULL);
}