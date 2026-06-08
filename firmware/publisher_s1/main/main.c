/*
 * Etapa 2 — Publicador MQTT via Wi-Fi e TCP
 * Disciplina: Sistemas de Tempo Real — UFBA
 *
 * O ESP32 conecta no Wi-Fi, conecta no broker Mosquitto
 * via TCP e publica uma mensagem por segundo no tópico
 * sensor/s1 com um timestamp e número de sequência.
 *
 * Isso replica o comportamento do publisher Python da
 * Etapa 1, agora em hardware real.
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
#define WIFI_SSID      "teste"           // nome da sua rede Wi-Fi
#define WIFI_PASSWORD  "12345678"          // senha da rede
#define BROKER_IP      "192.168.1.8"        // IP do seu PC
#define BROKER_PORT    1883
#define TOPIC_PUB      "sensor/s2"          // tópico de publicação
#define NODE_ID        "s2"                 // identificador deste ESP32
#define PUB_PERIOD_MS  1000                 // publica a cada 1 segundo

static const char *TAG = "PRIOMQTT";

/* -------------------------------------------------------
 * Event group para sincronização Wi-Fi e MQTT
 * ------------------------------------------------------- */
static EventGroupHandle_t s_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define MQTT_CONNECTED_BIT  BIT1

static esp_mqtt_client_handle_t mqtt_client = NULL;

/* -------------------------------------------------------
 * Handlers de eventos Wi-Fi
 * ------------------------------------------------------- */
static void wifi_event_handler(void *arg, esp_event_base_t base,
                               int32_t event_id, void *event_data)
{
    if (base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();

    } else if (base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi desconectado. Reconectando...");
        xEventGroupClearBits(s_event_group, WIFI_CONNECTED_BIT);
        esp_wifi_connect();

    } else if (base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *) event_data;
        ESP_LOGI(TAG, "IP obtido: " IPSTR, IP2STR(&event->ip_info.ip));
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

    // registra handlers para eventos Wi-Fi e IP
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

    ESP_LOGI(TAG, "Aguardando conexão Wi-Fi...");
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
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t) event_data;

    switch ((esp_mqtt_event_id_t) event_id) {

    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "Conectado ao broker MQTT.");
        xEventGroupSetBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "Desconectado do broker MQTT.");
        xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_PUBLISHED:
        // confirmação de que o broker recebeu (relevante para QoS > 0)
        ESP_LOGD(TAG, "Mensagem publicada, msg_id=%d", event->msg_id);
        break;

    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "Erro MQTT.");
        break;

    default:
        break;
    }
}

/* -------------------------------------------------------
 * Inicialização do cliente MQTT
 * ------------------------------------------------------- */
static void mqtt_init(void)
{
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.hostname  = BROKER_IP,
        .broker.address.port      = BROKER_PORT,
        .broker.address.transport = MQTT_TRANSPORT_OVER_TCP,
    };

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID,
                                   mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);

    ESP_LOGI(TAG, "Aguardando conexão MQTT...");
    xEventGroupWaitBits(s_event_group, MQTT_CONNECTED_BIT,
                        pdFALSE, pdTRUE, portMAX_DELAY);
}

/* -------------------------------------------------------
 * Tarefa principal de publicação
 *
 * FreeRTOS prio 5 — publicação periódica de 1 msg/s
 * O payload inclui:
 *   node : identificador do ESP32
 *   seq  : número de sequência (detecta perdas)
 *   ts   : timestamp em microssegundos (esp_timer_get_time)
 *          usado para calcular RTT no assinante
 * ------------------------------------------------------- */
static void pub_task(void *pvParameters)
{
    char payload[128];
    uint32_t seq = 0;

    while (1) {
        // aguarda MQTT estar conectado antes de publicar
        xEventGroupWaitBits(s_event_group, MQTT_CONNECTED_BIT,
                            pdFALSE, pdTRUE, portMAX_DELAY);

        int64_t ts = esp_timer_get_time(); // microssegundos desde o boot

        snprintf(payload, sizeof(payload),
                 "{\"node\":\"%s\",\"seq\":%lu,\"ts\":%lld}",
                 NODE_ID, (unsigned long) seq, ts);

        int msg_id = esp_mqtt_client_publish(
            mqtt_client,
            TOPIC_PUB,
            payload,
            0,      // len=0: calcula automaticamente pelo strlen
            0,      // QoS 0: sem confirmação (menor latência)
            0       // retain=0
        );

        if (msg_id >= 0) {
            ESP_LOGI(TAG, "PUB seq=%lu ts=%lld us", (unsigned long) seq, ts);
        } else {
            ESP_LOGE(TAG, "Falha ao publicar seq=%lu", (unsigned long) seq);
        }

        seq++;
        vTaskDelay(pdMS_TO_TICKS(PUB_PERIOD_MS));
    }
}

/* -------------------------------------------------------
 * Ponto de entrada
 * ------------------------------------------------------- */
void app_main(void)
{
    // NVS é necessário para o Wi-Fi armazenar configurações
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_LOGI(TAG, "=== PrioMQTT Publisher — Etapa 2 ===");

    wifi_init();
    mqtt_init();

    // cria a tarefa de publicação com prioridade 5
    // (acima das tarefas de sistema, abaixo de tarefas críticas)
    xTaskCreate(pub_task, "pub_task", 4096, NULL, 5, NULL);

    // app_main pode retornar — as tarefas FreeRTOS continuam rodando
}