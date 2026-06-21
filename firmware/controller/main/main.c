/*
 * PrioMQTT: No Controlador (ESP32 dedicado)
 * Etapa 3: Multiplos ESP32 no Modo 1 (TCP)
 * Disciplina: Sistemas de Tempo Real — UFBA
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

/* Topicos de dados aos quais o controlador se subscreve.
 * Para adicionar um quarto modulo IO (S4), basta incluir
 * "sensor/s4" aqui e aumentar N_TOPICS_DATA. */
static const char *TOPICS_DATA[] = {
    "sensor/s1",
    "sensor/s2",
    "sensor/s3",
};
#define N_TOPICS_DATA  3

static const char *TAG = "PRIOMQTT_CTRL";

static EventGroupHandle_t s_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define MQTT_CONNECTED_BIT  BIT1

static esp_mqtt_client_handle_t mqtt_client = NULL;

/* Contadores por no, apenas para o log periodico de estatisticas */
static uint32_t total_recebidas[N_TOPICS_DATA] = {0};

/* -------------------------------------------------------
 * Wi-Fi (identico aos demais firmwares do projeto)
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
 * Deriva o topico de comando a partir do topico de dados.
 *
 * Entrada : "sensor/s1"  (event->topic, sem terminador nulo
 *            garantido — por isso recebemos o tamanho)
 * Saida   : "cmd/s1"     (escrito em out_buf)
 *
 * A logica e puramente textual: localiza a barra '/' e
 * copia tudo que vem depois dela, prefixando com "cmd/".
 * Isso evita qualquer parsing de JSON so para extrair o
 * nome do no, o topico MQTT ja contem essa informacao.
 * ------------------------------------------------------- */
static bool derivar_topico_cmd(const char *topico_dados, int len,
                               char *out_buf, size_t out_size)
{
    /* localiza a posicao da barra dentro dos primeiros `len` bytes */
    int barra_pos = -1;
    for (int i = 0; i < len; i++) {
        if (topico_dados[i] == '/') {
            barra_pos = i;
            break;
        }
    }

    if (barra_pos < 0) {
        return false;  /* topico sem barra,formato inesperado */
    }

    const char *sufixo = topico_dados + barra_pos + 1;
    int sufixo_len = len - barra_pos - 1;

    /* monta "cmd/" + sufixo, respeitando o tamanho do buffer */
    int escrito = snprintf(out_buf, out_size, "cmd/%.*s",
                           sufixo_len, sufixo);

    return escrito > 0 && (size_t) escrito < out_size;
}

/* -------------------------------------------------------
 * Identifica o indice do no a partir do topico, apenas
 * para incrementar o contador de estatisticas correto.
 * Retorna -1 se nao encontrar correspondencia.
 * ------------------------------------------------------- */
static int indice_do_topico(const char *topico, int len)
{
    for (int i = 0; i < N_TOPICS_DATA; i++) {
        size_t ref_len = strlen(TOPICS_DATA[i]);
        if ((size_t) len == ref_len &&
            memcmp(topico, TOPICS_DATA[i], ref_len) == 0) {
            return i;
        }
    }
    return -1;
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
        for (int i = 0; i < N_TOPICS_DATA; i++) {
            esp_mqtt_client_subscribe(mqtt_client, TOPICS_DATA[i], 0);
            ESP_LOGI(TAG, "Subscrito em %s", TOPICS_DATA[i]);
        }
        xEventGroupSetBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "Desconectado do broker MQTT.");
        xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        break;

    case MQTT_EVENT_DATA:
        {
            /*
             * TB: momento em que o controlador recebe a mensagem.
             * Gravado imediatamente ao entrar no evento, antes de
             * qualquer processamento, para minimizar o erro na
             * medicao de (TC - TB) descontada pelo publicador.
             */
            int64_t tb_us = esp_timer_get_time();

            char topico_cmd[32];
            bool ok = derivar_topico_cmd(event->topic, event->topic_len,
                                         topico_cmd, sizeof(topico_cmd));

            if (!ok) {
                ESP_LOGW(TAG, "Topico nao reconhecido, ignorando.");
                break;
            }

            int idx = indice_do_topico(event->topic, event->topic_len);
            if (idx >= 0) {
                total_recebidas[idx]++;
            }

            /*
             * Republica o payload recebido sem modificacao.
             * O publicador original ja incluiu seu proprio
             * timestamp (TA) dentro do payload; o controlador
             * nao precisa adicionar nada ele so precisa
             * devolver a mensagem o mais rapido possivel.
             */
            int64_t tc_us = esp_timer_get_time();

            esp_mqtt_client_publish(
                mqtt_client,
                topico_cmd,
                event->data,
                event->data_len,
                0,   /* QoS 0 */
                0    /* retain */
            );

            /* Log a cada 50 mensagens por no para nao poluir
             * o monitor serial nas taxas de carga mais altas. */
            if (idx >= 0 && total_recebidas[idx] % 50 == 0) {
                ESP_LOGI(TAG,
                    "ECHO | %s -> %s | proc=%lld us | total=%lu",
                    TOPICS_DATA[idx], topico_cmd,
                    (long long)(tc_us - tb_us),
                    (unsigned long) total_recebidas[idx]);
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
 * Tarefa de estatisticas periodicas: resumo a cada 10 s
 * ------------------------------------------------------- */
static void stats_task(void *pvParameters)
{
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(10000));
        ESP_LOGI(TAG, "--- STATS CONTROLADOR ---");
        for (int i = 0; i < N_TOPICS_DATA; i++) {
            ESP_LOGI(TAG, "  %s: %lu mensagens processadas",
                     TOPICS_DATA[i],
                     (unsigned long) total_recebidas[i]);
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

    ESP_LOGI(TAG, "=== PrioMQTT Controlador (ESP32 dedicado) ===");
    ESP_LOGI(TAG, "Broker: %s:%d", BROKER_IP, BROKER_PORT);
    ESP_LOGI(TAG, "Gerenciando %d topicos de dados", N_TOPICS_DATA);

    wifi_init();
    mqtt_init();

    
    xTaskCreate(stats_task, "stats_task_ctrl", 2048, NULL, 2, NULL);
}