#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_system.h"
#include "esp_log.h"
#include "esp_http_client.h"
#include "freertos/queue.h"
#include "utils.h"
#include "client.h"
#include "controller.h"
#include "cJSON.h"
#include "event.h"
#include <string>

#define TAG "CLIENT"

esp_err_t MQTT::default_mqtt_event_handler(esp_mqtt_event_handle_t event) {
  MQTT* mqtt = reinterpret_cast<MQTT*>(event->user_context);
  CHECK_NOT_NULL(mqtt);

  // handler internal signals
  switch(event->event_id) {
    case MQTT_EVENT_CONNECTED:
      ESP_LOGI(TAG, "MQTT_EVENT_CONNECTED");
      xEventGroupClearBits(mqtt_event, MQTT_DISCONNECTED_BIT);
      xEventGroupSetBits(mqtt_event, MQTT_CONNECTED_BIT);
      break;
    case MQTT_EVENT_DISCONNECTED:
      ESP_LOGI(TAG, "MQTT_EVENT_DISCONNECTED");
      xEventGroupClearBits(mqtt_event, MQTT_CONNECTED_BIT);
      xEventGroupSetBits(mqtt_event, MQTT_DISCONNECTED_BIT);
      break;
    case MQTT_EVENT_SUBSCRIBED:
      // ESP_LOGI(TAG, "MQTT_EVENT_SUBSCRIBED, msg_id=%d\n", event->msg_id);
      xEventGroupSetBits(mqtt_event, MQTT_SUBSCRIBED_BIT);
      break;
    case MQTT_EVENT_UNSUBSCRIBED:
      // ESP_LOGI(TAG, "MQTT_EVENT_UNSUBSCRIBED, msg_id=%d", event->msg_id);
      xEventGroupSetBits(mqtt_event, MQTT_UNSUBSCRIBED_BIT);
      break;
    case MQTT_EVENT_PUBLISHED:
      // ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED, msg_id=%d", event->msg_id);
      xEventGroupSetBits(mqtt_event, MQTT_PUBLISHED_BIT);
      break;
    case MQTT_EVENT_DATA: {
      xEventGroupSetBits(mqtt_event, MQTT_DATA_BIT);

      string data(event->data, event->data_len);
      string topic_str(event->topic, event->topic_len);
      
      mqtt_request_info_t* info = allocate_incoming_data_request_info(topic, handler);
      CHECK_NOT_NULL(info);
      if ( pdFALSE == xQueueSend(request_queue, &info, 0))
        // xQueueSend fail
        deallocate_request_info(info);
      }
      break;
    }
    case MQTT_EVENT_ERROR:
      xEventGroupSetBits(mqtt_event, MQTT_ERROR_BIT);
      ESP_LOGI(TAG, "MQTT_EVENT_ERROR");
      break;
    default:
      ESP_LOGI(TAG, "Other event id:%d", event->event_id);
      break;
  };


  // handler external signal
  mqtt_callback_info_t info = mqtt->event_handler_registry[event->event_id];
  if (info.handler) {
    return info.handler(event, info.handler_arg);
  }

  return ESP_OK;
};

void MQTT::init(const char* broker_uri) {

  event_handler_registry.reserve(MQTT_EVENT_BEFORE_CONNECT);
  CHECK_NOT_NULL(broker_uri);
  esp_mqtt_client_config_t mqtt_cfg = { 0 };
  mqtt_cfg.uri = broker_uri;
  mqtt_cfg.event_handle = default_mqtt_event_handler;
  mqtt_cfg.user_context = this;

  client = esp_mqtt_client_init(&mqtt_cfg);
  CHECK_NOT_NULL(client);
  esp_mqtt_client_start(client);

  CHECK_PASS(xTaskCreate(mqtt_task, "mqtt_task", 4096, this, tskIDLE_PRIORITY, &mqtt_task_handle));
  request_queue = xQueueCreate(10, sizeof(mqtt_request_info_t*));
  mqtt_event = xEventGroupCreate();
  CHECK_NOT_NULL(request_queue);
  CHECK_NOT_NULL(mqtt_event);

  int connected = xEventGroupWaitBits(mqtt_event, MQTT_CONNECTED_BIT, false, true, portMAX_DELAY);
  CHECK(connected);
}

mqtt_request_info_t* MQTT::allocate_subscribe_request_info(const char* topic, mqtt_topic_handler_t handler, void* handler_arg, int qos = 1) {
  mqtt_subscribe_info_t* info = new mqtt_subscribe_info_t();
  CHECK_NOT_NULL(info);
  info->topic = topic;
  info->qos = qos;
  info->callback_info.handler = handler;
  info->callback_info.handler_arg = handler_arg;

  return new mqtt_request_info_t(mqtt_subscribe_request, info);
}

mqtt_request_info_t* MQTT::allocate_publish_request_info(const char* topic, const char* data, int qos = 1, int retain = 0) {
  mqtt_publish_info_t* info = new mqtt_publish_info_t();
  info->topic = topic;
  info->data = data;
  info->qos = qos;
  info->retain = retain;

  return new mqtt_request_info_t(mqtt_publish_request, info);
}

mqtt_request_info_t* MQTT::allocate_incoming_data_request_info(const char* topic, size_t topic_length, const char* data, size_t data_length) {
  mqtt_incoming_data_info_t* info = new mqtt_incoming_data_info_t();
  info->topic = std::string(topic, topic_length);
  info->data = std::string(data, data_length);

  return new mqtt_request_info_t(mqtt_handle_incoming_data_request, info);
}

void MQTT::deallocate_request_info(mqtt_request_info_t* info) {
  delete info->info;
  delete info;
}

bool MQTT::subscribe(const char* topic, mqtt_topic_handler_t handler, void* handler_arg, int qos = 1) {
  mqtt_request_info_t* info = allocate_subscribe_request_info(topic, handler, handler_arg, qos);
  CHECK_NOT_NULL(info);
  if ( pdTRUE == xQueueSend(request_queue, &info, 0)) {
    return true;
  }

  deallocate_request_info(info);
  return false;

  // int msg_id = esp_mqtt_client_subscribe(client, topic, qos);
  // return msg_id;
}

int MQTT::publish(const char* topic, const char* data, int qos, int retain) {
  mqtt_request_info_t* info = allocate_publish_request_info(topic, data, qos, retain);
  CHECK_NOT_NULL(info);
  if ( pdTRUE == xQueueSend(request_queue, &info, 0)) {
    return true;
  }

  deallocate_request_info(info);
  return false;

  // int msg_id = esp_mqtt_client_publish(client, topic, data, len, qos, retain);
  // return msg_id;
}

bool MQTT::try_to_subscribe(mqtt_subscribe_info_t* info) {

  assert(info);

  int connected = xEventGroupWaitBits(mqtt_event, MQTT_CONNECTED_BIT, false, true, portMAX_DELAY);
  CHECK(connected);

  xEventGroupClearBits(mqtt_event, MQTT_SUBSCRIBED_BIT);

  int msg_id = esp_mqtt_client_subscribe(client, info->topic.c_str(), info->qos);

  if (msg_id == -1) {
    // unsuccessful
    return false;
  }

  int status = xEventGroupWaitBits(mqtt_event, MQTT_SUBSCRIBED_BIT | MQTT_ERROR_BIT | MQTT_DISCONNECTED_BIT, false, false, portMAX_DELAY);

  if (0 == (status & MQTT_SUBSCRIBED_BIT)) {
    // unsuccessful
    return false;
  }

  // successful
  xEventGroupClearBits(mqtt_event, MQTT_SUBSCRIBED_BIT);

  if(topic_registry.find(info->topic) != topic_registry.end()) {
    ESP_LOGW(TAG, "topic %s is subscribed twice!", info->topic.c_str());
  }

  topic_registry[info->topic] = info->callback_info;
  return true;
}

bool MQTT::try_to_publish(mqtt_publish_info_t* info) {

  assert(info);

  int connected = xEventGroupWaitBits(mqtt_event, MQTT_CONNECTED_BIT, false, true, portMAX_DELAY);
  CHECK(connected);

  xEventGroupClearBits(mqtt_event, MQTT_PUBLISHED_BIT);

  int msg_id = esp_mqtt_client_publish(info->topic.c_str(), info->data.c_str(), info->data.length(), info->qos, info->retain);

  if (msg_id == -1) {
    // unsuccessful
    return false;
  }

  int status = xEventGroupWaitBits(mqtt_event, MQTT_PUBLISHED_BIT | MQTT_ERROR_BIT | MQTT_DISCONNECTED_BIT, false, false, portMAX_DELAY);

  if (0 == (status & MQTT_PUBLISHED_BIT)) {
    // unsuccessful
    return false;
  }

  // successful
  xEventGroupClearBits(mqtt_event, MQTT_PUBLISHED_BIT);
  return true;
}

void MQTT::handle_incoming_data_request(mqtt_incoming_data_info_t* info) {
  assert(info);
  const string& topic = info->topic;
  const string& data = info->data;

  auto it = topic_registry.find(topic);
  if (it == topic_registry.end()) {
    // unable to find the handler
    return;
  }

  mqtt_topic_callback_info_t info = it->second;
  assert(info);

  mqtt_topic_handler_t handler = info->handler;
  assert(handler);

  handler(topic, data, info->handler_arg);
}

void MQTT::mqtt_task(void* parameter) {
  MQTT* mqtt = reinterpret_cast<MQTT*>(parameter);
  CHECK_NOT_NULL(mqtt);
  CHECK_NOT_NULL(mqtt->request_queue);
  CHECK_NOT_NULL(mqtt->mqtt_event);

  mqtt_request_info_t* info = NULL;

  while (1) {
    CHECK_TRUE(xQueueReceive(mqtt->request_queue, &info, portMAX_DELAY));
    CHECK_NOT_NULL(info);

    switch(info->type) {
      case mqtt_subscribe_request: {
        while(false == mqtt->try_to_subscribe(reinterpret_cast<mqtt_subscribe_info_t*>(info->info))) {
          ESP_LOGW(TAG, "Subscribing to %s has failed.", info->topic->c_str());
        }
        break;
      }
      case mqtt_publish_request:
        while(false == mqtt->try_to_publish(reinterpret_cast<mqtt_publish_info_t*>(info->info))) {
          ESP_LOGW(TAG, "Publishing to %s has failed.", info->topic->c_str());
        }
        break;
      case mqtt_handle_incoming_data_request: {
        mqtt->handle_incoming_data_request(reinterpret_cast<mqtt_incoming_data_info_t*>(info->info));
      }
      default:
        ESP_LOGE(TAG, "Unknown MQTTClientRequestType = %d ... skip!", info->type);
        break;
    }
    deallocate_request_info(info);
    info = NULL;
  }
}

