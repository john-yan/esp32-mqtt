#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_system.h"
#include "esp_log.h"
#include "freertos/queue.h"
#include "esp32-mqtt.h"
#include <string>

#define TAG "ESP32_MQTT"

#define CHECK_NOT_NULL(p) assert((p) != NULL);

using std::string;

esp_err_t MQTT::default_mqtt_event_handler(esp_mqtt_event_handle_t event) {
  MQTT* mqtt = reinterpret_cast<MQTT*>(event->user_context);
  CHECK_NOT_NULL(mqtt);

  // handler internal signals
  switch(event->event_id) {
    case MQTT_EVENT_CONNECTED:
      ESP_LOGI(TAG, "MQTT_EVENT_CONNECTED");
      xEventGroupClearBits(mqtt->mqtt_event, MQTT_DISCONNECTED_BIT);
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_CONNECTED_BIT);
      break;
    case MQTT_EVENT_DISCONNECTED:
      ESP_LOGI(TAG, "MQTT_EVENT_DISCONNECTED");
      xEventGroupClearBits(mqtt->mqtt_event, MQTT_CONNECTED_BIT);
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_DISCONNECTED_BIT);
      break;
    case MQTT_EVENT_SUBSCRIBED:
      // ESP_LOGI(TAG, "MQTT_EVENT_SUBSCRIBED, msg_id=%d\n", event->msg_id);
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_SUBSCRIBED_BIT);
      break;
    case MQTT_EVENT_UNSUBSCRIBED:
      // ESP_LOGI(TAG, "MQTT_EVENT_UNSUBSCRIBED, msg_id=%d", event->msg_id);
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_UNSUBSCRIBED_BIT);
      break;
    case MQTT_EVENT_PUBLISHED:
      // ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED, msg_id=%d", event->msg_id);
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_PUBLISHED_BIT);
      break;
    case MQTT_EVENT_DATA: {
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_DATA_BIT);

      string data(event->data, event->data_len);
      string topic(event->topic, event->topic_len);
      
      mqtt_request_info_t* info = allocate_incoming_data_request_info(event->topic, event->topic_len, event->data, event->data_len);
      CHECK_NOT_NULL(info);
      if ( pdFALSE == xQueueSend(mqtt->request_queue, &info, 0)) {
        // xQueueSend fail
        deallocate_request_info(info);
      }
      break;
    }
    case MQTT_EVENT_ERROR:
      xEventGroupSetBits(mqtt->mqtt_event, MQTT_ERROR_BIT);
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

  static constexpr char const* MQTT_SERVER_URI = CONFIG_MQTT_SERVER_URI;

  event_handler_registry.resize(MQTT_EVENT_BEFORE_CONNECT + 1);
  if (broker_uri == NULL) {
    broker_uri = MQTT_SERVER_URI;
  }
  CHECK_NOT_NULL(broker_uri);
  esp_mqtt_client_config_t mqtt_cfg = { 0 };
  mqtt_cfg.uri = broker_uri;
  mqtt_cfg.event_handle = default_mqtt_event_handler;
  mqtt_cfg.user_context = this;

  client = esp_mqtt_client_init(&mqtt_cfg);
  CHECK_NOT_NULL(client);
  esp_mqtt_client_start(client);

  assert(pdPASS == xTaskCreate(mqtt_task, "mqtt_task", 4096, this, tskIDLE_PRIORITY, &mqtt_task_handle));
  request_queue = xQueueCreate(10, sizeof(mqtt_request_info_t*));
  mqtt_event = xEventGroupCreate();
  CHECK_NOT_NULL(request_queue);
  CHECK_NOT_NULL(mqtt_event);

  int connected = xEventGroupWaitBits(mqtt_event, MQTT_CONNECTED_BIT, false, true, portMAX_DELAY);
  assert(connected);
}

MQTT::mqtt_request_info_t* MQTT::allocate_subscribe_request_info(const char* topic, mqtt_topic_handler_t handler, void* handler_arg, int qos, mqtt_subscribed_callback_t callback, void* callback_arg) {
  mqtt_subscribe_info_t* info = new mqtt_subscribe_info_t();
  CHECK_NOT_NULL(info);
  info->topic = topic;
  info->qos = qos;
  info->topic_callback_info.handler = handler;
  info->topic_callback_info.handler_arg = handler_arg;
  info->subscribed_callback_info.callback = callback;
  info->subscribed_callback_info.callback_arg = callback_arg;

  mqtt_request_info_t* req_info = new mqtt_request_info_t();
  CHECK_NOT_NULL(req_info);
  req_info->type = mqtt_subscribe_request;
  req_info->info = info;
  return req_info;
}

MQTT::mqtt_request_info_t* MQTT::allocate_publish_request_info(const char* topic, const char* data, int qos, int retain, mqtt_published_callback_t, callback, void* callback_arg) {
  mqtt_publish_info_t* info = new mqtt_publish_info_t();
  info->topic = topic;
  info->data = data;
  info->qos = qos;
  info->retain = retain;
  info->published_callback_info.callback = callback;
  info->published_callback_info.callback_arg = callback_arg;

  mqtt_request_info_t* req_info = new mqtt_request_info_t();
  CHECK_NOT_NULL(req_info);
  req_info->type = mqtt_publish_request;
  req_info->info = info;
  return req_info;
}

MQTT::mqtt_request_info_t* MQTT::allocate_incoming_data_request_info(const char* topic, size_t topic_length, const char* data, size_t data_length) {
  mqtt_incoming_data_info_t* info = new mqtt_incoming_data_info_t();
  info->topic = std::string(topic, topic_length);
  info->data = std::string(data, data_length);

  mqtt_request_info_t* req_info = new mqtt_request_info_t();
  CHECK_NOT_NULL(req_info);
  req_info->type = mqtt_incoming_data_request;
  req_info->info = info;
  return req_info;
}

void MQTT::deallocate_request_info(mqtt_request_info_t* info) {
  switch(info->type) {
    case mqtt_subscribe_request: {
      delete reinterpret_cast<mqtt_subscribe_info_t*>(info->info);
      break;
    }
    case mqtt_publish_request:
      delete reinterpret_cast<mqtt_publish_info_t*>(info->info);
      break;
    case mqtt_incoming_data_request: {
      delete reinterpret_cast<mqtt_incoming_data_info_t*>(info->info);
    }
    default:
      ESP_LOGE(TAG, "Unknown mqtt_request_info_t type = %d ... skip!", info->type);
      break;
  }
  delete info;
}

bool MQTT::subscribe(const char* topic, mqtt_topic_handler_t handler, void* handler_arg, int qos, mqtt_subscribed_callback_t callback, void* callback_arg) {
  mqtt_request_info_t* info = allocate_subscribe_request_info(topic, handler, handler_arg, qos, callback, callback_arg);
  CHECK_NOT_NULL(info);
  if ( pdPASS == xQueueSend(request_queue, &info, 0)) {
    return true;
  }

  deallocate_request_info(info);
  return false;

  // int msg_id = esp_mqtt_client_subscribe(client, topic, qos);
  // return msg_id;
}

bool MQTT::publish(const char* topic, const char* data, int qos, int retain, mqtt_published_callback_t callback, void* callback_arg) {
  mqtt_request_info_t* info = allocate_publish_request_info(topic, data, qos, retain, callback, callback_arg);
  CHECK_NOT_NULL(info);
  if ( pdPASS == xQueueSend(request_queue, &info, 0)) {
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
  assert(connected);

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
  assert(connected);

  xEventGroupClearBits(mqtt_event, MQTT_PUBLISHED_BIT);

  int msg_id = esp_mqtt_client_publish(client, info->topic.c_str(), info->data.c_str(), info->data.length(), info->qos, info->retain);

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

  mqtt_topic_callback_info_t cb_info = it->second;
  mqtt_topic_handler_t handler = cb_info.handler;
  assert(handler);

  handler(topic, data, cb_info.handler_arg);
}

void MQTT::mqtt_task(void* parameter) {
  MQTT* mqtt = reinterpret_cast<MQTT*>(parameter);
  CHECK_NOT_NULL(mqtt);
  CHECK_NOT_NULL(mqtt->request_queue);
  CHECK_NOT_NULL(mqtt->mqtt_event);

  mqtt_request_info_t* info = NULL;

  while (1) {
    assert(pdPASS == (xQueueReceive(mqtt->request_queue, &info, portMAX_DELAY)));
    CHECK_NOT_NULL(info);

    switch(info->type) {
      case mqtt_subscribe_request: {
        mqtt_subscribe_info_t* sub_info = reinterpret_cast<mqtt_subscribe_info_t*>(info->info);
        while(false == mqtt->try_to_subscribe(sub_info)) {
          ESP_LOGW(TAG, "Subscribing to %s has failed.", sub_info->topic.c_str());
        }
        // success
        if (sub_info->subscribed_callback_info.callback) {
          sub_info->subscribed_callback_info.callback(true, sub_info->topic, sub_info->subscribed_callback_info.callback_arg);
        }
        break;
      }
      case mqtt_publish_request: {
        mqtt_publish_info_t* pub_info = reinterpret_cast<mqtt_publish_info_t*>(info->info);
        while(false == mqtt->try_to_publish(pub_info)) {
          ESP_LOGW(TAG, "Publishing to %s has failed.", pub_info->topic.c_str());
        }
        if (pub_info->published_callback_info.callback) {
          pub_info->published_callback_info.callback(true, pub_info->topic, pub_info->data, pub_info->published_callback_info.callback_arg);
        }
        break;
      }
      case mqtt_incoming_data_request: {
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

