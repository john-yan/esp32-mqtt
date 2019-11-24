
#ifndef ESP32_MQTT
#define ESP32_MQTT
#include "freertos/event_groups.h"
#include "esp_http_client.h"
#include "freertos/queue.h"
#include "mqtt_client.h"
#include <string>
#include <vector>
#include <map>

class MQTT {
  typedef esp_err_t (*mqtt_handler_t)(esp_mqtt_event_handle_t event, void* handler_arg);
  typedef struct {
    mqtt_handler_t handler;
    void* handler_arg;
  } mqtt_callback_info_t;

  typedef void (*mqtt_topic_handler_t)(const std::string& topic, const std::string& data, void* handler_arg);
  typedef struct {
    mqtt_topic_handler_t handler;
    void* handler_arg;
  } mqtt_topic_callback_info_t;

  typedef void (*mqtt_subscribed_callback_t)(int status, const std::string& topic, void* callback_arg);
  typedef struct {
    mqtt_subscribed_callback_t callback;
    void* callback_arg;
  } mqtt_subscribed_callback_info_t;

  typedef struct {
    std::string topic;
    int qos;
    mqtt_topic_callback_info_t topic_callback_info;
    mqtt_subscribed_callback_info_t subscribed_callback_info;
  } mqtt_subscribe_info_t;

  typedef void (*mqtt_published_callback_t)(int status, const std::string& topic, const std::string& data, void* callback_arg);
  typedef struct {
    mqtt_published_callback_t callback;
    void* callback_arg;
  } mqtt_published_callback_info_t;

  typedef struct {
    std::string topic;
    std::string data;
    int qos;
    int retain;
    mqtt_published_callback_info_t published_callback_info;
  } mqtt_publish_info_t;

  typedef struct {
    std::string topic;
    std::string data;
  } mqtt_incoming_data_info_t;

  enum MQTTEventBits {
    MQTT_ERROR_BIT = 1 << MQTT_EVENT_ERROR,
    MQTT_CONNECTED_BIT = 1 << MQTT_EVENT_CONNECTED,
    MQTT_DISCONNECTED_BIT = 1 << MQTT_EVENT_DISCONNECTED,
    MQTT_SUBSCRIBED_BIT = 1 << MQTT_EVENT_SUBSCRIBED,
    MQTT_UNSUBSCRIBED_BIT = 1 << MQTT_EVENT_UNSUBSCRIBED,
    MQTT_PUBLISHED_BIT = 1 << MQTT_EVENT_PUBLISHED,
    MQTT_DATA_BIT = 1 << MQTT_EVENT_DATA,
    MQTT_BEFORE_CONNECT_BIT = 1 << MQTT_EVENT_BEFORE_CONNECT
  };

  enum mqtt_request_t {
    mqtt_subscribe_request,
    mqtt_publish_request,
    mqtt_incoming_data_request,

    mqtt_unknown_request
  };

  typedef struct {
    mqtt_request_t type;
    void* info;
  } mqtt_request_info_t;

  static mqtt_request_info_t* allocate_subscribe_request_info(const char* topic,
      mqtt_topic_handler_t handler, void* handler_arg,
      int qos = 1, mqtt_subscribed_callback_t callback = NULL, void* callback_arg = NULL);
  static mqtt_request_info_t* allocate_publish_request_info(const char* topic,
      const char* data, int qos = 1, int retain = 0,
      mqtt_published_callback_t callback = NULL, void* callback_arg = NULL);
  static mqtt_request_info_t* allocate_incoming_data_request_info(const char* topic,
      size_t topic_length, const char* data, size_t data_length);
  static void deallocate_request_info(mqtt_request_info_t* info);

  static esp_err_t default_mqtt_event_handler(esp_mqtt_event_handle_t event);
  static void mqtt_task(void*);
  bool try_to_subscribe(mqtt_subscribe_info_t* info);
  bool try_to_publish(mqtt_publish_info_t* info);
  void handle_incoming_data_request(mqtt_incoming_data_info_t* info);

  TaskHandle_t mqtt_task_handle = NULL;
  QueueHandle_t request_queue = NULL;
  EventGroupHandle_t mqtt_event = NULL;
  esp_mqtt_client_handle_t client = NULL;
  std::vector<mqtt_callback_info_t> event_handler_registry;
  std::map<std::string, mqtt_topic_callback_info_t> topic_registry;

  public:
    void init(const char* broker_uri = NULL);

    mqtt_callback_info_t onEvent(esp_mqtt_event_id_t event_id, mqtt_handler_t _handler, void* _handler_arg) {
      mqtt_callback_info_t old = event_handler_registry[event_id];
      event_handler_registry[event_id] = {_handler, _handler_arg};
      return old;
    }

    bool subscribe(const char* topic, mqtt_topic_handler_t handler, void* handler_arg,
        int qos = 1, mqtt_subscribed_callback_t callback = NULL, void* callback_arg = NULL);
    bool publish(const char* topic, const char* data,
        int qos = 1, int retain = 0, mqtt_published_callback_t callback = NULL, void* callback_arg = NULL);
};

#endif // ESP32_MQTT

