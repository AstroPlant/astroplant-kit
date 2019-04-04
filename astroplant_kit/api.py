import paho.mqtt.client as mqtt
import json
import logging

logger = logging.getLogger("AstroPlant")

class Client(object):
    """
    AstroPlant API Client class implementing methods to interact with the AstroPlant API.
    """

    def __init__(self, host, port, keepalive=60, auth={}):
        self.connected = False
        
        self._mqtt_client = mqtt.Client()
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message
        
        self._mqtt_client.reconnect_delay_set(min_delay=1, max_delay=128)

        if auth:
            self.serial = auth['serial']
            self._mqtt_client.username_pw_set(username=auth['serial'], password=auth['secret'])
        else:
            self.serial = 'anon'
        
        self._mqtt_client.connect_async(host=host, port=port, keepalive=keepalive)

    def start(self):
        self._mqtt_client.loop_start()

    def stop(self):
        self._mqtt_client.loop_stop()

    def _on_connect(self, client, user_data, flags, rc):
        self.connected = True

    def _on_disconnect(self, client, user_data, rc):
        self.connected = False

    def _on_message(self, client, user_data, msg):
        topic = msg.topic
        payload = msg.payload

    def publish_stream_measurement(self, measurement):
        self._mqtt_client.publish(
            topic = f"kit/{self.serial}/measurements/stream",
            payload = json.dumps ({
                'peripheral': measurement.peripheral.get_name(),
                'physical_quantity': measurement.physical_quantity,
                'physical_unit': measurement.physical_unit,
                'datetime': measurement.start_datetime.isoformat() + 'Z',
                'value': measurement.value
            })
        )

    def publish_aggregate_measurement(self, measurement):
        self._mqtt_client.publish(
            topic = f"kit/{self.serial}/measurements/aggregate",
            payload = json.dumps ({
                'peripheral': measurement.peripheral.get_name(),
                'physical_quantity': measurement.physical_quantity,
                'physical_unit': measurement.physical_unit,
                'start_datetime': measurement.start_datetime.isoformat() + 'Z',
                'end_datetime': measurement.end_datetime.isoformat() + 'Z',
                'type': measurement.aggregate_type,
                'value': measurement.value
            })
        )
