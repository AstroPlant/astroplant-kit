import paho.mqtt.client as mqtt
import json
import fastavro
from io import BytesIO
import logging

logger = logging.getLogger("AstroPlant.api")

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

        with open('./schema/aggregate-measurement.avsc', 'r') as f:
            self._aggregate_schema = fastavro.parse_schema(json.load(f))

        with open('./schema/raw-measurement.avsc', 'r') as f:
            self._raw_schema = fastavro.parse_schema(json.load(f))

        if auth:
            self.serial = auth['serial']
            self._mqtt_client.username_pw_set(username=auth['serial'], password=auth['secret'])
        else:
            self.serial = 'anon'

        logger.debug(f"Connecting to MQTT broker at {host}:{port}.")
        self._mqtt_client.connect_async(host=host, port=port, keepalive=keepalive)

    def start(self):
        """
        Start the client background thread.
        """
        self._mqtt_client.loop_start()

    def stop(self):
        """
        Stop the client background thread.
        """
        self._mqtt_client.loop_stop()

    def _on_connect(self, client, user_data, flags, rc):
        logger.info("Connected.")
        self.connected = True

    def _on_disconnect(self, client, user_data, rc):
        logger.info("Disconnected.")
        self.connected = False

    def _on_message(self, client, user_data, msg):
        topic = msg.topic
        payload = msg.payload

    def publish_raw_measurement(self, measurement):
        """
        Publish a (real-time) raw measurement.
        """
        logger.debug(f"Sending raw measurement: {measurement.__dict__}")
        msg = BytesIO()
        fastavro.schemaless_writer(
            msg,
            self._raw_schema,
            {
                'kit_serial': '', # Filled on the backend-side for security reasons.
                'peripheral': measurement.peripheral.get_name(),
                'physical_quantity': measurement.physical_quantity,
                'physical_unit': measurement.physical_unit,
                'datetime': round(measurement.end_datetime.timestamp() * 1000),
                'value': measurement.value
            }
        )

        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/measurement/raw',
            payload = msg.getvalue(),
            qos = 0 # Deliver at most once.
        )

    def publish_aggregate_measurement(self, measurement):
        """
        Publish an aggregate measurement.
        """
        logger.debug(f"Sending aggregate measurement: {measurement.__dict__}")
        msg = BytesIO()
        fastavro.schemaless_writer(
            msg,
            self._aggregate_schema,
            {
                'kit_serial': '', # Filled on the backend-side for security reasons.
                'peripheral': measurement.peripheral.get_name(),
                'physical_quantity': measurement.physical_quantity,
                'physical_unit': measurement.physical_unit,
                'start_datetime': round(measurement.start_datetime.timestamp() * 1000),
                'end_datetime': round(measurement.end_datetime.timestamp() * 1000),
                'type': measurement.aggregate_type,
                'value': measurement.value
            }
        )

        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/measurement/aggregate',
            payload = msg.getvalue(),
            qos = 2 # Deliver exactly once. Maybe downgrade to `1`: deliver at least once.
        )
