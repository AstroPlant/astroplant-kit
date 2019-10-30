import asyncio
import paho.mqtt.client as mqtt
import json
from io import BytesIO
import logging

from .schema import astroplant_capnp
from .server_rpc import ServerRpc
from .kit_rpc import KitRpc

logger = logging.getLogger("astroplant_kit.api.client")


class Client(object):
    """
    AstroPlant API Client class implementing methods to interact with the AstroPlant API.
    """
    def __init__(self, event_loop, host, port, keepalive=60, auth={}):
        self.connected = False

        self._event_loop = event_loop
        self._kit_rpc_handler = None

        self._server_rpc = ServerRpc(event_loop, self._server_rpc_request)
        self._kit_rpc = KitRpc(event_loop, self._kit_rpc_response)

        self._mqtt_client = mqtt.Client()
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.reconnect_delay_set(min_delay=1, max_delay=128)

        if auth:
            self.serial = auth['serial']
            logger.warn("TODO FIX username")
            self._mqtt_client.username_pw_set(
                username="k_develop", #auth['serial'],
                password=auth['secret'],
            )
        else:
            self.serial = 'anon'

        logger.debug(f"Connecting to MQTT broker at {host}:{port}.")
        self._mqtt_client.connect_async(host=host, port=port, keepalive=keepalive)

    def register_kit_rpc_handler(self, kit_rpc_handler):
        self._kit_rpc._register_handler(kit_rpc_handler)

    def _server_rpc_request(self, payload):
        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/server-rpc/request',
            payload = payload,
            qos = 1 # Deliver at least once.
        )

    def _kit_rpc_response(self, payload):
        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/kit-rpc/response',
            payload = payload,
            qos = 1 # Deliver at least once.
        )

    def start(self):
        """
        Start the client background thread.
        """
        self._mqtt_client.loop_start()
        self._mqtt_client.subscribe(f'kit/#')

    def stop(self):
        """
        Stop the client background thread.
        """
        self._mqtt_client.loop_stop()

    @property
    def server_rpc(self):
        """
        Get a handle to the server RPC.
        """
        return self._server_rpc

    def _on_connect(self, client, user_data, flags, rc):
        """
        Handles (re)connections.
        """
        logger.info("Connected.")
        self._mqtt_client.subscribe(f'kit/{self.serial}/server-rpc/response', qos=1)
        self._mqtt_client.subscribe(f'kit/{self.serial}/kit-rpc/request', qos=1)
        self.connected = True

    def _on_disconnect(self, client, user_data, rc):
        """
        Handles disconnections.
        """
        logger.info("Disconnected.")
        self.connected = False

    def _on_message(self, client, user_data, msg):
        """
        Handles received messages.
        """
        topic = msg.topic
        payload = msg.payload

        topics = topic.split("/")

        router = {
            'server-rpc': {
                'response': self._server_rpc._on_response,
            },
            'kit-rpc': {
                'request': self._kit_rpc._on_request,
            },
        }

        if len(topics) >= 2:
            for path in topics[2:]:
                if path in router:
                    router = router[path]
                    if callable(router):
                        router(payload)
                        break
                else:
                    logger.warn("unknown MQTT route:", topics)

    def publish_raw_measurement(self, measurement):
        """
        Publish a (real-time) raw measurement.
        """
        logger.debug(f"Sending raw measurement: {measurement.__dict__}")

        raw_measurement_msg = astroplant_capnp.RawMeasurement.new_message(
                kitSerial = '', # Filled on the backend-side for security reasons
                datetime = round(measurement.end_datetime.timestamp() * 1000),
                peripheral = measurement.peripheral.get_id(),
                quantityType = measurement.quantity_type.id,
                value = measurement.value
            )

        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/measurement/raw',
            payload = raw_measurement_msg.to_bytes_packed(),
            qos = 0 # Deliver at most once.
        )

    def publish_aggregate_measurement(self, measurement):
        """
        Publish an aggregate measurement.
        """
        logger.debug(f"Sending aggregate measurement: {measurement.__dict__}")
        msg = BytesIO()

        aggregate_measurement_msg = astroplant_capnp.AggregateMeasurement.new_message(
                kitSerial = '', # Filled on the backend-side for security reasons
                datetimeStart = round(measurement.start_datetime.timestamp() * 1000),
                datetimeEnd = round(measurement.end_datetime.timestamp() * 1000),
                peripheral = measurement.peripheral.get_id(),
                quantityType = measurement.quantity_type.id,
                aggregateType = measurement.aggregate_type,
                value = measurement.value
            )

        self._mqtt_client.publish(
            topic = f'kit/{self.serial}/measurement/aggregate',
            payload = msg.getvalue(),
            qos = 2 # Deliver exactly once. Maybe downgrade to `1`: deliver at least once.
        )
