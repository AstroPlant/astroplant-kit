import signal
import functools
import asyncio
import astroplant_sensor_library.manager
import astroplant_client

class Kit(object):
    def __init__(self, sensor_manager: astroplant_sensor_library.manager.SensorManager, api_client: astroplant_client.Client):
        self.sensor_manager = sensor_manager
        self.sensor_manager.subscribe_predicate(lambda a: True, lambda m: self.publish_measurement(m))
        self.api_client = api_client
        self.api_client._open_websocket()

    def publish_measurement(self, measurement):
        self.api_client.publish_measurement(1, measurement.get_value())

    def run(self):
        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_until_complete(self.sensor_manager.run())
        except KeyboardInterrupt:
            pass
        finally:
            self.event_loop.stop()

        self.event_loop.close()
