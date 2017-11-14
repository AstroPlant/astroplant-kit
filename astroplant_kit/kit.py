import signal
import functools
import asyncio
import astroplant_client
import peripheral

class Kit(object):
    def __init__(self, api_client: astroplant_client.Client):
        self.peripheral_manager = peripheral.PeripheralManager()
        self.peripheral_manager.subscribe_predicate(lambda a: True, lambda m: self.publish_measurement(m))
        self.api_client = api_client
        self.configure()
        self.api_client._open_websocket()

    def configure(self):
        configuration = self.api_client.configuration_path.kit_configuration().body[0]
        self.name = configuration['name']

        self._configure_peripherals(configuration['peripherals'])

    def _configure_peripherals(self, peripheral_configurations):
        for peripheral_configuration in peripheral_configurations:
            self.peripheral_manager.create_peripheral(peripheral_configuration['class_name'], peripheral_configuration['peripheral_name'], peripheral_configuration['parameters'])
            print(peripheral_configuration)

    def publish_measurement(self, measurement):
        self.api_client.publish_measurement(measurement)

    def run(self):
        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_until_complete(self.peripheral_manager.run())
        except KeyboardInterrupt:
            pass
        finally:
            self.event_loop.stop()

        self.event_loop.close()
