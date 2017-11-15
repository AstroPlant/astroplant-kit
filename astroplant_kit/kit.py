import signal
import functools
import asyncio
import importlib
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
        """
        Configure the kit using the configuration from the backend.
        """
        configuration = self.api_client.configuration_path.kit_configuration().body[0]
        self.name = configuration['name']

        self._import_modules(configuration['modules'])
        self._configure_peripherals(configuration['peripherals'])

    def _import_modules(self, modules):
        """
        Import Python modules by name and add them to the globals.

        :param modules: An iterable with module names to import
        """
        for module_name in modules:
            module = importlib.import_module(module_name)
            globals()[module_name] = module

    def _configure_peripherals(self, peripheral_configurations):
        """
        Configure the kit peripherals using configuration dicts.

        :param peripheral_configurations: An iterable of peripheral configuration dicts.
        """
        for peripheral_configuration in peripheral_configurations:
            # Get class by class name
            try:
                peripheral_class = globals()[peripheral_configuration['module_name']].__dict__[peripheral_configuration['class_name']]
            except KeyError:
                raise ValueError("Could not find class '%s' in module '%s'" % (peripheral_configuration['class_name'], peripheral_configuration['module_name']))

            self.peripheral_manager.create_peripheral(peripheral_class, peripheral_configuration['peripheral_name'], peripheral_configuration['parameters'])
            print(peripheral_configuration)

    def publish_measurement(self, measurement):
        """
        Publish a measurement to the back-end.

        :param measurement: The measurement to publish.
        """
        self.api_client.publish_measurement(measurement)

    def run(self):
        """
        Run the async event loop.
        """

        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_until_complete(self.peripheral_manager.run())
        except KeyboardInterrupt:
            pass
        finally:
            self.event_loop.stop()

        self.event_loop.close()
