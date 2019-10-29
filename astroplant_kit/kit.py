"""
Contains the main kit routines.
"""

# Make sure astroplant_kit is in the path
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/..'))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import datetime
import signal
import functools
import asyncio
import threading
import time
import importlib
import logging
from astroplant_kit import peripheral
from .api import Client, RpcError
from .cache import Cache

logger = logging.getLogger("astroplant_kit.kit")

class Kit(object):
    def __init__(self, event_loop, api_client: Client, debug_configuration, cache: Cache):
        self.halt = False
        self.startup_time = datetime.datetime.now()

        self._event_loop = event_loop
        self.peripheral_modules = {}
        self.peripheral_manager = peripheral.PeripheralManager()
        self.peripheral_manager.subscribe_predicate(lambda a: True, lambda m: self.publish_measurement(m))
        self.api_client = api_client
        self.cache = cache

        self.messages = []
        self.messages_condition = threading.Condition()

        self.initialise_debug(debug_configuration)

    def initialise_debug(self, debug_configuration):
        """
        Initialise debugging options from the given debug configuration dictionary.

        :param debug_configuration: The debug configuration dictionary. Contains the
        configuration for a Display module to write to, as well as the level of logging
        required.
        """
        debug_level = debug_configuration['level']
        if 'peripheral_display' in debug_configuration:
            logger.info("Initialising peripheral debug display device.")
            peripheral_configuration = debug_configuration['peripheral_display']

            parameters = peripheral_configuration['parameters'] if ('parameters' in peripheral_configuration) else {}

            self._import_modules([peripheral_configuration['module_name']])
            peripheral_class = self.peripheral_modules[peripheral_configuration['module_name']].__dict__[peripheral_configuration['class_name']]
            peripheral_device = self.peripheral_manager.create_peripheral(peripheral_class, -1, "Debug display device", parameters)
            logger.info("Peripheral debug display device created.")

            log_handler = logging.StreamHandler(peripheral.DisplayDeviceStream(peripheral_device))
            log_handler.setLevel(debug_level)
            formatter = logging.Formatter("%(levelname)s\n%(message)s")
            log_handler.setFormatter(formatter)

            logger.addHandler(log_handler)
            logger.debug("Peripheral debug display device log handler added.")

    def _configure(self, configuration):
        """
        Configure the kit.
        """
        # configuration = self.api_client.configuration_path.kit_configuration().body[0]

        self._configuration = configuration
        logger.info(f"Activating configuration {configuration['description']}")

        modules = set()
        for peripheral_with_definition in configuration['peripherals']:
            definition = peripheral_with_definition['definition']
            modules.add(definition['moduleName'])

        self._import_modules(modules)
        self._configure_peripherals(configuration['peripherals'])

    def _import_modules(self, modules):
        """
        Import Python modules by name and add them to the dictionary
        of loaded peripheral modules.

        :param modules: An iterable with module names to import
        """
        for module_name in modules:
            module = importlib.import_module(module_name)
            self.peripheral_modules[module_name] = module

    def _configure_peripherals(self, peripherals):
        """
        Configure the kit peripherals using configuration dicts.

        :param peripheral_configurations: An iterable of peripheral configuration dicts.
        """
        for peripheral_with_definition in peripherals:
            peripheral = peripheral_with_definition['peripheral']
            definition = peripheral_with_definition['definition']

            module_name = definition['moduleName']
            class_name = definition['className']
            try:
                logger.debug(f'Initializing a peripheral of {module_name}.{class_name}')
                peripheral_class = self.peripheral_modules[module_name].__dict__[class_name]
            except KeyError:
                raise ValueError("Could not find class '%s' in module '%s'" % (class_name, module_name))

            self.peripheral_manager.create_peripheral(peripheral_class, peripheral['id'], peripheral['name'], peripheral['configuration'])

    def publish_measurement(self, measurement):
        """
        Enqueue a measurement for publishing to the back-end.

        :param measurement: The measurement to publish.
        """
        MAX_PENDING_MEASUREMENTS = 200

        with self.messages_condition:
            self.messages.append(measurement)

            if len(self.messages) > MAX_PENDING_MEASUREMENTS: # Only allow buffer to grow up to a cap.
                self.messages = self.messages[-MAX_PENDING_MEASUREMENTS:]
            self.messages_condition.notify()

    def _api_worker(self):
        """
        Runs the API message worker; publishes measurements to the API.
        """
        while True:
            with self.messages_condition:
                if len(self.messages) == 0:
                    # Wait until there is at least one measurement available
                    self.messages_condition.wait()

                measurement = self.messages.pop(0)

            logger.debug('Publishing measurement: %s' % measurement)
            # TODO: perhaps messsages should not be retried, and instead the API client
            # stores failed messages in the filesystem locally for intermittent retry.
            try:
                if measurement.aggregate_type is None:
                    self.api_client.publish_raw_measurement(measurement)
                else:
                    self.api_client.publish_aggregate_measurement(measurement)
            except Exception as e:
                with self.messages_condition:
                    # Re-insert failed measurement.
                    self.messages.insert(0, measurement)

                time.sleep(15)
                logger.warning(f"Failed to publish measurement. Will retry. Original exception: {e}")

    def run(self):
        """
        Run the kit.
        """

        logger.info('Starting.')

        # Run the API worker in a separate thread
        api_worker = threading.Thread(target=self._api_worker)
        api_worker.daemon = True
        api_worker.start()

        # Run the async event loop
        try:
            self._event_loop.create_task(self.async_bootstrap())
            self._event_loop.run_forever()
        except KeyboardInterrupt:
            # Request halt
            self.halt = True
            print("HALT received...")

    async def _fetch_and_store_configuration(self):
        logger.debug("Fetching kit configuration.")
        configuration = await self.api_client.server_rpc.get_active_configuration()
        print(configuration)
        self.cache.write_configuration(configuration)
        return configuration

    async def async_bootstrap(self):
        try:
            configuration = await self._fetch_and_store_configuration()
        except RpcError as e:
            logger.warn(f'Could not get configuration from server, trying cache. Original error: {e}')
            try:
                configuration = self.cache.read_configuration()
            except Exception as e:
                logger.warn(f'Could not get configuration from cache, stoping. Original error: {e}')
                return

        self._configure(configuration)
        self.peripheral_manager.run()
