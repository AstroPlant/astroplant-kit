"""
Contains the main kit routines.
"""

# Make sure astroplant_kit is in the path
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/..'))

import datetime
import signal
import functools
import asyncio
import threading
import time
import importlib
import logging
from astroplant_client import Client
from astroplant_kit import peripheral

logger = logging.getLogger("astroplant_kit.kit")

class Kit(object):
    def __init__(self, event_loop, api_client: Client, debug_configuration):
        self.halt = False
        self.startup_time = datetime.datetime.now()

        self._event_loop = event_loop
        self.peripheral_modules = {}
        self.peripheral_manager = peripheral.PeripheralManager()
        self.peripheral_manager.subscribe_predicate(lambda a: True, lambda m: self.publish_measurement(m))
        self.api_client = api_client

        self.messages = []
        self.messages_condition = threading.Condition()

        self.initialise_debug(debug_configuration)

        logger.info("Configuring kit.")
        try:
            self.configure()
        except Exception as e:
            logger.error("Could not configure kit: %s" % e)

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
            peripheral_device = self.peripheral_manager.create_peripheral(peripheral_class, "Debug display device", parameters)
            logger.info("Peripheral debug display device created.")

            log_handler = logging.StreamHandler(peripheral.DisplayDeviceStream(peripheral_device))
            log_handler.setLevel(debug_level)
            formatter = logging.Formatter("%(levelname)s\n%(message)s")
            log_handler.setFormatter(formatter)

            logger.addHandler(log_handler)
            logger.debug("Peripheral debug display device log handler added.")

    def configure(self):
        """
        Configure the kit using the configuration fetched from the backend.
        """
        # configuration = self.api_client.configuration_path.kit_configuration().body[0]

        # TODO: temporary configuration loading
        import json
        with open('./configuration.json') as f:
            configuration = json.load(f)[0]

        self.name = configuration['name']

        self._import_modules(configuration['modules'])
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

    def _configure_peripherals(self, peripheral_configurations):
        """
        Configure the kit peripherals using configuration dicts.

        :param peripheral_configurations: An iterable of peripheral configuration dicts.
        """
        for peripheral_configuration in peripheral_configurations:
            # Get class by class name
            try:
                peripheral_class = self.peripheral_modules[peripheral_configuration['module_name']].__dict__[peripheral_configuration['class_name']]
            except KeyError:
                raise ValueError("Could not find class '%s' in module '%s'" % (peripheral_configuration['class_name'], peripheral_configuration['module_name']))

            self.peripheral_manager.create_peripheral(peripheral_class, peripheral_configuration['peripheral_name'], peripheral_configuration['parameters'])
            print(peripheral_configuration)

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

                time.sleep(5)
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

    async def async_bootstrap(self):
        await self.rpc_test()


    async def rpc_test(self):
        print("running 1")
        r = await self.api_client.server_rpc().version()
        print(f'response 1 =========: {r}')

        print("running 2")
        r = await self.api_client.server_rpc().version()
        print(f'response 2 =========: {r}')

        print("running 3")
        r = await self.api_client.server_rpc().version()
        print(f'response 3 =========: {r}')
