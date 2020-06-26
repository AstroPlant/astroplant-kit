"""
Contains the main kit routines.
"""

# Make sure astroplant_kit is in the path
import datetime
import trio
import importlib
import logging
from astroplant_kit import peripheral
from astroplant_kit.controller import Controller
from astroplant_kit import errors
from .api import Client, RpcError, server_rpc
from .cache import Cache

from typing import Any, Optional, Dict, List, Iterable

logger = logging.getLogger("astroplant_kit.kit")


class Kit(object):
    def __init__(self, api_client: Client, debug_configuration, cache: Cache):
        self.halt = False
        self.startup_time = datetime.datetime.now()

        self._modules: Dict[str, Any] = {}
        self._controller: Optional[Controller] = None
        self.peripheral_manager = peripheral.PeripheralManager()
        self.api_client = api_client
        self.cache = cache

        self.initialise_debug(debug_configuration)

    def initialise_debug(self, debug_configuration: Any) -> None:
        """
        Initialise debugging options from the given debug configuration dictionary.

        :param debug_configuration: The debug configuration dictionary. Contains the
        configuration for a Display module to write to, as well as the level of logging
        required.
        """
        debug_level = debug_configuration["level"]
        if "peripheral_display" in debug_configuration:
            logger.info("Initialising peripheral debug display device.")
            peripheral_configuration = debug_configuration["peripheral_display"]

            configuration = (
                peripheral_configuration["configuration"]
                if ("configuration" in peripheral_configuration)
                else {}
            )

            self._import_modules([peripheral_configuration["module_name"]])
            peripheral_class = self._modules[
                peripheral_configuration["module_name"]
            ].__dict__[peripheral_configuration["class_name"]]
            peripheral_device = self.peripheral_manager.create_debug_display(
                peripheral_class, configuration
            )
            logger.info("Peripheral debug display device created.")

            log_handler = logging.StreamHandler(
                peripheral.DisplayDeviceStream(peripheral_device)  # type: ignore
            )
            log_handler.setLevel(debug_level)
            formatter = logging.Formatter("%(levelname)s\n%(message)s")
            log_handler.setFormatter(formatter)

            logger.addHandler(log_handler)
            logger.debug("Peripheral debug display device log handler added.")

    def _configure(self, configuration: Any) -> None:
        """
        Configure the kit.
        """
        # configuration = self.api_client.configuration_path.kit_configuration().body[0]

        self._configuration = configuration
        logger.info(f"Activating configuration {configuration['description']}")

        modules = set()
        modules.add(configuration["controllerSymbolLocation"])

        for peripheral_with_definition in configuration["peripherals"]:
            definition = peripheral_with_definition["definition"]
            modules.add(definition["symbolLocation"])

        self._import_modules(modules)
        self._configure_peripherals(configuration["peripherals"])

        controller_class = self._modules[
            configuration["controllerSymbolLocation"]
        ].__dict__[configuration["controllerSymbol"]]

        self._controller = controller_class(
            self.peripheral_manager, configuration["controlRules"]
        )

    def _import_modules(self, modules: Iterable[str]) -> None:
        """
        Import Python modules by name and add them to the dictionary
        of loaded peripheral modules.

        :param modules: An iterable with module names to import
        """
        for module_name in modules:
            module = importlib.import_module(module_name)
            self._modules[module_name] = module

    def _configure_peripherals(self, peripherals: Iterable[Any]) -> None:
        """
        Configure the kit peripherals using configuration dicts.

        :param peripheral_configurations: An iterable of peripheral configuration dicts.
        """
        for peripheral_with_definition in peripherals:
            peripheral = peripheral_with_definition["peripheral"]
            definition = peripheral_with_definition["definition"]

            module_name = definition["symbolLocation"]
            class_name = definition["symbol"]
            try:
                logger.debug(f"Initializing a peripheral of {module_name}.{class_name}")
                peripheral_class = self._modules[module_name].__dict__[class_name]
            except KeyError:
                raise ValueError(
                    "Could not find class '%s' in module '%s'"
                    % (class_name, module_name)
                )

            self.peripheral_manager.create_peripheral(
                peripheral_class,
                peripheral["id"],
                peripheral["name"],
                peripheral["configuration"],
            )

    def publish_data(self, data: peripheral.Data) -> None:
        """
        Publish a measurement to the back-end.

        :param measurement: The measurement to publish.
        """
        if data.is_measurement():
            assert data.measurement is not None
            self.api_client.publish_raw_measurement(data.measurement)
        elif data.is_aggregate_measurement():
            assert data.aggregate_measurement is not None
            self.api_client.publish_aggregate_measurement(data.aggregate_measurement)
        elif data.is_media():
            assert data.media is not None
            self.api_client.publish_media(data.media)

    async def run(self) -> None:
        """
        Run the kit.
        """
        logger.info("Starting.")

        try:
            await self.bootstrap()
        except KeyboardInterrupt:
            # Request halt
            self.halt = True
            print("HALT received...")

    async def _fetch_and_store_configuration(self) -> Any:
        logger.debug("Fetching kit configuration.")
        configuration = await self.api_client.server_rpc.get_active_configuration()
        self.cache.write_configuration(configuration)
        return configuration

    async def _fetch_and_store_quantity_types(self) -> List[server_rpc.QuantityType]:
        logger.debug("Fetching quantity types.")
        quantity_types = await self.api_client.server_rpc.get_quantity_types()
        self.cache.write_quantity_types(quantity_types)
        return quantity_types

    async def bootstrap(self) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.peripheral_manager.run_debug_display)

            try:
                configuration = await self._fetch_and_store_configuration()
            except RpcError as e:
                logger.warn(
                    f"Could not get configuration from server, trying cache. Original error: {e}"
                )
                try:
                    configuration = self.cache.read_configuration()
                except Exception as e:
                    logger.warn(
                        f"Could not get configuration from cache, stoping. Original error: {e}"
                    )
                    return

            if configuration is None:
                logger.error("No configuration set. Exiting.")
                raise errors.NoConfigurationError()

            try:
                quantity_types = await self._fetch_and_store_quantity_types()
            except RpcError as e:
                logger.warn(
                    f"Could not get quantity types from server, trying cache. Original error: {e}"
                )
                try:
                    quantity_types = self.cache.read_quantity_types()
                except Exception as e:
                    logger.warn(
                        f"Could not get quantity types from cache, stoping. Original error: {e}"
                    )
                    return

            self.peripheral_manager.set_quantity_types(
                map(
                    lambda qt: peripheral.QuantityType(
                        qt["id"],
                        qt["physicalQuantity"],
                        qt["physicalUnit"],
                        physical_unit_symbol=qt["physicalUnitSymbol"] or None,
                    ),
                    quantity_types,
                )
            )
            self._configure(configuration)
            data_rx = self.peripheral_manager.data_receiver()

            logger.debug("Starting peripheral manager.")
            nursery.start_soon(self.peripheral_manager.run)
            if self._controller is not None:
                logger.debug("Starting controller.")
                nursery.start_soon(self._controller.run)

            async for data in data_rx:
                self.publish_data(data)
