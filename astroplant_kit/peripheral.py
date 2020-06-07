import logging
import abc
import sys
import collections
import datetime as dt
import uuid
import trio
import collections

from typing import Dict

logger = logging.getLogger("astroplant_kit.peripheral")


class PeripheralCommandResult(
    collections.namedtuple(
        "PeripheralCommandResult", ["media_type", "data", "metadata"]
    )
):
    media_type: str
    data: bytes
    metadata: Dict


class PeripheralManager(object):
    """
    A peripheral device manager; this manager keeps track of all peripherals,
    provides the ability to subscribe to peripheral device measurements,
    and provides the ability to run all runnable peripheral devices (such as
    sensors).
    """

    def __init__(self):
        self._peripherals: Dict[str, Peripheral] = {}
        self._peripheral_control_locks: Dict[Peripheral, trio.Lock] = {}
        self._debug_display: Peripheral = None
        self.measurement_txs = []
        self.event_loop = None
        self.quantity_types = []

        (measurement_tx, measurement_rx) = trio.open_memory_channel(64)
        self._measurement_tx = measurement_tx
        self._measurement_rx = measurement_rx

    def set_quantity_types(self, quantity_types):
        """
        Set the quantity types known to the server.
        """
        self.quantity_types = list(
            map(
                lambda qt: QuantityType(
                    qt["id"],
                    qt["physicalQuantity"],
                    qt["physicalUnit"],
                    physical_unit_symbol=qt["physicalUnitSymbol"] or None,
                ),
                quantity_types,
            )
        )

    def measurements_receiver(self, buffer=10) -> trio.MemoryReceiveChannel:
        """
        Create and get a measurement receiver channel.

        If the receiver does not keep up with the messages, the channel will
        be dropped.
        """
        tx, rx = trio.open_memory_channel(buffer)
        self.measurement_txs.append(tx)

        return rx

    def _get_quantity_type(self, physical_quantity, physical_unit):
        for qt in self.quantity_types:
            if (
                qt.physical_quantity == physical_quantity
                and qt.physical_unit == physical_unit
            ):
                return qt
        return None

    def create_raw_measurement(
        self, peripheral, physical_quantity, physical_unit, value, datetime=None,
    ):
        quantity_type = self._get_quantity_type(physical_quantity, physical_unit)
        if quantity_type == None:
            return None

        return Measurement(
            peripheral,
            quantity_type,
            value,
            datetime=datetime or dt.datetime.now(dt.timezone.utc),
        )

    def create_aggregate_measurement(
        self,
        peripheral,
        physical_quantity,
        physical_unit,
        values,
        start_datetime=None,
        end_datetime=None,
    ):
        quantity_type = self._get_quantity_type(physical_quantity, physical_unit)
        if quantity_type == None:
            return None

        return AggregateMeasurement(
            peripheral,
            quantity_type,
            values,
            start_datetime=start_datetime,
            end_datetime=end_datetime or dt.datetime.now(dt.timezone.utc),
        )

    @property
    def runnable_peripherals(self):
        """
        :return: An iterable of all runnable peripherals.
        """
        return filter(
            lambda peripheral: peripheral.RUNNABLE, self._peripherals.values()
        )

    @property
    def peripherals(self):
        """
        :return: An iterable of all peripherals.
        """
        return self._peripherals.values()

    def control(self, peripheral):
        """
        :return: An async context manager for getting exclusive control access to a peripheral.
        """
        if peripheral not in self._peripheral_control_locks:
            self._peripheral_control_locks[peripheral] = trio.Lock()
        return PeripheralControl(peripheral, self._peripheral_control_locks[peripheral])

    def get_peripheral_by_name(self, name):
        """
        :param name: The name of the peripheral to get.
        :return: Get the peripheral with the given name, or None if no such
        peripheral exists.
        """
        if name in self._peripherals:
            return self._peripherals[name]
        else:
            return None

    async def run_debug_display(self):
        """
        Run the debug display device.
        """
        if self._debug_display:
            await self._debug_display.run()

    async def run(self):
        """
        Run all runnable peripherals and broadcast measurements.
        """
        async with trio.open_nursery() as nursery:
            for peripheral in self.runnable_peripherals:
                nursery.start_soon(peripheral.run)

            async for measurement in self._measurement_rx:
                await self._broadcast(measurement)

    async def _publish_handle(self, measurement):
        """
        Publish a measurement.

        :param measurement: The measurement to publish.
        """
        await self._measurement_tx.send(measurement)

    async def _broadcast(self, measurement):
        for i in reversed(range(len(self.measurement_txs))):
            tx = self.measurement_txs[i]
            try:
                tx.send_nowait(measurement)
            except (trio.WouldBlock, trio.EndOfChannel):
                await tx.aclose()
                del self.measurement_txs[i]

    def create_peripheral(self, peripheral_class, id, name, configuration):
        """
        Create and add a peripheral by its class name.

        :param peripheral_class: The class of the peripheral to add.
        :param id: The id of the specific peripheral to add.
        :param name: The name of the peripheral.
        :param configuration: The instantiation paramaters (configuration) of the peripheral.
        :return: The created peripheral.
        """
        logger.debug("creating peripheral %s", name)

        # Instantiate peripheral
        peripheral = peripheral_class(id, name, self, configuration=configuration)

        # Set message publication handle
        peripheral._set_publish_handle(self._publish_handle)

        self._peripherals[name] = peripheral

        return peripheral

    def create_debug_display(self, peripheral_class, configuration):
        """
        Create a peripheral used for displaying debug messages.

        :param peripheral_class: The class of the peripheral to add.
        :param configuration: The instantiation parameters (configuration) of the peripheral.
        """
        logger.debug("creating debug display peripheral")

        # Instantiate peripheral
        peripheral = peripheral_class(
            -1, "debug-display-device", self, configuration=configuration
        )

        # Set message publication handle
        peripheral._set_publish_handle(self._publish_handle)

        self._debug_display = peripheral

        return peripheral


class Peripheral(object):
    """
    Abstract peripheral device base class.
    """

    #: Boolean indicating whether the peripheral is runnable.
    RUNNABLE = False

    #: Boolean indicating whether the peripheral can accept commands.
    COMMANDS = False

    def __init__(self, id, name, peripheral_device_manager):
        self.id = id
        self.name = name
        self.manager = peripheral_device_manager
        self.logger = logger.getChild("peripheral").getChild(self.name)
        self._publish_handle = lambda *args: None

    @abc.abstractmethod
    async def run(self):
        """
        Asynchronously run the peripheral device.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def do(self, command) -> PeripheralCommandResult:
        """
        Asynchronously perform a command on the device.

        :return: Optionally return a dictionary with an explicit media type, data, and metadata.
        """
        raise NotImplementedError()

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def _set_publish_handle(self, publish_handle):
        """
        Set the handle this device's measurements should be published to.
        """
        self._publish_handle = publish_handle

    def __str__(self):
        return self.name


class Sensor(Peripheral):
    """
    Abstract sensor base class.
    """

    RUNNABLE = True

    #: Default interval in seconds to wait between taking measurements.
    DEFAULT_MEASUREMENT_INTERVAL = 60

    #: Default interval in seconds over which measurements are aggregated.
    DEFAULT_AGGREGATE_INTERVAL = 60 * 30

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.measurement_interval = self.DEFAULT_MEASUREMENT_INTERVAL
        self.aggregate_interval = self.DEFAULT_AGGREGATE_INTERVAL

        self.measurements = []
        self.reducers = [
            {"name": "count", "fn": lambda values: len(values)},
            {"name": "average", "fn": lambda values: sum(values) / len(values)},
            {"name": "minimum", "fn": lambda values: min(values)},
            {"name": "maximum", "fn": lambda values: max(values)},
        ]

    def create_raw_measurement(
        self, physical_quantity, physical_unit, value, datetime=None,
    ):
        return self.manager.create_raw_measurement(
            self, physical_quantity, physical_unit, value, datetime=datetime,
        )

    def create_aggregate_measurement(
        self,
        physical_quantity,
        physical_unit,
        values,
        start_datetime=None,
        end_datetime=None,
    ):
        return self.manager.create_aggregate_measurement(
            self,
            physical_quantity,
            physical_unit,
            values,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._make_measurements)
            nursery.start_soon(self._reduce_measurements)

    async def _make_measurements(self):
        """
        Repeatedly make measurements.
        """
        while True:
            measurement = await self.measure()
            if isinstance(measurement, collections.Iterable):
                # Add all measurements to the sensor's measurement list (for later reduction)
                self.measurements.extend(measurement)

                # Publish each measurement
                for m in measurement:
                    await self._publish_measurement(m)
            else:
                # Add measurement to the sensor's measurement list (for later reduction)
                self.measurements.append(measurement)

                # Publish the measurement
                await self._publish_measurement(measurement)
            await trio.sleep(self.measurement_interval)

    async def _reduce_measurements(self):
        """
        Repeatedly reduce multiple measurements made to a single measurement.
        """
        while True:
            start_datetime = dt.datetime.now(dt.timezone.utc)

            await trio.sleep(self.aggregate_interval)

            self.logger.debug("Reducing measurements. %s" % len(self.measurements))

            # Group measurements by physical quantity and unit
            grouped_measurements = collections.defaultdict(list)
            for measurement in self.measurements:
                grouped_measurements[
                    (
                        measurement.quantity_type.physical_quantity,
                        measurement.quantity_type.physical_unit,
                    )
                ].append(measurement)

            # Empty the list
            self.measurements = []

            end_datetime = dt.datetime.now(dt.timezone.utc)

            # Produce list of reduced measurements
            try:
                reduced_measurements = [
                    self.reduce(val, start_datetime, end_datetime)
                    for (_, val) in grouped_measurements.items()
                ]

            except Exception as e:
                self.logger.error("Could not reduce measurements: %s" % e)
                return

            # Publish reduced measurements
            for reduced_measurement in reduced_measurements:
                await self._publish_measurement(reduced_measurement)

    @abc.abstractmethod
    async def measure(self):
        raise NotImplementedError()

    async def _publish_measurement(self, measurement):
        await self._publish_handle(measurement)

    def reduce(self, measurements, start_datetime, end_datetime):
        """
        Reduce a list of measurements to aggregates.

        :param measurements: The list of measurements to reduce.
        :param start_datetime: The aggregate window start date and time.
        :param end_datetime: The aggregate window end date and time.
        :return: A list of aggregates.
        """
        if not measurements:
            return []

        values = list(map(lambda m: m.value, measurements))
        aggregates = {
            reducer["name"]: reducer["fn"](values) for reducer in self.reducers
        }

        return self.manager.create_aggregate_measurement(
            measurements[0].peripheral,
            measurements[0].quantity_type.physical_quantity,
            measurements[0].quantity_type.physical_unit,
            aggregates,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )


class Actuator(Peripheral):
    """
    Abstract actuator base class. Incomplete.
    """

    RUNNABLE = False


class PeripheralControl(object):
    """
    A peripheral control object.
    
    Used to give exclusive control access over a peripheral device.

    Can be used with the given methods, or as an async context manager. On lock
    acquisition, returns a handle to an async function to send commands to the
    peripheral device.
    """

    def __init__(self, peripheral: Peripheral, lock: trio.Lock):
        self._peripheral = peripheral
        self._lock = lock

    async def __aenter__(self):
        """
        :return: A handle to an async function to send commands to the peripheral device.
        """
        await self._lock.acquire()
        return self._do

    async def __aexit__(self, _type, _value, _traceback):
        self._lock.release()

    async def acquire(self):
        """
        Acquire the lock, blocking if necessary.
        :return: A handle to an async function to send commands to the peripheral device.
        This handle should only be used for as long as the lock is held.
        """
        await self._lock.acquire()
        return self._do

    def acquire_nowait(self):
        """
        Attempt to acquire the underlying lock, without blocking.
        :return: A handle to an async function to send commands to the peripheral device.
        This handle should only be used for as long as the lock is held.
        :raises: trio.WouldBlock if the lock is held.
        """
        self._lock.acquire_nowait()
        return self._do

    def release(self):
        """
        Release the underlying lock.
        """
        self._lock.release()

    async def _do(self, command) -> PeripheralCommandResult:
        return await self._peripheral.do(command)


class QuantityType(object):
    """
    Quantity type class.

    The quantity type id is given by the server.
    """

    def __init__(self, id, physical_quantity, physical_unit, physical_unit_symbol=None):
        self.id = id
        self.physical_quantity = physical_quantity
        self.physical_unit = physical_unit
        self.physical_unit_symbol = physical_unit_symbol

    @property
    def physical_unit_short(self):
        if self.physical_unit_symbol:
            return self.physical_unit_symbol
        else:
            return self.physical_unit


class Measurement(object):
    """
    Measurement class.
    """

    def __init__(
        self, peripheral, quantity_type, value, datetime=None,
    ):
        self.id = uuid.uuid4()
        self.peripheral = peripheral
        self.quantity_type = quantity_type
        self.value = value
        self.datetime = datetime

    def __str__(self):
        return "%s - %s %s: %s %s [%s]" % (
            self.datetime,
            self.peripheral,
            self.quantity_type.physical_quantity,
            self.value,
            self.quantity_type.physical_unit,
            self.id,
        )


class AggregateMeasurement(object):
    """
    Aggregate measurement class.
    """

    def __init__(
        self, peripheral, quantity_type, values, start_datetime=None, end_datetime=None,
    ):
        self.id = uuid.uuid4()
        self.peripheral = peripheral
        self.quantity_type = quantity_type
        self.values = values
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def __str__(self):
        return "%s-%s - %s %s %s [%s]: %s" % (
            self.start_datetime,
            self.end_datetime,
            self.peripheral,
            self.quantity_type.physical_quantity,
            self.quantity_type.physical_unit,
            self.id,
            self.values,
        )


class Display(Peripheral):
    """
    An abstract class for peripheral display devices. These devices can display strings.

    Todo: improve implementation, and add ability to somehow "rotate" messages, e.g. showing
    up-to-date measurement statistics.
    """

    RUNNABLE = True

    def __init__(self, *args):
        super().__init__(*args)
        self._log_message_queue = []
        self._measurements = {}
        self._trio_token = None
        self._condition = trio.Condition()

    async def _update_measurements(self):
        """
        Listen to new measurements and handle them.
        """
        async for m in self.manager.measurements_receiver():
            if isinstance(m, Measurement):
                self._measurements[
                    (m.peripheral, m.quantity_type.physical_quantity)
                ] = m

    async def run(self):
        idx = 0

        self._trio_token = trio.hazmat.current_trio_token()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._update_measurements)

            while True:
                if len(self._log_message_queue) > 0:
                    # Display log.
                    msg = self._log_message_queue.pop(0)
                    self.display(msg)
                elif len(self._measurements) > 0:
                    # No logs; display a measurement.
                    measurement = list(self._measurements.values())[idx]

                    self.display(
                        "{quantity} ({peripheral})\n{value:.5g} {unit}".format(
                            peripheral=measurement.peripheral,
                            quantity=measurement.quantity_type.physical_quantity,
                            value=measurement.value,
                            unit=measurement.quantity_type.physical_unit_short,
                        )
                    )

                    idx = (idx + 1) % len(self._measurements)

                if len(self._log_message_queue) > 0:
                    await trio.sleep(5)
                else:
                    # No remaining logs. Wait for a log notification,
                    # or for 15 seconds, whichever comes first.
                    with trio.move_on_after(15):
                        async with self._condition:
                            await self._condition.wait()

    async def _condition_notify(self):
        async with self._condition:
            self._condition.notify()

    def add_log_message(self, msg):
        """
        Add a log message to be displayed on the device.

        :param msg: The message to be displayed.
        """
        import threading

        self._log_message_queue.append(msg)
        if self._trio_token is not None:
            # Logging could be called from within a Trio task or an external
            # thread. For simplicity, always spawn a new thread.
            def t():
                trio.from_thread.run(
                    self._condition_notify, trio_token=self._trio_token
                )

            thread = threading.Thread(target=t, daemon=True)
            thread.start()

    @abc.abstractmethod
    def display(self, str):
        """
        Display a string on the device.

        :param str: The string to display.
        """
        raise NotImplementedError()


class DebugDisplay(Display):
    """
    A trivial peripheral display device implementation printing messages to the terminal.
    """

    def __init__(self, *args, configuration):
        super().__init__(*args)

    def display(self, str):
        print("Debug Display: %s" % str)


class BlackHoleDisplay(Display):
    """
    A trivial peripheral display device implementation ignoring all display messages.
    """

    def display(self, str):
        pass


class DisplayDeviceStream(object):
    """
    A stream class to be used for loggers logging to peripheral display devices.
    """

    def __init__(self, peripheral_display_device):
        self.peripheral_display_device = peripheral_display_device
        self.str = ""

    def write(self, str):
        self.str += str

    def flush(self):
        self.peripheral_display_device.add_log_message(self.str)
        self.str = ""


class LocalDataLogger(Actuator):
    """
    A virtual peripheral device writing observations to internal storage.
    """

    RUNNABLE = True

    def __init__(self, *args, configuration):
        super().__init__(*args)
        self.storage_path = configuration["storagePath"]

    async def run(self):
        """
        Listen to new measurements and store them.
        """
        async for m in self.manager.measurements_receiver():
            if isinstance(m, AggregateMeasurement):
                self._store_measurement(m)

    def _store_measurement(self, measurement):
        # Import required modules.
        import csv
        import os

        measurement_dict = {
            "start_datetime": measurement.start_datetime,
            "end_datetime": measurement.end_datetime,
            "peripheral": measurement.peripheral.get_id(),
            "peripheral_name": measurement.peripheral.name,
            "physical_quantity": measurement.quantity_type.physical_quantity,
            "physical_unit": measurement.quantity_type.physical_unit,
            "values": measurement.values,
        }

        file_name = "%s-%s.csv" % (
            measurement.end_datetime.strftime("%Y%m%d"),
            measurement.quantity_type.physical_quantity,
        )
        path = os.path.join(self.storage_path, file_name)

        # Check whether the file exists.
        exists = os.path.isfile(path)

        # Create file and directories if it does not exist yet.
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "a", newline="") as csv_file:
            # Get the measurement object field names.
            # Sort them to ensure csv headers have
            # consistent field ordering.
            field_names = sorted(measurement_dict.keys())
            writer = csv.DictWriter(csv_file, fieldnames=field_names)

            if not exists:
                # File is new: write csv header.
                writer.writeheader()
            writer.writerow(measurement_dict)
