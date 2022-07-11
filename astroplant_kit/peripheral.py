import logging
import abc
import collections
import datetime as dt
import uuid
import trio

from typing import (
    TypeVar,
    Any,
    Optional,
    Union,
    Iterable,
    Collection,
    Dict,
    List,
    Callable,
    Awaitable,
)

T = TypeVar("T")

logger = logging.getLogger("astroplant_kit.peripheral")


class Measurement:
    """
    Measurement class.
    """

    def __init__(
        self,
        peripheral: "Peripheral",
        quantity_type: "QuantityType",
        value: float,
        datetime: dt.datetime,
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


class AggregateMeasurement:
    """
    Aggregate measurement class.
    """

    def __init__(
        self,
        peripheral: "Peripheral",
        quantity_type: "QuantityType",
        values: Dict[str, float],
        start_datetime: dt.datetime,
        end_datetime: dt.datetime,
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


class Media:
    """
    A media object.
    """

    def __init__(
        self,
        peripheral: "Peripheral",
        name: str,
        type: str,
        data: bytes,
        metadata: Any,
        datetime: dt.datetime,
    ):
        self.id = uuid.uuid4()
        self.datetime = datetime
        self.peripheral = peripheral
        self.name = name
        self.type = type
        self.data = data
        self.metadata = metadata
        self.datetime = datetime


class PeripheralCommandResult(
    collections.namedtuple("PeripheralCommandResult", ["media"], defaults=[None])
):
    media: Optional[Media]


class Data:
    def __init__(self, data: Any):
        self.data = data

    @property
    def measurement(self) -> Optional[Measurement]:
        if isinstance(self.data, Measurement):
            return self.data
        else:
            return None

    def is_measurement(self) -> bool:
        return self.measurement is not None

    @property
    def aggregate_measurement(self) -> Optional[AggregateMeasurement]:
        if isinstance(self.data, AggregateMeasurement):
            return self.data
        else:
            return None

    def is_aggregate_measurement(self) -> bool:
        return self.aggregate_measurement is not None

    @property
    def media(self) -> Optional[Media]:
        if isinstance(self.data, Media):
            return self.data
        else:
            return None

    def is_media(self) -> bool:
        return self.media is not None


class Peripheral:
    """
    Abstract peripheral device base class.
    """

    #: Boolean indicating whether the peripheral is runnable.
    RUNNABLE = False

    #: Boolean indicating whether the peripheral can accept commands.
    COMMANDS = False

    def __init__(
        self, id: int, name: str, peripheral_device_manager: "PeripheralManager"
    ):
        self.id = id
        self.name = name
        self.manager = peripheral_device_manager
        self.logger = logger.getChild("peripheral").getChild(self.name)
        self._publish_handle: Optional[Callable[[Data], Awaitable[None]]] = None

    def create_media(
        self,
        name: str,
        type: str,
        data: bytes,
        metadata: Any,
        datetime: Optional[dt.datetime] = None,
    ) -> Media:
        return Media(
            self,
            name,
            type,
            data,
            metadata,
            datetime=datetime or dt.datetime.now(dt.timezone.utc),
        )

    @abc.abstractmethod
    async def run(self) -> None:
        """
        Asynchronously run the peripheral device.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def do(self, command: Any) -> PeripheralCommandResult:
        """
        Asynchronously perform a command on the device.

        :return: Optionally return a dictionary with an explicit media type, data, and metadata.
        """
        raise NotImplementedError()

    def get_id(self) -> int:
        return self.id

    def get_name(self) -> str:
        return self.name

    def _set_publish_handle(self, publish_handle: Callable) -> None:
        """
        Set the handle this device's data should be published to.
        """
        self._publish_handle = publish_handle

    async def _publish_data(self, data: Data) -> None:
        if self._publish_handle is None:
            raise Exception("Publish handle not set.")

        await self._publish_handle(data)

    def __str__(self) -> str:
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
        self,
        physical_quantity: str,
        physical_unit: str,
        value: float,
        datetime: Optional[dt.datetime] = None,
    ) -> Optional[Measurement]:
        return self.manager.create_raw_measurement(
            self, physical_quantity, physical_unit, value, datetime=datetime,
        )

    def create_aggregate_measurement(
        self,
        physical_quantity: str,
        physical_unit: str,
        values: Dict[str, float],
        start_datetime: dt.datetime,
        end_datetime: Optional[dt.datetime] = None,
    ) -> Optional[AggregateMeasurement]:
        return self.manager.create_aggregate_measurement(
            self,
            physical_quantity,
            physical_unit,
            values,
            start_datetime,
            end_datetime=end_datetime,
        )

    async def run(self) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._make_measurements)
            nursery.start_soon(self._reduce_measurements)

    async def _make_measurements(self) -> None:
        """
        Repeatedly make measurements.
        """
        while True:
            measurement = await self.measure()
            if isinstance(measurement, collections.abc.Iterable):
                # Add all measurements to the sensor's measurement list (for later reduction)
                self.measurements.extend(measurement)

                for m in measurement:
                    await self._publish_data(Data(m))
            else:
                # Add measurement to the sensor's measurement list (for later reduction)
                self.measurements.append(measurement)

                await self._publish_data(Data(measurement))
            await trio.sleep(self.measurement_interval)

    async def _reduce_measurements(self) -> None:
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
                await self._publish_data(Data(reduced_measurement))

    @abc.abstractmethod
    async def measure(self) -> "Union[Measurement, Iterable[Measurement]]":
        raise NotImplementedError()

    def reduce(
        self,
        measurements: List[Measurement],
        start_datetime: dt.datetime,
        end_datetime: dt.datetime,
    ) -> Optional[AggregateMeasurement]:
        """
        Reduce a list of measurements to aggregates.

        :param measurements: The list of measurements to reduce.
        :param start_datetime: The aggregate window start date and time.
        :param end_datetime: The aggregate window end date and time.
        :return: A list of aggregates.
        """
        if not measurements:
            return None

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

    self.reset_on_exit can be set by the task currently owning the control lock.
    Causes the context manager to resend the last command received prior to lock
    acquisition.
    """

    def __init__(self, peripheral: Peripheral, lock: trio.Lock):
        self._peripheral = peripheral
        self._lock = lock
        self._previous_command = None
        self._reset_command = None
        self._reset_on_exit = False

    @property
    def reset_on_exit(self) -> bool:
        return self._reset_on_exit

    @reset_on_exit.setter
    def reset_on_exit(self, reset_on_exit: bool) -> None:
        owner = self._lock.statistics().owner
        if owner is None or trio.hazmat.current_task() is not owner:
            raise Exception(
                "Calling task is not the lock owner. It must be the lock owner to change reset-on-exit behaviour."
            )
        self._reset_on_exit = reset_on_exit

    async def __aenter__(self) -> Callable[[Any], Awaitable[PeripheralCommandResult]]:
        """
        :return: A handle to an async function to send commands to the peripheral device.
        """
        return await self.acquire()

    async def __aexit__(self, _type, _value, _traceback) -> None:
        if self._reset_on_exit:
            await self.reset_and_release()
        else:
            self.release()

    async def acquire(self) -> Callable[[Any], Awaitable[PeripheralCommandResult]]:
        """
        Acquire the lock, blocking if necessary.
        :return: A handle to an async function to send commands to the peripheral device.
        This handle should only be used for as long as the lock is held.
        """
        await self._lock.acquire()
        self._reset_command = self._previous_command
        return self._do

    def acquire_nowait(self) -> Callable[[Any], Awaitable[PeripheralCommandResult]]:
        """
        Attempt to acquire the underlying lock, without blocking.
        :return: A handle to an async function to send commands to the peripheral device.
        This handle should only be used for as long as the lock is held.
        :raises: trio.WouldBlock if the lock is held.
        """
        self._lock.acquire_nowait()
        self._reset_command = self._previous_command
        return self._do

    def release(self) -> None:
        """
        Release the underlying lock.
        """
        self._lock.release()
        self._reset_on_exit = False

    async def reset_and_release(self) -> None:
        """
        Reset to the last command received prior to lock acquisition,
        then release the underlying lock.
        """
        try:
            if self._reset_command is not None:
                await self._do(self._reset_command)
                self._previous_command = self._reset_command
        finally:
            self.release()

    async def _do(self, command: Any) -> PeripheralCommandResult:
        self._previous_command = command
        return await self._peripheral.do(command)


class QuantityType(object):
    """
    Quantity type class.

    The quantity type id is given by the server.
    """

    def __init__(
        self,
        id: int,
        physical_quantity: str,
        physical_unit: str,
        physical_unit_symbol: str = None,
    ):
        self.id = id
        self.physical_quantity = physical_quantity
        self.physical_unit = physical_unit
        self.physical_unit_symbol = physical_unit_symbol

    @property
    def physical_unit_short(self) -> str:
        if self.physical_unit_symbol:
            return self.physical_unit_symbol
        else:
            return self.physical_unit


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

    async def _update_measurements(self) -> None:
        """
        Listen to new measurements and handle them.
        """
        async for measurement in self.manager.measurement_receiver():
            self._measurements[
                (measurement.peripheral, measurement.quantity_type.physical_quantity,)
            ] = measurement

    async def run(self) -> None:
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

    async def _condition_notify(self) -> None:
        async with self._condition:
            self._condition.notify()

    def add_log_message(self, msg: str) -> None:
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
    def display(self, message: str) -> None:
        """
        Display a string on the device.

        :param str: The string to display.
        """
        raise NotImplementedError()


class DebugDisplay(Display):
    """
    A trivial peripheral display device implementation printing messages to the terminal.
    """

    def __init__(self, *args, configuration: Any):
        super().__init__(*args)

    def display(self, message: str) -> None:
        print("Debug Display: %s" % message)


class BlackHoleDisplay(Display):
    """
    A trivial peripheral display device implementation ignoring all display messages.
    """

    def display(self, message: str) -> None:
        pass


class DisplayDeviceStream(object):
    """
    A stream class to be used for loggers logging to peripheral display devices.
    """

    def __init__(self, peripheral_display_device: Display):
        self.peripheral_display_device = peripheral_display_device
        self.str = ""

    def write(self, str: str) -> None:
        self.str += str

    def flush(self) -> None:
        self.peripheral_display_device.add_log_message(self.str)
        self.str = ""


class LocalDataLogger(Actuator):
    """
    A virtual peripheral device writing observations to internal storage.
    """

    RUNNABLE = True

    def __init__(self, *args, configuration: Any):
        super().__init__(*args)
        self.storage_path = configuration["storagePath"]

    async def run(self) -> None:
        """
        Listen to new aggregate measurements and store them.
        """
        async for aggregate_measurement in self.manager.aggregate_measurement_receiver():
            self._store_aggregate_measurement(aggregate_measurement)

    def _store_aggregate_measurement(
        self, aggregate_measurement: AggregateMeasurement
    ) -> None:
        # Import required modules.
        import csv
        import os

        aggregate_measurement_dict = {
            "start_datetime": aggregate_measurement.start_datetime,
            "end_datetime": aggregate_measurement.end_datetime,
            "peripheral": aggregate_measurement.peripheral.get_id(),
            "peripheral_name": aggregate_measurement.peripheral.name,
            "physical_quantity": aggregate_measurement.quantity_type.physical_quantity,
            "physical_unit": aggregate_measurement.quantity_type.physical_unit,
            "values": aggregate_measurement.values,
        }

        file_name = "%s-%s.csv" % (
            aggregate_measurement.end_datetime.strftime("%Y%m%d"),
            aggregate_measurement.quantity_type.physical_quantity,
        )
        path = os.path.join(self.storage_path, file_name)

        file_exists = os.path.isfile(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "a", newline="") as csv_file:
            # Get the aggregate measurement object field names.
            # Sort them to ensure csv headers have
            # consistent field ordering.
            field_names = sorted(aggregate_measurement_dict.keys())
            writer = csv.DictWriter(csv_file, fieldnames=field_names)

            if not file_exists:
                # File is new: write csv header.
                writer.writeheader()
            writer.writerow(aggregate_measurement_dict)


DataFilterMap = Callable[[Data], Optional[T]]


class PeripheralManager(object):
    """
    A peripheral device manager; this manager keeps track of all peripherals,
    provides the ability to subscribe to peripheral device measurements,
    and provides the ability to run all runnable peripheral devices (such as
    sensors).
    """

    def __init__(self):
        self._peripherals: Dict[str, Peripheral] = {}
        self._peripheral_control_locks: Dict[Peripheral, PeripheralControl] = {}
        self._debug_display: Peripheral = None
        self._data_txs: List[Tuple[trio.MemorySendChannel, DataFilterMap]] = []
        self.event_loop = None
        self.quantity_types: List[QuantityType] = []

        (data_tx, data_rx) = trio.open_memory_channel[Data](64)
        self._data_tx = data_tx
        self._data_rx = data_rx

    def set_quantity_types(self, quantity_types: Iterable[QuantityType]) -> None:
        """
        Set the quantity types known to the server.
        """
        self.quantity_types = list(quantity_types)

    def _receiver(
        self, filter_map: DataFilterMap[T], buffer: int = 10,
    ) -> "trio.MemoryReceiveChannel[T]":
        """
        Create and get a data receiver channel.

        If the receiver does not keep up with the messages, the channel will
        be dropped.
        """
        tx, rx = trio.open_memory_channel[T](buffer)
        self._data_txs.append((tx, filter_map))

        return rx

    def measurement_receiver(
        self, buffer: int = 10
    ) -> "trio.MemoryReceiveChannel[Measurement]":
        def filter_map(d: Data) -> Optional[Measurement]:
            return d.measurement

        return self._receiver(filter_map, buffer)

    def aggregate_measurement_receiver(
        self, buffer: int = 10
    ) -> "trio.MemoryReceiveChannel[AggregateMeasurement]":
        def filter_map(d: Data) -> Optional[AggregateMeasurement]:
            return d.aggregate_measurement

        return self._receiver(filter_map, buffer)

    def media_receiver(self, buffer: int = 10) -> "trio.MemoryReceiveChannel[Media]":
        def filter_map(d: Data) -> Optional[Media]:
            return d.media

        return self._receiver(filter_map, buffer)

    def data_receiver(self, buffer: int = 10) -> "trio.MemoryReceiveChannel[Data]":
        def filter_map(d: Data) -> Data:
            return d

        return self._receiver(filter_map, buffer)

    def _get_quantity_type(
        self, physical_quantity: str, physical_unit: str
    ) -> Optional[QuantityType]:
        for qt in self.quantity_types:
            if (
                qt.physical_quantity == physical_quantity
                and qt.physical_unit == physical_unit
            ):
                return qt
        return None

    def create_raw_measurement(
        self,
        peripheral: Peripheral,
        physical_quantity: str,
        physical_unit: str,
        value: float,
        datetime: Optional[dt.datetime] = None,
    ) -> Optional[Measurement]:
        quantity_type = self._get_quantity_type(physical_quantity, physical_unit)
        if quantity_type is None:
            return None

        return Measurement(
            peripheral,
            quantity_type,
            value,
            datetime=datetime or dt.datetime.now(dt.timezone.utc),
        )

    def create_aggregate_measurement(
        self,
        peripheral: Peripheral,
        physical_quantity: str,
        physical_unit: str,
        values: Dict[str, float],
        start_datetime: dt.datetime,
        end_datetime: Optional[dt.datetime] = None,
    ) -> Optional[AggregateMeasurement]:
        quantity_type = self._get_quantity_type(physical_quantity, physical_unit)
        if quantity_type is None:
            return None

        return AggregateMeasurement(
            peripheral,
            quantity_type,
            values,
            start_datetime=start_datetime,
            end_datetime=end_datetime or dt.datetime.now(dt.timezone.utc),
        )

    @property
    def runnable_peripherals(self) -> Iterable[Peripheral]:
        """
        :return: An iterable of all runnable peripherals.
        """
        return filter(
            lambda peripheral: peripheral.RUNNABLE, self._peripherals.values()
        )

    @property
    def peripherals(self) -> Collection[Peripheral]:
        """
        :return: An iterable of all peripherals.
        """
        return self._peripherals.values()

    def control(self, peripheral: Peripheral) -> PeripheralControl:
        """
        :return: An async context manager for getting exclusive control access to a peripheral.
        """
        if peripheral not in self._peripheral_control_locks:
            self._peripheral_control_locks[peripheral] = PeripheralControl(
                peripheral, trio.Lock()
            )
        return self._peripheral_control_locks[peripheral]

    def get_peripheral_by_name(self, name: str) -> Optional[Peripheral]:
        """
        :param name: The name of the peripheral to get.
        :return: Get the peripheral with the given name, or None if no such
        peripheral exists.
        """
        if name in self._peripherals:
            return self._peripherals[name]
        else:
            return None

    async def run_debug_display(self) -> None:
        """
        Run the debug display device.
        """
        if self._debug_display:
            await self._debug_display.run()

    async def run(self) -> None:
        """
        Run all runnable peripherals and broadcast data.
        """
        async with trio.open_nursery() as nursery:
            for peripheral in self.runnable_peripherals:
                nursery.start_soon(peripheral.run)

            async for data in self._data_rx:
                await self._broadcast(data)

    async def _publish_handle(self, data: Data) -> None:
        """
        Handle to pass to peripherals for publishing data.

        :param data: The data to publish.
        """
        await self._data_tx.send(data)

    async def _broadcast(self, data: Data) -> None:
        """
        Broadcast data to listener channels.

        :param data: The data to broadcast.
        """
        for i in reversed(range(len(self._data_txs))):
            (tx, filter_map) = self._data_txs[i]
            d = filter_map(data)
            if d is None:
                continue
            try:
                tx.send_nowait(d)
            except (trio.WouldBlock, trio.EndOfChannel):
                await tx.aclose()
                del self._data_txs[i]

    def create_peripheral(
        self,
        peripheral_class: Callable[..., "Peripheral"],
        id: int,
        name: str,
        configuration: Any,
    ) -> Peripheral:
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

    def create_debug_display(
        self, peripheral_class: Callable[..., Display], configuration: Any
    ) -> Display:
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
