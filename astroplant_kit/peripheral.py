import abc
import sys
import collections
import datetime
import asyncio
import concurrent.futures
import logging
import collections

logger = logging.getLogger("AstroPlant")

class PeripheralManager(object):
    """
    A peripheral device manager; this manager keeps track of all peripherals,
    provides the ability to subscribe to peripheral device measurements,
    and provides the ability to run all runnable peripheral devices (such as
    sensors).
    """

    def __init__(self):
        self.peripherals = []
        self.subscribers = []
        self.event_loop = None

    def runnable_peripherals(self):
        """
        :return: An iterable of all runnable peripherals.
        """
        return filter(lambda peripheral: peripheral.RUNNABLE, self.peripherals)

    def run(self):
        """
        Run all runnable peripherals.
        """
        for peripheral in self.runnable_peripherals():
            asyncio.ensure_future(peripheral.run())

    def subscribe_physical_quantity(self, physical_quantity, callback):
        """
        Subscribe to messages concerning a specific physical quantity.

        :param physical_quantity: The name of the physical quantity for which the measurements are being subscribed to.
        :param callback: The callback to call with the measurement.
        """
        self.subscribe_predicate(lambda measurement: measurement.physical_quantity == physical_quantity, callback)

    def subscribe_predicate(self, predicate, callback):
        """
        Subscribe to messages that conform to a predicate.

        :param predicate: A function taking as input a measurement and returning true or false.
        :param callback: The callback to call with the measurement.
        """
        self.subscribers.append((predicate, callback))

    def _publish_handle(self, measurement):
        """
        Publish a measurement.

        :param measurement: The measurement to publish.
        """
        for (predicate, callback) in self.subscribers:
            if predicate(measurement):
                callback(measurement)

    def create_peripheral(self, peripheral_class, peripheral_object_name, peripheral_parameters):
        """
        Create and add a peripheral by its class name.

        :param peripheral_class: The class of the peripheral to add.
        :param peripheral_object_name: The name of the specific peripheral to add.
        :param peripheral_parameters: The instantiation parameters of the peripheral.
        :return: The created peripheral.
        """

        # Instantiate peripheral
        peripheral = peripheral_class(peripheral_object_name, self, **peripheral_parameters)

        # Set message publication handle
        peripheral._set_publish_handle(self._publish_handle)

        self.peripherals.append(peripheral)

        return peripheral

class Peripheral(object):
    """
    Abstract peripheral device base class.
    """

    #: Boolean indicating whether the peripheral is runnable.
    RUNNABLE = False

    #: Boolean indicating whether the peripheral can accept commands.
    COMMANDS = False

    def __init__(self, name, peripheral_device_manager):
        self.name = name
        self.manager = peripheral_device_manager
        self.logger = logger.getChild("peripheral").getChild(self.name)

    @abc.abstractmethod
    async def run(self):
        """
        Asynchronously run the peripheral device.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def do(self, command):
        """
        Asynchronously perform a command on the device.
        """
        raise NotImplementedError()

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

    #: Amount of time in seconds to wait between making measurements
    TIME_SLEEP_BETWEEN_MEASUREMENTS = 2.0

    #: Amount of time in seconds over which measurements are reduced before publishing them for storage
    TIME_REDUCE_MEASUREMENTS = 3600.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.measurements = []
        self.reducers = [
            {
                'name': 'count',
                'fn': lambda values: len(values)
            },
            {
                'name': 'average',
                'fn': lambda values: sum(values) / len(values),
            },
            {
                'name': 'minimum',
                'fn': lambda values: min(values)
            },
            {
                'name': 'maximum',
                'fn': lambda values: max(values)
            }
        ]

    async def run(self):
        await asyncio.wait([self._make_measurements(), self._reduce_measurements()])

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
                    asyncio.ensure_future(self._publish_measurement(m))
            else:
                # Add measurement to the sensor's measurement list (for later reduction)
                self.measurements.append(measurement)

                # Publish the measurement
                asyncio.ensure_future(self._publish_measurement(measurement))
            await asyncio.sleep(self.TIME_SLEEP_BETWEEN_MEASUREMENTS)

    async def _reduce_measurements(self):
        """
        Repeatedly reduce multiple measurements made to a single measurement.
        """
        while True:
            start_datetime = datetime.datetime.utcnow()

            await asyncio.sleep(self.TIME_REDUCE_MEASUREMENTS)

            self.logger.debug("Reducing measurements. %s" % len(self.measurements))

            # Group measurements by physical quantity and unit
            grouped_measurements = collections.defaultdict(list)
            for measurement in self.measurements:
                grouped_measurements[(measurement.physical_quantity, measurement.physical_unit)].append(measurement)

            # Emty the list
            self.measurements = []

            end_datetime = datetime.datetime.utcnow()

            # Produce list of reduced measurements
            try:
                reduced_measurements = [
                    aggregate
                    for (_, val) in grouped_measurements.items()
                    for aggregate in self.reduce(val, start_datetime, end_datetime)
                ]

            except Exception as e:
                self.logger.error("Could not reduce measurements: %s" % e)
                return

            # Publish reduced measurements
            for reduced_measurement in reduced_measurements:
                asyncio.ensure_future(self._publish_measurement(reduced_measurement))

    @abc.abstractmethod
    async def measure(self):
        raise NotImplementedError()

    async def _publish_measurement(self, measurement):
        self._publish_handle(measurement)

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

        return [
            Measurement(
                measurements[0].peripheral,
                measurements[0].physical_quantity,
                measurements[0].physical_unit,
                reducer['fn'](values),
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                aggregate_type=reducer['name']
            )
            for reducer in self.reducers
        ]

class Actuator(Peripheral):
    """
    Abstract actuator base class. Incomplete.
    """

    RUNNABLE = False

class Measurement(object):
    """
    Measurement class.

    Note that in general for non-aggregate measurements `start_datetime`
    need not be defined.
    """

    def __init__(
            self,
            peripheral,
            physical_quantity,
            physical_unit,
            value,
            start_datetime = None,
            end_datetime = None,
            aggregate_type = None
    ):
        self.peripheral = peripheral
        self.physical_quantity = physical_quantity
        self.physical_unit = physical_unit
        self.value = value
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime or datetime.datetime.utcnow()
        self.aggregate_type = aggregate_type

    def get_physical_unit_short(self):
        # Todo:
        # It's probably better to have a predefined registry of
        # physical quantity / unit combinations, that define
        # short names for units as well. This promotes consistency
        # across peripheral implementations as well.
        if (self.physical_unit == "Degrees Celsius"):
            return "Degrees C"
        elif (self.physical_unit == "Parts per million"):
            return "PPM"
        else:
            return self.physical_unit

    def __str__(self):
        return "%s-%s - %s %s %s: %s %s" % (self.start_datetime, self.end_datetime, self.aggregate_type, self.peripheral, self.physical_quantity, self.value, self.physical_unit)

class Display(Peripheral):
    """
    An abstract class for peripheral display devices. These devices can display strings.

    Todo: improve implementation, and add ability to somehow "rotate" messages, e.g. showing
    up-to-date measurement statistics.
    """
    RUNNABLE = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_message_queue = []
        self.log_condition = asyncio.Condition()
        self.measurements = {}

        # Subscribe to all measurements.
        self.manager.subscribe_predicate(lambda a: True, lambda m: self.handle_measurement(m))

    async def run(self):
        idx = 0

        while True:
            if len(self.log_message_queue) > 0:
                # Display log.
                msg = self.log_message_queue.pop(0)
                self.display(msg)
            elif len(self.measurements) > 0:
                # No logs; display a measurement.
                measurement = list(self.measurements.values())[idx]

                self.display("{quantity} ({peripheral})\n{value:.5g} {unit}".format(
                    peripheral = measurement.peripheral,
                    quantity = measurement.physical_quantity,
                    value = measurement.value,
                    unit = measurement.get_physical_unit_short()
                ))

                idx = (idx + 1) % len(self.measurements)

            if len(self.log_message_queue) == 0:
                # No remaining logs. Wait for a log notification,
                # or for 15 seconds, whichever comes first.
                await self.log_condition.acquire()

                try:
                    log_task = asyncio.ensure_future(self.log_condition.wait())
                    await asyncio.wait_for(log_task, timeout=15.0)
                except asyncio.TimeoutError:
                    pass
                finally:
                    # Check whether we have acquired the condition lock (in case of timeout, we don't have the lock)
                    if self.log_condition.locked():
                        self.log_condition.release()

    async def _log_notify(self):
        async with self.log_condition:
            self.log_condition.notify()

    def add_log_message(self, msg):
        """
        Add a log message to be displayed on the device.

        :param msg: The message to be displayed.
        """
        self.log_message_queue.append(msg)

        def notify():
            self.manager.event_loop.create_task(self._log_notify())

        if self.manager.event_loop is not None:
            self.manager.event_loop.call_soon_threadsafe(notify)

    @abc.abstractmethod
    def display(self, str):
        """
        Display a string on the device.

        :param str: The string to display.
        """
        raise NotImplementedError()

    def handle_measurement(self, m):
        """
        :param m: The measurement to handle.
        """
        self.measurements[(m.peripheral, m.physical_quantity)] = m

class DebugDisplay(Display):
    """
    A trivial peripheral display device implementation printing messages to the terminal.
    """
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

    def __init__(self, *args, storage_path, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_path = storage_path

        # Subscribe to all aggregate measurements.
        self.manager.subscribe_predicate(
            lambda m: m.aggregate_type is not None,
            self._store_measurement
        );

    def _store_measurement(self, measurement):
        # Import required modules.
        import csv
        import os

        measurement_dict = measurement.__dict__

        file_name = "%s-%s.csv" % (measurement.end_datetime.strftime("%Y%m%d"), measurement.physical_quantity)
        path = os.path.join(self.storage_path, file_name)

        # Check whether the file exists.
        exists = os.path.isfile(path)

        # Create file and directories if it does not exist yet.
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'a', newline='') as csv_file:
            # Get the measurement object field names.
            # Sort them to ensure csv headers have
            # consistent field ordering.
            field_names = sorted(measurement_dict.keys())
            writer = csv.DictWriter(csv_file, fieldnames=field_names)

            if not exists:
                # File is new: write csv header.
                writer.writeheader()
            writer.writerow(measurement_dict)
