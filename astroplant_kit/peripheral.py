import abc
import sys
import collections
import datetime
import asyncio
import logging
import collections

logger = logging.getLogger("AstroPlant")

class PeripheralManager(object):
    def __init__(self):
        self.peripherals = []
        self.subscribers = []

    def runnable_peripherals(self):
        """
        :return: An iterable of all runnable peripherals.
        """
        return filter(lambda peripheral: peripheral.RUNNABLE, self.peripherals)

    async def run(self):
        """
        Run all runnable peripherals.
        """
        await asyncio.wait([peripheral.run() for peripheral in self.runnable_peripherals()])

    def subscribe_physical_quantity(self, physical_quantity, callback):
        """
        Subscribe to messages concerning a specific physical quantity.

        :param physical_quantity: The name of the physical quantity for which the measurements are being subscribed to.
        :param callback: The callback to call with the measurement.
        """
        self.subscribe_predicate(lambda measurement: measurement.get_physical_quantity() == physical_quantity, callback)

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
        peripheral = peripheral_class(peripheral_object_name, **peripheral_parameters)

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

    def __init__(self, name):
        self.name = name
        self.logger = logger.getChild("peripheral").getChild(self.name)

    @abc.abstractmethod
    async def run(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def do(self, command):
        raise NotImplementedError()

    def get_name(self):
        return self.name

    def _set_publish_handle(self, publish_handle):
        self._publish_handle = publish_handle

    def __str__(self):
        return self.name

class Sensor(Peripheral):
    """
    Abstract sensor base class.
    """

    RUNNABLE = True

    #: Amount of time in seconds to wait between making measurements
    TIME_SLEEP_BETWEEN_MEASUREMENTS = 0.5

    #: Amount of time in seconds over which measurements are reduced before publishing them for storage
    TIME_REDUCE_MEASUREMENTS = 10.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.measurements = []

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
            self.logger.debug("Reducing measurements. %s" % len(self.measurements))

            # Group measurements by physical quantity and unit
            grouped_measurements = collections.defaultdict(list)
            for measurement in self.measurements:
                grouped_measurements[(measurement.get_physical_quantity(), measurement.get_physical_unit())].append(measurement)

            # Empty list
            self.measurements = []

            # Produce list of reduced measurements
            try:
                reduced_measurements = [self.reduce(val) for (key, val) in grouped_measurements.items()]
            except Exception as e:
                self.logger.error("Could not reduce measurements: %s" % e)
                return

            # Publish reduced measurements
            for reduced_measurement in reduced_measurements:
                self.logger.debug("Publish: %s" % reduced_measurement)

            await asyncio.sleep(self.TIME_REDUCE_MEASUREMENTS)

    @abc.abstractmethod
    async def measure(self):
        raise NotImplementedError()

    async def _publish_measurement(self, measurement):
        self._publish_handle(measurement)

    def reduce(self, measurements):
        """
        Reduce a list of measurements to a single measurement.

        :param measurements: The list of measurements to reduce.
        :return: A single measurement, or None
        """
        if not measurements:
            return None

        values = list(map(lambda m: m.get_value(), measurements))
        avg_value = sum(values) / len(values)

        # Make a new measurement based on the old measurements
        measurement = measurements[0]
        return Measurement(measurement.get_peripheral(), measurement.get_physical_quantity(), measurement.get_physical_unit(), avg_value)

class Measurement(object):
    """
    Measurement class.
    """

    def __init__(self, peripheral, physical_quantity, physical_unit, value):
        self.peripheral = peripheral
        self.physical_quantity = physical_quantity
        self.physical_unit = physical_unit
        self.value = value
        self.date_time = datetime.datetime.utcnow()

    def get_peripheral(self):
        return self.peripheral

    def get_physical_quantity(self):
        return self.physical_quantity

    def get_physical_unit(self):
        return self.physical_unit

    def get_value(self):
        return self.value

    def get_date_time(self):
        return self.date_time

    def __str__(self):
        return "%s - %s %s: %s %s" % (self.date_time, self.peripheral, self.physical_quantity, self.value, self.physical_unit)

class Display(Peripheral):

    RUNNABLE = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_message_queue = []

    async def run(self):
        while True:
            if len(self.log_message_queue) > 0:
                msg = self.log_message_queue.pop(0)
                self.display(msg)
            await self._run()

    async def _run(self):
        # Async wait for new instructions
        await asyncio.sleep(0.5)

    def add_log_message(self, msg):
        self.log_message_queue.append(msg)

    @abc.abstractmethod
    def display(self, str):
        raise NotImplementedError()

class DebugDisplay(Display):
    def display(self, str):
        print("Debug Display: %s" % str)

class DisplayDeviceStream(object):
    def __init__(self, peripheral_display_device):
        self.peripheral_display_device = peripheral_display_device
        self.str = ""

    def write(self, str):
        self.str += str

    def flush(self):
        self.peripheral_display_device.add_log_message(self.str)
        self.str = ""
