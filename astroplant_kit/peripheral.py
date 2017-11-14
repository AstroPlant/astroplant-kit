import abc
import sys
import datetime
import asyncio

class PeripheralManager(object):
    def __init__(self):
        self.peripherals = []
        self.subscribers = []

    def runnable_peripherals(self):
        return filter(lambda peripheral: peripheral.RUNNABLE, self.peripherals)

    async def run(self):
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
        for (predicate, callback) in self.subscribers:
            if predicate(measurement):
                callback(measurement)

    def create_peripheral(self, peripheral_class_name, peripheral_object_name, peripheral_parameters):
        """
        Create and add a peripheral by its class name.

        :param peripheral_class_name: The class name of the peripheral to add.
        :param peripheral_object_name: The name of the specific peripheral to add.
        :param peripheral_parameters: The instantiation parameters of the peripheral.
        """
        # Get class by class name
        try:
            peripheral_class = globals()[peripheral_class_name]
        except KeyError:
            raise ValueError("Could not find class '%s'" % peripheral_class_name)

        # Instantiate peripheral
        peripheral = peripheral_class(peripheral_object_name, **peripheral_parameters)
        if not isinstance(peripheral, Peripheral):
            raise TypeError("Class '%s' does not extend Peripheral" % peripheral_class_name)

        # Set message publication handle
        peripheral._set_publish_handle(self._publish_handle)

        self.peripherals.append(peripheral)

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

    @abc.abstractmethod
    @asyncio.coroutine
    def run(self):
        raise NotImplementedError()

    @abc.abstractmethod
    @asyncio.coroutine
    def do(self, command):
        raise NotImplementedError()

    def get_name(self):
        return self.name

    def _set_publish_handle(self, publish_handle):
        self._publish_handle = publish_handle

class Sensor(Peripheral):
    """
    Abstract sensor base class.
    """

    RUNNABLE = True

    @asyncio.coroutine
    def run(self):
        while True:
            measurement = yield from self.measure()
            asyncio.ensure_future(self._publish_measurement(measurement))

    @abc.abstractmethod
    @asyncio.coroutine
    def measure(self):
        raise NotImplementedError()

    @asyncio.coroutine
    def _publish_measurement(self, measurement):
        self._publish_handle(measurement)

class Measurement(object):
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

class Mock(Sensor):
    """
    A mock sensor implementation yielding fake measurements.
    """

    def __init__(self, *args, sleep):
        super().__init__(*args)
        self.sleep = int(sleep)

    async def measure(self):
        import random

        temperature = random.uniform(19, 22)
        measurement = Measurement(self, "Temperature", "Degrees Celsius", temperature)
        await asyncio.sleep(self.sleep / 1000)
        return measurement
