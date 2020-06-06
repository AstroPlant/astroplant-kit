# AstroPlant kit
This repository contains the code for the AstroPlant kit growth system framework.

The AstroPlant kits communicate with the AstroPlant [back-end](https://github.com/astroplant/astroplant-server) for configuration and publishing measurements.

# Documentation

Full documentation can be found [here](https://astroplant-kit.readthedocs.io/en/latest/index.html).

# Install the kit

To install the kit through setuptools, run

```bash
$ python setup.py install
```

This installs the AstroPlant kit on your system, allowing you to use the AstroPlant kit CLI through:

```bash
$ astroplant-kit --help
```

# Install kit dependencies through pip

To install the kit dependencies through pip, run

```bash
$ pip3 install -r requirements.txt
```

This allows you to use the AstroPlant kit CLI through:

```bash
$ ./scripts/astroplant-kit --help
```

# Configure AstroPlant

The kit requires some basic configuration to function.

Copy `kit_config.sample.toml` to `kit_config.toml` and edit the configuration.

For example, a basic configuration for connecting the kit to the official AstroPlant network is as follows.
The `message_broker.auth.serial` and `message_broker.auth.secret` are obtained by registering a new kit on the [My AstroPlant website](https://my.astroplant.io).

```toml
[message_broker]
host = "mqtt.astroplant.io"
port = 1883

[message_broker.auth]
serial = "k-********"
secret = "********"

[debug]
level = "INFO"

[debug.peripheral_display]
module_name = "astroplant_kit.peripheral"
class_name = "BlackHoleDisplay"
```

If your kit should log in to the MQTT broker with a username different than its serial, you can specify the username for MQTT separately:

```toml
#...
[message_broker.auth]
username = "mqtt_username"
serial = "k-********"
secret = "********"
```

## Debug configuration

Log messages are printed to stdout, and can additionally be sent to a peripheral device.
The `level` field configures the minimum level of log messages that are sent to the peripheral display device.
See the [Python documentation](https://docs.python.org/3/library/logging.html#logging-levels) for available log levels.
A peripheral display device also displays recent measurements.

The `debug.peripheral_display` entry is used to configure the peripheral device for display.
The `module_name` field refers to a module where the implementation of the peripheral display device can be found, and `class_name` is the specific class to instantiate.
The module and class are then dynamically imported and instantiated.
Additional configuration can be performed on class instantiation by adding a `configuration` table.

The default `BlackHoleDisplay` ignores all log messages.
For debugging purposes, `class_name` can be set to `DebugDisplay`, which will send all display messages to stdout.

To display messages on a standard I2C LCD device, you can use the following configuration (requires [`astroplant-peripheral-device-library`](https://github.com/AstroPlant/astroplant-peripheral-device-library)):

```toml
# ...

[debug]
level = "INFO"

[debug.peripheral_display]
module_name = "astroplant_peripheral_device_library.lcd"
class_name = "LCD"

[debug.peripheral_display.configuration]
i2cAddress = "0x27"
```

Set `i2cAddress` to the address used by the device.

# Run the kit

To run the kit, perform:

```bash
$ astroplant-kit run kit_config.toml
```

or

```bash
$ ./scripts/astroplant-kit run kit_config.toml
```

## Timekeeping
Note that the kit uses the system time, and this time is reported in measurements.
On low power devices (such as the Raspberry Pi) this system time is often wrong until the system has been running and connected to the internet for some time.
Using systemd, you can delay running the kit until after the time has been synchronized using `time-sync.target`.
