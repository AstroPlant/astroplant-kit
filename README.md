# AstroPlant kit
This repository contains the code for the AstroPlant kit growth system framework.

The AstroPlant kits communicate with the AstroPlant [back-end](https://github.com/astroplant/astroplant-server) for configuration and publishing measurements.

# Documentation

Full documentation can be found [here](https://astroplant-kit.readthedocs.io/en/latest/index.html).

# Install kit dependencies

Install the kit dependencies:

```bash
pip3 install -r requirements.txt
```

# Configure AstroPlant

The kit requires some basic configuration to function.

Copy `astroplant_kit/kit_config.sample.json` to `astroplant_kit/kit_config.json` and edit the configuration.

For example, a basic configuration for connecting the kit to the official AstroPlant backend is as follows.
The `auth.serial` and `auth.secret` are obtained by registering a new kit on the backend.

```json
{
    "api": {
        "root": "https://my.astroplant.io/api/"
    },
    "websockets": {
        "url": "wss://my.astroplant.io/"
    },
    "auth": {
        "serial": "k.********",
        "secret": "********"
    },
    "debug": {
        "level": "INFO",
        "peripheral_display": {
            "module_name": "peripheral",
            "class_name": "BlackHoleDisplay"
        }
    }
}
```

## Debug configuration

Log messages are printed to stdout, and can additionally be sent to a peripheral device.
The `level` field configures the minimum level of log messages that are sent to the peripheral display device.
See the [Python documentation](https://docs.python.org/3/library/logging.html#logging-levels) for available log levels.
A peripheral display device also displays recent measurements.

The `peripheral_display` entry in `debug` is used to configure the peripheral device for display.
The `module_name` field refers to a module where the implementation of the peripheral display device can be found, and `class_name` is the specific class to instantiate.
The module and class are then dynamically imported and instantiated.
Additional parameters can be supplied on class instantiation by adding a `parameters` fields.

The default `BlackHoleDisplay` ignores all log messages.
For debugging purposes, `class_name` can be set to `DebugDisplay`, which will send all display messages to stdout.

To display messages on a standard I2C LCD device, you can use the following configuration (requires [`astroplant-peripheral-device-library`](https://github.com/AstroPlant/astroplant-peripheral-device-library)):

```json
{
    ...,
    "debug": {
        "level": "INFO",
        "peripheral_display": {
            "module_name": "astroplant_peripheral_device_library.lcd",
            "class_name": "LCD",
            "parameters": {
                "i2c_address": "0x27"
            }
        }
    }
}
```

Set `i2c_address` to the address used by the device.

# Run the kit

To run the kit, perform:

```bash
./core.py
```
