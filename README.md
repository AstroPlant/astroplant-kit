# AstroPlant kit
This repository contains the code for the AstroPlant kit growth system framework.

The AstroPlant kits communicate with the AstroPlant [back-end](https://github.com/astroplant/astroplant-server) for configuration and publishing measurements.

# Documentation

Full documentation can be found [here](https://astroplant-kit.readthedocs.io/en/latest/index.html).

# Install kit dependencies

Install the kit dependencies:

```bash
pip install -r requirements.txt
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
            "class_name": "DebugDisplay"
        }
    }
}
```

# Run the kit

To run the kit, perform:

```bash
python core.py
```
