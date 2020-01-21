from pathlib import Path
import json

CONFIGURATION_CACHE = Path("configuration.json")
QUANTITY_TYPES_CACHE = Path("quantity-types.json")


class Cache(object):
    def __init__(self, cache_dir):
        self._dir = Path(cache_dir)
        # Create cache directory if it does not exist yet.
        self._dir.mkdir(parents=True, exist_ok=True)

    def write_configuration(self, configuration):
        with open(self._dir / CONFIGURATION_CACHE, "w") as f:
            json.dump(configuration, f)

    def read_configuration(self):
        with open(self._dir / CONFIGURATION_CACHE, "r") as f:
            return json.load(f)

    def write_quantity_types(self, quantity_types):
        with open(self._dir / QUANTITY_TYPES_CACHE, "w") as f:
            json.dump(quantity_types, f)

    def read_quantity_types(self):
        with open(self._dir / QUANTITY_TYPES_CACHE, "r") as f:
            return json.load(f)
