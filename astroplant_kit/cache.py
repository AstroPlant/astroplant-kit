from pathlib import Path
import json

CONFIGURATION_CACHE = Path("configuration.json")

class Cache(object):
    def __init__(self, cache_dir):
        self._dir = Path(cache_dir)

    def write_configuration(self, configuration):
        with open(self._dir / CONFIGURATION_CACHE, 'w') as f:
            json.dump(configuration, f)

    def read_configuration(self, configuration):
        with open(self._dir / CONFIGURATION_CACHE, 'r') as f:
            return json.load(f)

