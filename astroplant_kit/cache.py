from pathlib import Path
import json

from .api import server_rpc

from typing import Any, List

CONFIGURATION_CACHE = Path("configuration.json")
QUANTITY_TYPES_CACHE = Path("quantity-types.json")


class Cache(object):
    def __init__(self, cache_dir):
        self._dir = Path(cache_dir)
        # Create cache directory if it does not exist yet.
        self._dir.mkdir(parents=True, exist_ok=True)

    def write_configuration(self, configuration: Any) -> None:
        with open(self._dir / CONFIGURATION_CACHE, "w") as f:
            json.dump(configuration, f)

    def read_configuration(self) -> Any:
        with open(self._dir / CONFIGURATION_CACHE, "r") as f:
            return json.load(f)

    def write_quantity_types(
        self, quantity_types: List[server_rpc.QuantityType]
    ) -> None:
        with open(self._dir / QUANTITY_TYPES_CACHE, "w") as f:
            json.dump(quantity_types, f)

    def read_quantity_types(self) -> List[server_rpc.QuantityType]:
        with open(self._dir / QUANTITY_TYPES_CACHE, "r") as f:
            return json.load(f)
