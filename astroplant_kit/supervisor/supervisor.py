"""
Specifies the abstract kit supervisor, a base class to be extended, intended to
link sensors and actuators together.
"""

from typing import Any, Dict, List
import abc
from ..peripheral import PeripheralManager


class Supervisor:
    def __init__(self, peripheral_manager: PeripheralManager, rules: Any):
        self.peripheral_manager = peripheral_manager
        self.rules = rules

    @abc.abstractmethod
    async def run(self):
        """
        Asynchronously run the supervisor.
        """
        raise NotImplementedError()
