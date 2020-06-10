"""
Provides an opinionated implementation of a supervisor allowing for some common
use-cases.
"""

import logging
import trio
from typing import Dict, Any, List
from collections import namedtuple
from datetime import datetime, time

from ..peripheral import PeripheralManager, Peripheral, Measurement
from .supervisor import Supervisor

logger = logging.getLogger("astroplant_kit.supervisor")


class AstroplantSupervisorV1(Supervisor):
    """
    The AstroPlant supervisor assumes peripheral devices' commands are
    idempotent.
    """

    class Condition:
        def __init__(self, condition):
            if condition is None:
                self._always = True
            else:
                self._always = False
                self._peripheral = condition["peripheral"]
                self._quantity_type = condition["quantityType"]

                value = float(condition["value"])
                comparison = condition["comparison"]
                if comparison == "lt":
                    self._comparison = lambda v: v < value
                elif comparison == "eq":
                    self._comparison = lambda v: v == value
                elif comparison == "gt":
                    self._comparison = lambda v: v > value

        def matches(self, measurement):
            if self._always:
                return True

            if measurement.quantity_type.id != self._quantity_type:
                return False

            if measurement.peripheral.name != self._peripheral:
                return False

            return self._comparison(measurement.value)

    class PeripheralRules:
        def __init__(self, peripheral_rules):
            ConditionalActions = namedtuple(
                "ConditionalActions", ["condition", "actions"]
            )
            TimedConditionalActions = namedtuple(
                "TimedConditionalActions", ["time", "conditional_actions_list"]
            )

            self._timed_conditional_actions: List[TimedConditionalActions] = []
            for rule in peripheral_rules:
                t = time.fromisoformat(rule["time"])
                conditional_actions_list = []
                for conditional_actions in rule["conditionalActions"]:
                    condition = AstroplantSupervisorV1.Condition(
                        conditional_actions["condition"]
                    )
                    _conditional_actions = ConditionalActions(
                        condition=condition, actions=conditional_actions["actions"]
                    )

                    conditional_actions_list.append(_conditional_actions)

                self._timed_conditional_actions.append(
                    TimedConditionalActions(
                        time=t, conditional_actions_list=conditional_actions_list
                    )
                )

            self._timed_conditional_actions.sort(key=lambda t: t.time)

        def get_conditional_actions_list(self, time):
            """
            Fetch the conditional actions list for the most recently passed time.
            """
            if len(self._timed_conditional_actions) == 0:
                return None

            conditional_actions_list = None
            for timed_conditional_actions in self._timed_conditional_actions:
                if time >= timed_conditional_actions.time:
                    conditional_actions_list = (
                        timed_conditional_actions.conditional_actions_list
                    )

            if conditional_actions_list is None:
                # Wrapped around the clock.
                timed_conditional_actions = self._timed_conditional_actions[-1]
                conditional_actions_list = (
                    timed_conditional_actions.conditional_actions_list
                )

            return conditional_actions_list

        def get_actions(self, time, measurement):
            conditional_actions_list = self.get_conditional_actions_list(time)
            if conditional_actions_list is None:
                return None

            # Return actions of the first condition that matches.
            for conditional_actions in conditional_actions_list:
                if conditional_actions.condition.matches(measurement):
                    return conditional_actions.actions

            return []

    def __init__(self, peripheral_manager: PeripheralManager, rules: Dict[str, Any]):
        super().__init__(peripheral_manager, rules)
        self._peripheral_rules: Dict[str, AstroplantSupervisorV1.PeripheralRules] = {}
        for (peripheral_name, peripheral_rules) in rules.items():
            self._peripheral_rules[peripheral_name] = self.PeripheralRules(
                peripheral_rules
            )

    async def run(self):
        """
        TODO: apply rules on a timer as well, especially useful when
        measurements come in slowly.
        """
        async for measurement in self.peripheral_manager.measurements_receiver():
            if not isinstance(measurement, Measurement):
                continue

            for (peripheral_name, rules) in self._peripheral_rules.items():
                peripheral = self.peripheral_manager.get_peripheral_by_name(
                    peripheral_name
                )
                if peripheral is None:
                    logger.warning("the peripheral %s does not exist", peripheral_name)
                    continue

                actions = rules.get_actions(datetime.now().time(), measurement)
                control = self.peripheral_manager.control(peripheral)
                try:
                    do = control.acquire_nowait()
                    try:
                        for action in actions:
                            logger.debug(
                                "sending action to peripheral %s: %s",
                                peripheral_name,
                                action,
                            )
                            await do(action)
                    finally:
                        control.release()
                except trio.WouldBlock:
                    # TODO: perhaps retry action after some time.
                    logger.debug(
                        "tried to control peripheral %s but could not immediately acquire lock",
                        peripheral_name,
                    )
