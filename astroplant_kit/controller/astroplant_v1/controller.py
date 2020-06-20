import logging
import trio
import math
from datetime import datetime, time

from enum import Enum
from typing import NewType, Optional, Iterable, Callable, Dict, List, Set, Tuple
from typing_extensions import TypedDict

from ...peripheral import PeripheralManager, Peripheral, Measurement
from ..controller import Controller
from .fuzzy_logic import (
    FUZZY_TRUE,
    FUZZY_FALSE,
    Fuzzy,
    Shape,
    Singleton,
    Triangle,
    And,
    Or,
    Very,
    Slightly,
    Not,
    centroid,
)

logger = logging.getLogger("astroplant_kit.controller.astroplant_v1")


InputFuzzyVar = str
OutputFuzzyVar = str
TimeStr = str
PeripheralName = str
QuantityTypeIdStr = str
QuantityTypeId = int
Minutes = int
CommandName = str
SenseDefinitionName = str
ActionDefinitionName = str
Negation = bool
Delta = bool

InputMembership = NewType("InputMembership", int)
OutputMembership = NewType("OutputMembership", int)


class InputFuzzySet(Enum):
    LargeNegative = "largeNegative"
    # MediumNegative = "mediumNegative"
    SmallNegative = "smallNegative"
    Nominal = "nominal"
    SmallPositive = "smallPositive"
    # MediumPositive = "mediumPositive"
    LargePositive = "largePositive"


class OutputFuzzySet(Enum):
    Minimum = "minimal"
    Low = "low"
    Medium = "medium"
    High = "high"
    Maximum = "maximal"


class Hedge(Enum):
    Very = "very"
    Slightly = "slightly"


class Setpoint(TypedDict):
    time: str
    value: float


class InputSettings(TypedDict):
    nominalRange: float
    nominalDeltaRange: float
    deltaMeasurements: int
    setpoints: List[Setpoint]
    interpolation: int


class OutputSettingsContinuous(TypedDict):
    minimal: float
    maximal: float


class OutputSettingsDiscrete(TypedDict):
    values: List[float]


class OutputTypes(Enum):
    Discrete = "discrete"
    Continuous = "continuous"


class OutputSettings(TypedDict):
    type: OutputTypes
    continuous: Optional[OutputSettingsContinuous]
    discrete: Optional[OutputSettingsDiscrete]


class FuzzyRuleCondition(TypedDict):
    negation: Negation
    hedge: Optional[Hedge]
    delta: Delta
    peripheral: PeripheralName
    quantityType: QuantityTypeId
    fuzzyVariable: InputFuzzyVar


class FuzzyRuleImplication(TypedDict):
    peripheral: PeripheralName
    command: CommandName
    fuzzyVariable: OutputFuzzyVar


class FuzzyRule(TypedDict):
    condition: List[FuzzyRuleCondition]  # Conjunction
    implication: List[FuzzyRuleImplication]  # Conjunction
    activeFrom: TimeStr
    activeTo: TimeStr


class FuzzyControlRules(TypedDict):
    input: Dict[PeripheralName, Dict[QuantityTypeIdStr, InputSettings]]
    output: Dict[PeripheralName, Dict[CommandName, OutputSettings]]
    rules: List[FuzzyRule]


class Rules(TypedDict):
    fuzzyControl: FuzzyControlRules


def seconds_between_times(time1: time, time2: time) -> int:
    """Get the difference in seconds between two times."""
    diff = (time2.minute - time1.minute) * 60 + time2.second - time1.second
    if time1 < time2:
        diff += (time2.hour - time1.hour) * 60 * 60
    else:
        # Wrap around midnight.
        diff += (24 + time2.hour - time1.hour) * 60 * 60
    return diff


class Setpoints:
    def __init__(
        self, setpoints: List[Setpoint], interpolation: int,
    ):
        self._setpoints: Dict[time, float] = {}
        self._times: List[time] = []
        self._interpolation = interpolation

        for setpoint in setpoints:
            t = time.fromisoformat(setpoint["time"])
            self._times.append(t)
            self._setpoints[t] = setpoint["value"]

    def for_time(self, t: time) -> float:
        current_time = self._times[-1]
        next_time = self._times[0]
        time_diff = seconds_between_times(current_time, next_time)
        for (idx, setpoint_time) in enumerate(self._times):
            if t >= setpoint_time:
                current_time = setpoint_time
                next_time = self._times[(idx + 1) % len(self._times)]

        current_setpoint = self._setpoints[current_time]
        next_setpoint = self._setpoints[next_time]
        if (
            time_diff < self._interpolation
            and current_setpoint is not None
            and next_setpoint is not None
        ):
            current_setpoint += (
                (next_setpoint - current_setpoint) / self._interpolation * time_diff
            )
        return current_setpoint


class Input:
    def __init__(
        self,
        input_settings: Dict[PeripheralName, Dict[QuantityTypeIdStr, InputSettings]],
    ):
        self._shapes: Dict[InputFuzzySet, Shape] = {}
        self._error_transforms: Dict[
            Tuple[PeripheralName, QuantityTypeId], Callable[[float], float]
        ] = {}
        self._membership_values: Dict[
            Tuple[PeripheralName, QuantityTypeId, InputFuzzySet], Optional[Fuzzy]
        ] = {}
        self._setpoints: Dict[Tuple[PeripheralName, QuantityTypeId], Setpoints] = {}

        triangle_half_width = 2.0 / 5
        for (n, fuzzy_var) in enumerate(InputFuzzySet):
            triangle_offset = 2.0 * n / (len(InputFuzzySet) - 1)
            self._shapes[fuzzy_var] = Triangle(
                -1.0 + triangle_offset, triangle_half_width
            )

        def error_transform(input_settings: InputSettings) -> Callable[[float], float]:
            half_range = input_settings["nominalRange"] / triangle_half_width
            left_range = -half_range
            right_range = half_range

            def transform(error: float) -> float:
                if error <= left_range:
                    return -1.0
                elif error >= right_range:
                    return 1.0
                else:
                    return error / half_range

            return transform

        for (peripheral, qt_input_settings) in input_settings.items():
            for (quantity_type_str, settings) in qt_input_settings.items():
                quantity_type = int(quantity_type_str)
                self._setpoints[(peripheral, quantity_type)] = Setpoints(
                    settings["setpoints"], settings["interpolation"]
                )

                for (n, fuzzy_var) in enumerate(InputFuzzySet):
                    self._error_transforms[
                        (peripheral, quantity_type)
                    ] = error_transform(settings)
                    self._membership_values[
                        (peripheral, quantity_type, fuzzy_var)
                    ] = None

    def update(
        self, measurement: Measurement
    ) -> Set[Tuple[PeripheralName, QuantityTypeId, InputFuzzySet]]:
        """
        :returns: A set of memberships that were changed by the update.
        """
        p = measurement.peripheral
        qt = measurement.quantity_type
        if (p.name, qt.id) not in self._setpoints:
            return set()

        t = datetime.now().time()
        changed = set()

        setpoint = self._setpoints[(p.name, qt.id)].for_time(t)
        error = self._error_transforms[(p.name, qt.id)](measurement.value - setpoint)

        logger.debug(
            f"setpoint for {p.name} {qt.physical_quantity} {qt.physical_unit}: {setpoint} - current error: {error}"
        )

        for fuzzy_var in InputFuzzySet:
            old_value = self._membership_values[(p.name, qt.id, fuzzy_var)]
            self._membership_values[(p.name, qt.id, fuzzy_var)] = self._shapes[
                fuzzy_var
            ].fuzzify(error)
            if old_value != self._membership_values[(p.name, qt.id, fuzzy_var)]:
                changed.add((p.name, qt.id, fuzzy_var))

        return changed

    def membership_value(
        self,
        peripheral: PeripheralName,
        quantity_type: QuantityTypeId,
        fuzzy_var: InputFuzzySet,
    ) -> Optional[Fuzzy]:
        return self._membership_values[(peripheral, quantity_type, fuzzy_var)]


class Output:
    def __init__(
        self, output_settings: Dict[PeripheralName, Dict[CommandName, OutputSettings]],
    ):
        self._shapes: Dict[OutputFuzzySet, Shape] = {}
        self._output_transforms: Dict[
            Tuple[PeripheralName, CommandName], Callable[[Fuzzy], float]
        ] = {}
        self._membership_values: Dict[
            Tuple[PeripheralName, CommandName], Dict[OutputFuzzySet, Optional[Fuzzy]]
        ] = {}

        self.triangle_half_width = 1.0 / 5
        for (n, fuzzy_var) in enumerate(OutputFuzzySet):
            triangle_offset = 1.0 * n / (len(OutputFuzzySet) - 1)
            self._shapes[fuzzy_var] = Triangle(
                triangle_offset, self.triangle_half_width
            )

        def output_transform(
            output_settings: OutputSettings,
        ) -> Callable[[Fuzzy], float]:
            assert output_settings["continuous"] is not None
            lowest = output_settings["continuous"]["minimal"]
            highest = output_settings["continuous"]["maximal"]

            def transform(x: Fuzzy) -> float:
                val = lowest + x * (highest - lowest)

                # Ensure we're within bounds (floating point errors can throw us out).
                return max(lowest, min(highest, val))

            return transform

        for (peripheral, command_settings) in output_settings.items():
            for (command, settings) in command_settings.items():
                self._membership_values[(peripheral, command)] = {}
                for fuzzy_var in OutputFuzzySet:
                    self._membership_values[(peripheral, command)][fuzzy_var] = None

                type = OutputTypes(settings["type"])
                if type is OutputTypes.Discrete:
                    raise NotImplementedError(
                        "Not yet implemented for discrete outputs"
                    )
                elif type is OutputTypes.Continuous:
                    self._output_transforms[(peripheral, command)] = output_transform(
                        settings
                    )

    def set_membership_value(
        self,
        peripheral: PeripheralName,
        command: CommandName,
        fuzzy_var: OutputFuzzySet,
        value: Fuzzy,
    ) -> None:
        self._membership_values[(peripheral, command)][fuzzy_var] = value

    def defuzzified_action(
        self, peripheral: PeripheralName, command: CommandName
    ) -> float:
        fuzzy_values = self._membership_values[(peripheral, command)]
        curves: List[Tuple[Shape, Fuzzy]] = [
            (self._shapes[fuzzy_var], value)
            for (fuzzy_var, value) in fuzzy_values.items()
            if value is not None
        ]
        return self._output_transforms[(peripheral, command)](centroid(curves))


class Evaluator:
    class Condition:
        def __init__(self, condition: FuzzyRuleCondition):
            self.peripheral = condition["peripheral"]
            self.quantity_type = condition["quantityType"]
            self.fuzzy_var = InputFuzzySet(condition["fuzzyVariable"])
            self.negation = condition.get("negation", False)
            self.delta = condition.get("delta", False)
            hedge = condition.get("hedge", None)
            self.hedge = None if hedge is None else Hedge(hedge)

            if self.delta:
                raise NotImplementedError("Deltas are not yet implemented")

        def evaluate(self, input: Input) -> Fuzzy:
            assert not self.delta

            truth = input.membership_value(
                self.peripheral, self.quantity_type, self.fuzzy_var
            )

            if truth is None:
                return FUZZY_FALSE

            if self.hedge is Hedge.Very:
                truth = Very(truth)
            elif self.hedge is Hedge.Slightly:
                truth = Slightly(truth)

            if self.negation:
                truth = Not(truth)

            return truth

    class Rule:
        def __init__(self, fuzzy_rule: FuzzyRule):
            self.condition = [Evaluator.Condition(c) for c in fuzzy_rule["condition"]]

            self.implication = [
                (i["peripheral"], i["command"], OutputFuzzySet(i["fuzzyVariable"]))
                for i in fuzzy_rule["implication"]
            ]

            self.active_from = time.fromisoformat(fuzzy_rule["activeFrom"])
            self.active_to = time.fromisoformat(fuzzy_rule["activeTo"])

    def __init__(
        self, fuzzy_rules: List[FuzzyRule], input: Input, output: Output,
    ):
        self._rules = [Evaluator.Rule(fuzzy_rule) for fuzzy_rule in fuzzy_rules]
        self._input = input
        self._output = output

    def process(
        self, changed_inputs: Set[Tuple[PeripheralName, QuantityTypeId, InputFuzzySet]]
    ) -> Set[Tuple[PeripheralName, CommandName, OutputFuzzySet]]:
        output: Dict[Tuple[PeripheralName, CommandName, OutputFuzzySet], Fuzzy] = {}
        for rule in self._rules:
            evaluate = any(
                map(
                    lambda c: (c.peripheral, c.quantity_type, c.fuzzy_var)
                    in changed_inputs,
                    rule.condition,
                )
            )
            if evaluate:
                fuzzy = FUZZY_TRUE
                for condition in rule.condition:
                    fuzzy = And(fuzzy, condition.evaluate(self._input))

                for (p, command, fuzzy_var) in rule.implication:
                    if (p, command, fuzzy_var) not in output:
                        output[(p, command, fuzzy_var)] = FUZZY_FALSE
                    output[(p, command, fuzzy_var)] = Or(
                        output[(p, command, fuzzy_var)], fuzzy
                    )
        for ((p, command, fuzzy_var), truth) in output.items():
            self._output.set_membership_value(p, command, fuzzy_var, truth)

        return set(output.keys())


class AstroplantControllerV1(Controller):
    """
    The AstroPlant controller assumes peripheral devices' commands are
    idempotent.
    """

    def __init__(self, peripheral_manager: PeripheralManager, rules: Rules):
        super().__init__(peripheral_manager, rules)
        fuzzy_control = rules.get(
            "fuzzyControl", {"input": {}, "output": {}, "rules": []}
        )

        self._input = Input(fuzzy_control["input"])
        self._output = Output(fuzzy_control["output"])
        self._evaluator = Evaluator(fuzzy_control["rules"], self._input, self._output)

    async def run(self) -> None:
        """
        TODO: apply rules on a timer as well, especially useful when
        measurements come in slowly.
        """
        async for data in self.peripheral_manager.data_receiver():
            measurement = data.measurement
            if measurement is None:
                continue

            changed = self._input.update(measurement)
            commands_affected = self._evaluator.process(changed)

            for (peripheral_name, command, _) in commands_affected:
                peripheral = self.peripheral_manager.get_peripheral_by_name(
                    peripheral_name
                )
                if peripheral is None:
                    logger.warning("the peripheral %s does not exist", peripheral_name)
                    continue
                control = self.peripheral_manager.control(peripheral)
                try:
                    do = control.acquire_nowait()
                    value = self._output.defuzzified_action(peripheral_name, command)
                    action = {command: value}
                    try:
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
