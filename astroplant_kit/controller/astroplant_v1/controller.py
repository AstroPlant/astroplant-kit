import logging
import trio
from datetime import datetime, time
from collections import defaultdict

from enum import Enum
from typing import NewType, Any, Optional, Iterable, Callable, Dict, List, Set, Tuple
from typing_extensions import TypedDict

from ...peripheral import PeripheralManager, Measurement
from ..controller import Controller
from .fuzzy_logic import (
    FUZZY_TRUE,
    FUZZY_FALSE,
    Fuzzy,
    Shape,
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
OutputScheduleId = int
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
    interpolated: bool


class OutputSettingsContinuous(TypedDict):
    minimal: float
    maximal: float


class OutputSettingsScheduleEntry(TypedDict):
    time: str
    value: Any


class OutputSettingsSchedule(TypedDict):
    schedule: List[OutputSettingsScheduleEntry]


class OutputSettingsScheduled(TypedDict):
    interpolated: bool
    schedules: List[OutputSettingsSchedule]


class OutputTypes(Enum):
    Scheduled = "scheduled"
    Continuous = "continuous"


class OutputSettings(TypedDict):
    type: OutputTypes
    continuous: Optional[OutputSettingsContinuous]
    scheduled: Optional[OutputSettingsScheduled]


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


class FuzzyRuleSchedule(TypedDict):
    peripheral: PeripheralName
    command: CommandName
    schedule: OutputScheduleId


class FuzzyRule(TypedDict):
    condition: List[FuzzyRuleCondition]  # Conjunction
    implication: List[FuzzyRuleImplication]  # Conjunction
    schedules: List[FuzzyRuleSchedule]
    activeFrom: TimeStr
    activeTo: TimeStr


class FuzzyControlRules(TypedDict):
    input: Dict[PeripheralName, Dict[QuantityTypeIdStr, InputSettings]]
    output: Dict[PeripheralName, Dict[CommandName, OutputSettings]]
    rules: List[FuzzyRule]


class Rules(TypedDict):
    fuzzyControl: FuzzyControlRules


def seconds_between_times(time1: time, time2: time) -> float:
    """Get the difference in seconds between two times."""
    diff = (
        (time2.minute - time1.minute) * 60
        + (time2.second - time1.second)
        + (time2.microsecond - time1.microsecond) / 1000000
    )
    if time1 < time2:
        diff += (time2.hour - time1.hour) * 60 * 60
    else:
        # Wrap around midnight.
        diff += (24 + time2.hour - time1.hour) * 60 * 60
    return diff


class Setpoints:
    def __init__(
        self, setpoints: List[Setpoint], interpolated: bool,
    ):
        assert len(setpoints) > 0

        self._setpoints: Dict[time, float] = {}
        self._times: List[time] = []
        self._interpolated = interpolated

        for setpoint in setpoints:
            t = time.fromisoformat(setpoint["time"])
            self._times.append(t)
            self._setpoints[t] = setpoint["value"]

    def for_time(self, t: time) -> float:
        current_time = self._times[-1]
        next_time = self._times[0]
        for (idx, setpoint_time) in enumerate(self._times):
            if t >= setpoint_time:
                current_time = setpoint_time
                next_time = self._times[(idx + 1) % len(self._times)]

        entry_time_diff = seconds_between_times(current_time, next_time)
        time_elapsed_from_current = seconds_between_times(current_time, t)

        current_setpoint = self._setpoints[current_time]
        next_setpoint = self._setpoints[next_time]
        if self._interpolated:
            current_setpoint += (
                (next_setpoint - current_setpoint)
                / entry_time_diff
                * time_elapsed_from_current
            )
        return current_setpoint


TRIANGLE_HALF_WIDTH = 2.0 / 5
SHAPES: Dict[InputFuzzySet, Shape] = {}

for (n, fuzzy_var) in enumerate(InputFuzzySet):
    triangle_offset = 2.0 * n / (len(InputFuzzySet) - 1)
    SHAPES[fuzzy_var] = Triangle(-1.0 + triangle_offset, TRIANGLE_HALF_WIDTH)


def _transform_builder(nominal_range: float) -> Callable[[float], float]:
    """Creates a transformer, mapping inputs to lie between -1.0 and 1.0."""
    half_range = nominal_range / TRIANGLE_HALF_WIDTH
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


class InputType(Enum):
    ERROR = 1
    DELTA = 2


class Input:
    class _MeasurementHandler:
        def __init__(self, input_settings: InputSettings):
            assert input_settings["nominalRange"] > 0.0
            assert input_settings["nominalDeltaRange"] > 0.0
            assert input_settings["deltaMeasurements"] >= 1

            self._error_transform: Callable[[float], float] = _transform_builder(
                input_settings["nominalRange"]
            )
            self._delta_transform: Callable[[float], float] = _transform_builder(
                input_settings["nominalDeltaRange"]
            )
            self._delta_measurements: int = input_settings["deltaMeasurements"]

            self.membership_values: Dict[InputFuzzySet, Optional[Fuzzy]] = {}
            self.delta_membership_values: Dict[InputFuzzySet, Optional[Fuzzy]] = {}
            self._measurements: List[Measurement] = []

            self._setpoints: Setpoints = Setpoints(
                input_settings["setpoints"], input_settings["interpolated"]
            )

            for (n, fuzzy_var) in enumerate(InputFuzzySet):
                self.membership_values[fuzzy_var] = None
                self.delta_membership_values[fuzzy_var] = None

        def update(
            self, measurement: Measurement
        ) -> Set[Tuple[InputType, InputFuzzySet]]:
            t = datetime.now().time()
            changed = set()

            self._measurements.append(measurement)
            num_measurements = len(self._measurements)
            if num_measurements >= self._delta_measurements + 1:
                if num_measurements > self._delta_measurements + 1:
                    self._measurements.pop(0)

                delta = self._delta_transform(
                    self._measurements[-1].value - self._measurements[0].value
                )

                for fuzzy_var in InputFuzzySet:
                    old_value = self.delta_membership_values[fuzzy_var]
                    self.delta_membership_values[fuzzy_var] = SHAPES[fuzzy_var].fuzzify(
                        delta
                    )
                    if old_value != self.delta_membership_values[fuzzy_var]:
                        changed.add((InputType.DELTA, fuzzy_var))

            setpoint = self._setpoints.for_time(t)
            error = self._error_transform(measurement.value - setpoint)

            for fuzzy_var in InputFuzzySet:
                old_value = self.membership_values[fuzzy_var]
                self.membership_values[fuzzy_var] = SHAPES[fuzzy_var].fuzzify(error)
                if old_value != self.membership_values[fuzzy_var]:
                    changed.add((InputType.ERROR, fuzzy_var))

            return changed

    def __init__(
        self,
        input_settings: Dict[PeripheralName, Dict[QuantityTypeIdStr, InputSettings]],
    ):
        self._handlers: Dict[
            Tuple[PeripheralName, QuantityTypeId], Input._MeasurementHandler
        ] = {}

        for (peripheral, qt_input_settings) in input_settings.items():
            for (quantity_type_str, settings) in qt_input_settings.items():
                quantity_type = int(quantity_type_str)
                self._handlers[(peripheral, quantity_type)] = Input._MeasurementHandler(
                    settings
                )

    def update(
        self, measurement: Measurement
    ) -> Set[Tuple[PeripheralName, QuantityTypeId, InputType, InputFuzzySet]]:
        """
        :returns: A set of memberships that were changed by the update.
        """
        p = measurement.peripheral
        qt = measurement.quantity_type
        if (p.name, qt.id) not in self._handlers:
            return set()

        handler = self._handlers[(p.name, qt.id)]
        return set(
            map(
                lambda change: (p.name, qt.id, change[0], change[1]),
                handler.update(measurement),
            )
        )

    def membership_value(
        self,
        peripheral: PeripheralName,
        quantity_type: QuantityTypeId,
        fuzzy_var: InputFuzzySet,
    ) -> Optional[Fuzzy]:
        return self._handlers[(peripheral, quantity_type)].membership_values[fuzzy_var]

    def delta_membership_value(
        self,
        peripheral: PeripheralName,
        quantity_type: QuantityTypeId,
        fuzzy_var: InputFuzzySet,
    ) -> Optional[Fuzzy]:
        return self._handlers[(peripheral, quantity_type)].delta_membership_values[
            fuzzy_var
        ]


class OutputSchedule:
    def __init__(self, interpolated: bool, schedule: OutputSettingsSchedule):
        self._schedule: Dict[time, Any] = {}
        self._times: List[time] = []
        self._interpolated = interpolated

        for schedule_entry in schedule["schedule"]:
            t = time.fromisoformat(schedule_entry["time"])
            self._times.append(t)
            self._schedule[t] = schedule_entry["value"]

    def for_time(self, t: time) -> float:
        current_time = self._times[-1]
        next_time = self._times[0]
        for (idx, schedule_entry_time) in enumerate(self._times):
            if t >= schedule_entry_time:
                current_time = schedule_entry_time
                next_time = self._times[(idx + 1) % len(self._times)]

        entry_time_diff = seconds_between_times(current_time, next_time)
        time_elapsed_from_current = seconds_between_times(current_time, t)

        current_schedule_entry = self._schedule[current_time]
        next_schedule_entry = self._schedule[next_time]
        if (
            self._interpolated
            and current_schedule_entry is not None
            and next_schedule_entry is not None
        ):
            current_schedule_entry += (
                (next_schedule_entry - current_schedule_entry)
                / entry_time_diff
                * time_elapsed_from_current
            )
        return current_schedule_entry


class Output:
    def __init__(
        self, output_settings: Dict[PeripheralName, Dict[CommandName, OutputSettings]],
    ):
        self._shapes: Dict[OutputFuzzySet, Shape] = {}
        self._continuous_output_transforms: Dict[
            Tuple[PeripheralName, CommandName], Callable[[Fuzzy], float]
        ] = {}
        self._scheduled_output_schedules: Dict[
            Tuple[PeripheralName, CommandName], List[OutputSchedule]
        ] = {}
        self._continuous_membership_values: Dict[
            Tuple[PeripheralName, CommandName], Dict[OutputFuzzySet, Optional[Fuzzy]]
        ] = {}
        self._scheduled_membership_values: Dict[
            Tuple[PeripheralName, CommandName], Dict[OutputScheduleId, Optional[Fuzzy]]
        ] = {}

        triangle_half_width = 1.0 / 5
        for (n, fuzzy_var) in enumerate(OutputFuzzySet):
            triangle_offset = 1.0 * n / (len(OutputFuzzySet) - 1)
            self._shapes[fuzzy_var] = Triangle(triangle_offset, triangle_half_width)

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
                type = OutputTypes(settings["type"])
                if type is OutputTypes.Scheduled:
                    self._scheduled_membership_values[(peripheral, command)] = {}
                    self._scheduled_output_schedules[(peripheral, command)] = []

                    assert settings["scheduled"] is not None
                    for (idx, schedule) in enumerate(
                        settings["scheduled"]["schedules"]
                    ):
                        self._scheduled_membership_values[(peripheral, command)][
                            idx + 1
                        ] = None
                        self._scheduled_output_schedules[(peripheral, command)].append(
                            OutputSchedule(
                                settings["scheduled"]["interpolated"], schedule
                            )
                        )
                elif type is OutputTypes.Continuous:
                    self._continuous_membership_values[(peripheral, command)] = {}
                    for fuzzy_var in OutputFuzzySet:
                        self._continuous_membership_values[(peripheral, command)][
                            fuzzy_var
                        ] = None

                    self._continuous_output_transforms[
                        (peripheral, command)
                    ] = output_transform(settings)

    @property
    def scheduled_commands(self) -> Iterable[Tuple[PeripheralName, CommandName]]:
        return self._scheduled_membership_values.keys()

    def get_continuous_membership_value(
        self,
        peripheral: PeripheralName,
        command: CommandName,
        fuzzy_var: OutputFuzzySet,
    ) -> Optional[Fuzzy]:
        return self._continuous_membership_values[(peripheral, command)][fuzzy_var]

    def set_continuous_membership_value(
        self,
        peripheral: PeripheralName,
        command: CommandName,
        fuzzy_var: OutputFuzzySet,
        value: Fuzzy,
    ) -> None:
        self._continuous_membership_values[(peripheral, command)][fuzzy_var] = value

    def get_scheduled_membership_value(
        self,
        peripheral: PeripheralName,
        command: CommandName,
        schedule: OutputScheduleId,
    ) -> Optional[Fuzzy]:
        return self._scheduled_membership_values[(peripheral, command)][schedule]

    def set_scheduled_membership_value(
        self,
        peripheral: PeripheralName,
        command: CommandName,
        schedule: OutputScheduleId,
        value: Fuzzy,
    ) -> None:
        self._scheduled_membership_values[(peripheral, command)][schedule] = value

    def _defuzzified_continuous_action(
        self, peripheral: PeripheralName, command: CommandName
    ) -> float:
        fuzzy_values = self._continuous_membership_values[(peripheral, command)]
        curves: List[Tuple[Shape, Fuzzy]] = [
            (self._shapes[fuzzy_var], value)
            for (fuzzy_var, value) in fuzzy_values.items()
            if value is not None
        ]
        return self._continuous_output_transforms[(peripheral, command)](
            centroid(curves)
        )

    def _defuzzified_scheduled_action(
        self, peripheral: PeripheralName, command: CommandName, for_time: time
    ) -> Any:
        schedule_fuzzy_values = self._scheduled_membership_values[(peripheral, command)]

        max_fuzzy = None
        argmax_schedule_id = 1
        for (schedule_id, fuzzy) in schedule_fuzzy_values.items():
            if max_fuzzy is None or fuzzy is not None and fuzzy > max_fuzzy:
                max_fuzzy = fuzzy
                argmax_schedule_id = schedule_id

        schedule = self._scheduled_output_schedules[(peripheral, command)][
            argmax_schedule_id - 1
        ]
        return schedule.for_time(for_time)

    def defuzzified_action(
        self, peripheral: PeripheralName, command: CommandName, for_time: time
    ) -> Any:
        if (peripheral, command) in self._continuous_membership_values:
            return self._defuzzified_continuous_action(peripheral, command)
        elif (peripheral, command) in self._scheduled_membership_values:
            return self._defuzzified_scheduled_action(peripheral, command, for_time)


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

        def evaluate(self, input: Input) -> Fuzzy:
            truth = None
            if self.delta:
                truth = input.delta_membership_value(
                    self.peripheral, self.quantity_type, self.fuzzy_var
                )
            else:
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
            self.truth = FUZZY_FALSE

            self.condition = [Evaluator.Condition(c) for c in fuzzy_rule["condition"]]

            self.implication = [
                (i["peripheral"], i["command"], OutputFuzzySet(i["fuzzyVariable"]))
                for i in fuzzy_rule["implication"]
            ]
            self.schedules = [
                (i["peripheral"], i["command"], int(i["schedule"]))
                for i in fuzzy_rule["schedules"]
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
        self,
        changed_inputs: Set[
            Tuple[PeripheralName, QuantityTypeId, InputType, InputFuzzySet]
        ],
        now: time,
    ) -> Set[Tuple[PeripheralName, CommandName]]:
        affected_commands = set()

        total_implication: Dict[
            Tuple[PeripheralName, CommandName, OutputFuzzySet], Fuzzy
        ] = defaultdict(lambda: FUZZY_FALSE)
        total_schedules: Dict[
            Tuple[PeripheralName, CommandName, OutputScheduleId], Fuzzy
        ] = defaultdict(lambda: FUZZY_FALSE)

        for rule in self._rules:
            evaluate = any(
                map(
                    lambda c: (
                        c.peripheral,
                        c.quantity_type,
                        InputType.DELTA if c.delta else InputType.ERROR,
                        c.fuzzy_var,
                    )
                    in changed_inputs,
                    rule.condition,
                )
            )
            if evaluate:
                fuzzy = FUZZY_TRUE
                for condition in rule.condition:
                    fuzzy = And(fuzzy, condition.evaluate(self._input))
                rule.truth = fuzzy

            if rule.active_from <= now <= rule.active_to:
                for (p, command, fuzzy_var) in rule.implication:
                    total_implication[(p, command, fuzzy_var)] = Or(
                        total_implication[(p, command, fuzzy_var)], rule.truth
                    )

                for (p, command, schedule) in rule.schedules:
                    total_schedules[(p, command, schedule)] = Or(
                        total_schedules[(p, command, schedule)], rule.truth
                    )

        # Currently, if due to the rules' from_time and to_time settings there are no
        # longer active rules for a specific peripheral and command (but there have been
        # previosuly), the last truth value stays active.
        for ((p, command, fuzzy_var), truth) in total_implication.items():
            old = self._output.get_continuous_membership_value(p, command, fuzzy_var)
            if old != truth:
                affected_commands.add((p, command))
            self._output.set_continuous_membership_value(p, command, fuzzy_var, truth)

        for ((p, command, schedule), truth) in total_schedules.items():
            old = self._output.get_scheduled_membership_value(p, command, schedule)
            if old != truth:
                affected_commands.add((p, command))
            self._output.set_scheduled_membership_value(p, command, schedule, truth)

        return affected_commands


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

        self._sending: Set[Tuple[PeripheralName, CommandName]] = set()
        self._current_command_value: Dict[Tuple[PeripheralName, CommandName], Any] = {}

    async def _do_command(self, peripheral_name: PeripheralName, command: CommandName):
        """
        Waits until the peripheral is ready to accept a command, then sends the most
        recent command value.
        """
        if (peripheral_name, command) in self._sending:
            return
        self._sending.add((peripheral_name, command))

        peripheral = self.peripheral_manager.get_peripheral_by_name(peripheral_name)
        if peripheral is None:
            logger.warning("the peripheral %s does not exist", peripheral_name)
            return

        async with self.peripheral_manager.control(peripheral) as do:
            action = {command: self._current_command_value[(peripheral_name, command)]}
            try:
                logger.debug(
                    "sending action to peripheral %s: %s", peripheral_name, action,
                )
                await do(action)
            finally:
                self._sending.remove((peripheral_name, command))

    async def _perform_scheduled_commands(self) -> None:
        scheduled_commands = list(self._output.scheduled_commands)

        async with trio.open_nursery() as nursery:
            while True:
                t = datetime.now().time()
                for (peripheral, command) in scheduled_commands:
                    value = self._output.defuzzified_action(peripheral, command, t)
                    if value != self._current_command_value.get((peripheral, command)):
                        self._current_command_value[(peripheral, command)] = value
                        nursery.start_soon(self._do_command, peripheral, command)
                await trio.sleep(0.5)

    async def run(self) -> None:
        """
        TODO: apply rules on a timer as well, especially useful when
        measurements come in slowly.
        """
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._perform_scheduled_commands)

            async for measurement in self.peripheral_manager.measurement_receiver():
                t = datetime.now().time()
                changed = self._input.update(measurement)
                commands_affected = self._evaluator.process(changed, t)

                for (peripheral, command) in commands_affected:
                    value = self._output.defuzzified_action(peripheral, command, t)
                    if value != self._current_command_value.get((peripheral, command)):
                        self._current_command_value[(peripheral, command)] = value
                        nursery.start_soon(self._do_command, peripheral, command)
