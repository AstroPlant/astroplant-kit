"""
A rough implementation of fuzzy logic.
"""

import logging
import abc
import trio
import math
from collections import namedtuple

from typing import (
    NewType,
    TypeVar,
    Any,
    Optional,
    Iterable,
    Callable,
    Dict,
    List,
    Set,
    Tuple,
)
from typing_extensions import TypedDict

logger = logging.getLogger("astroplant_kit.supervisor.astroplant_v1")

T = TypeVar("T")

Fuzzy = NewType("Fuzzy", float)

FUZZY_TRUE: Fuzzy = Fuzzy(1.0)
FUZZY_FALSE: Fuzzy = Fuzzy(0.0)


def And(x: Fuzzy, y: Fuzzy) -> Fuzzy:
    return min(x, y)


def Or(x: Fuzzy, y: Fuzzy) -> Fuzzy:
    return max(x, y)


def Very(x: Fuzzy) -> Fuzzy:
    """Sharphen membership curve."""
    return Fuzzy(x ** 2)


def Slightly(x: Fuzzy) -> Fuzzy:
    """Flatten membership curve."""
    return Fuzzy(x ** 0.75)


def Not(x: Fuzzy) -> Fuzzy:
    return Fuzzy(1 - x)


class Shape:
    @abc.abstractmethod
    def fuzzify(self, x: float) -> Fuzzy:
        pass

    @abc.abstractmethod
    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        """Returns a tuple containing the center of mass and the total mass."""
        pass


class Inverse(Shape):
    def __init__(self, shape: Shape):
        self.inner_shape = shape

    def fuzzify(self, x: float) -> Fuzzy:
        return Fuzzy(1 - self.inner_shape.fuzzify(x))

    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        raise NotImplementedError()


class Gaussian(Shape):
    def __init__(self, mean: float, standard_deviation: float):
        self.mean = mean
        self.standard_deviation = standard_deviation

    def fuzzify(self, x: float) -> Fuzzy:
        from math import exp, sqrt, pi

        gauss = (
            1
            / self.standard_deviation
            * sqrt(2 * pi)
            * exp(-1 / 2 * ((x - self.mean) / self.standard_deviation) ** 2)
        )

        return Fuzzy(gauss)

    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        raise NotImplementedError()


class Singleton(Shape):
    def __init__(self, mean: float):
        self.mean = mean

    def fuzzify(self, x: float) -> Fuzzy:
        return Fuzzy(1 if x == self.mean else 0)

    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        # Hacky: give the singleton a very small non-zero mass.
        return (self.mean, 0.00001)


class Linear(Shape):
    def __init__(self, x1: float, x2: float):
        if x1 >= x2:
            raise ValueError("x1 must be smaller than x2")

        self.x1 = x1
        self.x2 = x2

    def fuzzify(self, x: float) -> Fuzzy:
        if x <= self.x1:
            return Fuzzy(0)
        elif x >= self.x2:
            return Fuzzy(0)
        else:
            return Fuzzy((x - self.x1) / (self.x2 - self.x1))

    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        raise NotImplementedError()


class Triangle(Shape):
    def __init__(self, x: float, half_width: float):
        self.x = x
        self.half_width = half_width

    def fuzzify(self, x: float) -> Fuzzy:
        dist = abs(x - self.x)
        if dist > self.half_width:
            return Fuzzy(0.0)
        else:
            return Fuzzy(1.0 - dist / self.half_width)

    def center_of_mass(self, max: Fuzzy) -> Tuple[float, float]:
        height_missing = 1 - max
        half_width_missing = height_missing * self.half_width
        surface_missing = half_width_missing * height_missing
        total_mass = self.half_width - surface_missing
        return (self.x, total_mass)


def centroid(curves: List[Tuple[Shape, Fuzzy]]) -> Fuzzy:
    """Calculate the centroid of the given curves with max membership values."""

    total_mass = 0.0
    total_center_of_mass = 0.0

    for (shape, value) in curves:
        (center, mass) = shape.center_of_mass(value)
        total_mass += mass
        total_center_of_mass += center * mass

    if total_mass == 0.0:
        return Fuzzy(total_center_of_mass / len(curves))

    return Fuzzy(total_center_of_mass / total_mass)


def argmax(fn: Callable[[T], Any], args: Iterable[T]):
    m = None
    for arg in args:
        if m is None or fn(arg) > m:
            m = arg
    return m
