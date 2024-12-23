"""
Time things with a context manager. Nice display of time_to_wait_s elapsed.
"""

from __future__ import annotations

import time
import collections as cl
import datetime as dt

class TimeDelta:
    def __init__(self,
        nanoseconds: int|float|dt.timedelta = 0,
        microseconds: int|float = 0,
        milliseconds: int|float = 0,
        seconds: int|float = 0,
        minutes: int|float = 0,
        hours: int|float = 0,
        days: int|float = 0,
        weeks: int|float = 0,
        years: int|float = 0,
    ):
        if isinstance(nanoseconds, dt.timedelta):
            self.nanoseconds = nanoseconds.total_seconds() * 1e9
        else:
            self.nanoseconds = (
                nanoseconds
                + microseconds * 1e3
                + milliseconds * 1e6
                + seconds * 1e9
                + minutes * 60 * 1e9
                + hours * 3600 * 1e9
                + days * 86400 * 1e9
                + weeks * 604800 * 1e9
                + years * 31556952 * 1e9)
    @property
    def microseconds(self):
        return self.nanoseconds / 1e3
    @property
    def milliseconds(self):
        return self.nanoseconds / 1e6
    @property
    def seconds(self):
        return self.nanoseconds / 1e9
    @property
    def minutes(self):
        return self.nanoseconds / (1e9 * 60)
    @property
    def hours(self):
        return self.nanoseconds / (1e9 * 3600)
    @property
    def days(self):
        return self.nanoseconds / (1e9 * 86400)
    @property
    def weeks(self):
        return self.nanoseconds / (1e9 * 604800)
    @property
    def years(self):
        return self.nanoseconds / (1e9 * 31556952)
    def timedelta(self):
        return dt.timedelta(microseconds=self.nanoseconds // 1e3)
    def __sub__(self, other):
        if isinstance(other, TimeDelta):
            return TimeDelta(nanoseconds=self.nanoseconds - other.nanoseconds)
        elif isinstance(other, dt.timedelta):
            return TimeDelta(nanoseconds=self.nanoseconds - other.total_seconds() * 1e9)
        else:
            return TimeDelta(nanoseconds=self.nanoseconds - other)
    def __add__(self, other):
        if isinstance(other, TimeDelta):
            return TimeDelta(nanoseconds=self.nanoseconds + other.nanoseconds)
        elif isinstance(other, dt.timedelta):
            return TimeDelta(nanoseconds=self.nanoseconds + other.total_seconds() * 1e9)
        else:
            return TimeDelta(nanoseconds=self.nanoseconds + other)
    def __mul__(self, other):
        if isinstance(other, TimeDelta):
            return TimeDelta(nanoseconds=self.nanoseconds * other.nanoseconds)
        elif isinstance(other, dt.timedelta):
            return TimeDelta(nanoseconds=self.nanoseconds * other.total_seconds() * 1e9)
        else:
            return TimeDelta(nanoseconds=self.nanoseconds * other)
    def __truediv__(self, other):
        if isinstance(other, TimeDelta):
            return TimeDelta(nanoseconds=self.nanoseconds / other.nanoseconds)
        elif isinstance(other, dt.timedelta):
            return TimeDelta(nanoseconds=self.nanoseconds / other.total_seconds() * 1e9)
        else:
            return TimeDelta(nanoseconds=self.nanoseconds / other)
    def __floordiv__(self, other):
        if isinstance(other, TimeDelta):
            return TimeDelta(nanoseconds=self.nanoseconds // other.nanoseconds)
        elif isinstance(other, dt.timedelta):
            return TimeDelta(nanoseconds=self.nanoseconds // other.total_seconds() * 1e9)
        else:
            return TimeDelta(nanoseconds=self.nanoseconds // other)
    def __lt__(self, other):
        if isinstance(other, TimeDelta):
            return self.nanoseconds < other.nanoseconds
        elif isinstance(other, dt.timedelta):
            return self.nanoseconds < other.total_seconds() * 1e9
        else:
            return self.nanoseconds < other
    def __eq__(self, other):
        if isinstance(other, TimeDelta):
            return self.nanoseconds == other.nanoseconds
        elif isinstance(other, dt.timedelta):
            return self.nanoseconds == other.total_seconds() * 1e9
        else:
            return self.nanoseconds == other
    def __hash__(self):
        return hash(self.nanoseconds)
    def display(self):
        if self.milliseconds < 10:
            return f"{self.milliseconds:.2f}ms"
        elif self.seconds < 1:
            return f"{self.milliseconds:.0f}ms"
        elif self.seconds < 10:
            return f"{self.seconds:.2f}s"
        elif self.seconds < 60:
            return f"{self.seconds:.0f}s"
        elif self.minutes < 10:
            return f"{self.minutes:.2f}m"
        elif self.minutes < 60:
            return f"{self.minutes:.0f}m"
        elif self.hours < 5:
            return f"{self.hours:.2f}h"
        elif self.hours < 24:
            return f"{self.hours:.0f}h"
        elif self.days < 7:
            return f"{self.days:.2f}d"
        elif self.days < 30:
            return f"{self.days:.0f}d"
        elif self.days < 35:
            return f"{self.weeks:.2f}w"
        elif self.days < 730:
            return f"{self.weeks:.0f}w"
        else:
            return f"{self.years:.2f}y"

class Timer:
    def __init__(self, label=None, max_laps=None, wall_time=False):
        self.label = label
        self.end = None
        self.delta = None
        self.start = time.time_ns() if wall_time else time.perf_counter_ns()
        self.end = None
        self.laps = cl.deque(maxlen=max_laps)
        self.entered = False
        self.wall_time = wall_time

    @property
    def elapsed(self):
        return TimeDelta((time.time_ns() if self.wall_time else time.perf_counter_ns()) - self.start)

    def lap(self):
        lap = TimeDelta((time.time_ns() if self.wall_time else time.perf_counter_ns()) 
               - self.laps[-1] if self.laps else self.start)
        self.laps.append(lap)
        return lap

    @property
    def pace(self):
        return TimeDelta(sum(self.laps) / len(self.laps) if self.laps else 0)

    @property
    def str(self):
        return TimerStr(self)

    def __enter__(self):
        self.entered = True
        if self.label:
            print('⏱', self.label, end='... ')
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.end = time.time_ns() if self.wall_time else time.perf_counter_ns()
        self.delta = TimeDelta(self.end - self.start)
        if self.label:
            if self.entered:
                print(self.elapsed.display(), '🏁')
            else:
                print(f'{self.label}: {self.elapsed.display()}', '🏁')

    def stop(self):
        self.__exit__()
        return self.elapsed.display()


class TimerStr:
    def __init__(self, timer):
        self.timer = timer

    @property
    def elapsed(self):
        return self.timer.elapsed.display()

    @property
    def pace(self):
        return self.timer.pace.display()

    @property
    def delta(self):
        return self.timer.delta.display()

    @property
    def laps(self):
        return [lap.display() for lap in self.timer.laps]

    def __enter__(self):
        self.timer.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        message = self.timer.label
        self.timer.label = message or 'Elapsed'
        self.timer.stop()
        self.timer.label = message

    def stop(self):
        return self.timer.stop()


class WallTimer(Timer):
    def __init__(self, message=None, max_laps=None, wall_time=True):
        Timer.__init__(self, message, max_laps, wall_time)





