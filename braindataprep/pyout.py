# Derived from `dandi.support.pyout`
# https://github.com/dandi/dandi-cli/blob/master/dandi/support/pyout.py
# Apache License Version 2.0
import sys
import pyout
import humanize
import datetime
import time
import logging
from typing import Any
from contextlib import contextmanager
from collections import Counter

lg = logging.getLogger(__name__)


Status = dict[str, Any]


def naturalsize(v):
    """Format a number of bytes like a human readable filesize"""
    if v in ["", None]:
        return ""
    return humanize.naturalsize(v)


def datefmt(v, fmt="%Y-%m-%d/%H:%M:%S"):
    """Format a date/time"""
    if isinstance(v, datetime.datetime):
        return v.strftime(fmt)
    else:
        return time.strftime(fmt, time.localtime(v))


# def empty_for_none(v):
#     return "" if v is None else v


def summary_dates(values):
    return (
        ["%s>" % datefmt(min(values)), "%s<" % datefmt(max(values))]
        if values else []
    )


def counts(values):
    return [f"{v:d} {k}" for k, v in Counter(values).items()]


# class mapped_counts(object):
#     def __init__(self, mapping):
#         self._mapping = mapping
#
#     def __call__(self, values):
#         mapped = [self._mapping.get(v, v) for v in values]
#         return counts(mapped)


def get_style(hide_if_missing=True, has_size=True):
    KB = 1024
    MB = 1024**2
    GB = 1024**3
    progress_style = dict(  # % done
        transform=lambda f: "%d%%" % f,
        align="right",
        color=dict(
            interval=[
                [0, 10, "red"],
                [10, 100, "yellow"],
                [100, None, "green"]
            ]
        ),
    )
    size_style = dict(
        transform=naturalsize,
        color=dict(
            interval=[
                [0, KB, "blue"],
                [KB, MB, "green"],
                [MB, GB, "red"],
                [GB, None, "orangered"],
            ]
        ),
        aggregate=lambda x: naturalsize(sum([y for y in x if y is not None])),
        # summary=sum,
    )
    speed_style = dict(
        transform=lambda f: "%s/s" % naturalsize(f),
        color=dict(
            interval=[
                [0, MB, "orangered"],
                [MB, 10*MB, "red"],
                [10*MB, 100*MB, "yellow"],
                [100*MB, GB, "greenyellow"],
                [GB, None, "green"],
            ]
        ),
    )
    STYLE = {
        "summary_": {"bold": True},
        "header_": dict(bold=True),
        "default_": dict(missing=""),
        "path": dict(
            bold=True,
            align="left",
            underline=True,
            width=dict(
                truncate="left",
                min=20,
                # min=max_filename_len + 4 #  .../
                # min=0.3  # not supported yet by pyout,
                # https://github.com/pyout/pyout/issues/85
            ),
            aggregate=lambda _: "Summary:"
            # TODO: seems to be wrong
            # width="auto"
            # summary=lambda x: "TOTAL: %d" % len(x)
        ),
        "session_start_time": dict(
            transform=datefmt,
            aggregate=summary_dates,
            # summary=summary_dates
        ),
        "errors": dict(
            align="center",
            color=dict(interval=[[0, 1, "green"], [1, None, "red"]]),
            aggregate=lambda x: (
                f"{sum(map(bool, x))} with errors"
                if any(x) else ""
            ),
        ),
        "status": dict(
            color=dict(lookup={
                "skipped": "yellow",
                "done": "green",
                "error": "red"
            }),
            aggregate=counts,
        ),
        "message": dict(
            color=dict(
                re_lookup=[
                    ["^exists", "yellow"],
                    ["^(failed|error|ERROR)", "red"]
                ]
            ),
            aggregate=counts,
        ),
        "checksum": dict(
            align="center",
            color=dict(
                re_lookup=[
                    ["ok", "green"],
                    ["^(-|NA|N/A)", "yellow"],
                    ["^(differ|failed|error|ERROR)", "red"],
                ]
            ),
        ),
        "size": dict(size_style),
        "done": dict(size_style),
        "done%": dict(progress_style),
        "dspeed": dict(speed_style),
        "wspeed": dict(speed_style),
    }
    if hide_if_missing:
        # To just quickly switch for testing released or not released (with
        # hide)
        # pyout
        if "hide" in pyout.elements.schema["definitions"]:
            lg.debug("pyout with 'hide' support detected")
            STYLE["default_"]["hide"] = "if_missing"
            # to avoid https://github.com/pyout/pyout/pull/102
            for f in STYLE:
                if not f.endswith("_"):
                    STYLE[f]["hide"] = "if_missing"
            # but make always visible for some
            for f in ("path",):
                STYLE[f]["hide"] = False
        else:
            lg.warning(
                "pyout without 'hide' support. Expect too many columns"
            )

    if not sys.stdout.isatty():
        # TODO: ATM width in the final mode is hardcoded
        #  https://github.com/pyout/pyout/issues/70
        # and depending on how it would be resolved, there might be a
        # need to specify it here as "max" or smth like that.
        # For now hardcoding to hopefully wide enough 200 if stdout is not
        # a tty
        STYLE["width_"] = 200

    return STYLE


def get_style_bidsify():
    ONGOING = "○"
    PENDING = "..."
    DONE = "done"
    ERROR = "error"
    SKIP = "skipped"
    # done%
    progress_style = dict(
        transform=lambda f: "%d%%" % f,
        align="right",
        color=dict(
            interval=[
                [0, 10, "red"],
                [10, 100, "yellow"],
                [100, None, "green"]
            ]
        ),
    )
    return {
        "summary_": {"bold": True},
        "header_": dict(bold=True),
        "default_": dict(missing=""),
        "modality": dict(
            bold=True,
            align="left",
            underline=True,
            width=dict(
                truncate="left",
                min=20,
                # min=max_filename_len + 4 #  .../
                # min=0.3  # not supported yet by pyout,
                # https://github.com/pyout/pyout/issues/85
            ),
            aggregate=lambda _: "Summary:"
            # TODO: seems to be wrong
            # width="auto"
            # summary=lambda x: "TOTAL: %d" % len(x)
        ),
        "status": dict(
            color=dict(lookup={
                SKIP: "blue",
                ONGOING: "yellow",
                PENDING: "yellow",
                DONE: "green",
                ERROR: "red",
            }),
            aggregate=counts,
        ),
        "message": dict(
            color=dict(
                re_lookup=[
                    ["^exists", "yellow"],
                    ["^(failed|error|ERROR)", "red"]
                ]
            ),
            aggregate=counts,
        ),
        "done%": dict(progress_style),
    }


class LogSafeTabular(pyout.Tabular):

    @staticmethod
    def exclude_all(r):
        return False

    def __enter__(self):
        super().__enter__()
        root = logging.getLogger()
        if root.handlers:
            for h in root.handlers:
                # Use `type()` instead of `isinstance()` because FileHandler is
                # a subclass of StreamHandler, and we don"t want to disable it:
                if type(h) is logging.StreamHandler:
                    h.addFilter(self.exclude_all)
            self.__added_handler = None
        else:
            self.__added_handler = logging.NullHandler()
            root.addHandler(self.__added_handler)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        try:
            super().__exit__(exc_type, exc_value, tb)
        finally:
            root = logging.getLogger()
            for h in root.handlers:
                # Use `type()` instead of `isinstance()` because FileHandler is
                # a subclass of StreamHandler, and we don"t want to disable it:
                if type(h) is logging.StreamHandler:
                    h.removeFilter(self.exclude_all)
            if self.__added_handler is not None:
                root.removeHandler(self.__added_handler)
                self.__added_handler = None


class MemorizingTabular(LogSafeTabular):
    """
    A Tabular environement that memorizes certain status

    It adds the `memorize` option, which takes a string or list of
    strings. If the "status" value of an update is one of these keys,
    the update is saved in the `memorized` dictionary.

    Attributes
    ----------
    memorize : tuple[str]
        "status" keys that should be memorized
    memorized : dict[str, list[dict]]
        Mapping form each "status" key to the list of memorized updates
    """

    def __init__(self, *args, memorize="error", **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(memorize, str):
            memorize = [memorize]
        self.memorize = tuple(memorize)
        self.memorized = {key: [] for key in memorize}

    def __call__(self, status):
        key = status.get("status", "")
        if key in self.memorize:
            self.memorized[key].append(status)
        return super().__call__(status)


@contextmanager
def memory_tab(main: MemorizingTabular, summary: LogSafeTabular):

    # 1. Use main tabular context
    with main:
        yield main

    # 2. Use summary tabular context to display memorized statuses
    if any(map(len, main.memorized.values())):
        with summary:
            for statuses in main.memorize.values():
                for status in statuses:
                    summary(status)


@contextmanager
def bidsify_tab(hide_if_missing=True):
    ONGOING = "○"
    PENDING = "..."
    DONE = "done"
    ERROR = "error"
    SKIP = "skipped"

    # 0. Setup Tabulars

    common_style = {
        "summary_": {"bold": True},
        "header_": {"bold": True},
        "default_": {"missing": ""},
        "status": {
            "color": {"lookup": {
                SKIP: "blue",
                ONGOING: "yellow",
                PENDING: "yellow",
                DONE: "green",
                ERROR: "red",
            }},
            "aggregate": counts,
        },
        "message": {
            "color": {
                "re_lookup": [
                    ["^exists", "yellow"],
                    ["^(failed|error|ERROR)", "red"]
                ]
            },
            "aggregate": counts,
        },
    }

    main_columns = [
        "modality",
        "progress"
        "path",
        "checksum",
        "status",
        "message",
        "errors",
        "skipped",
    ]

    main_style = {
        **common_style,
        "modality": {
            "bold": True,
            "align": "left",
            "underline": True,
            "width": {
                "truncate": "left",
                "min": 20,
            },
            "aggregate": lambda _: "Summary:",
        },
        "progress": {
            "transform": lambda f: "%d%%" % f,
            "align": "right",
            "color": {
                "interval": [
                    [0, 10, "red"],
                    [10, 100, "yellow"],
                    [100, None, "green"]
                ]
            },
        },
        "errors": {"color": "red"},
        "skipped": {"color": "blue"},
    }

    summary_columns = [
        "path",
        "status",
        "message",
    ]

    summary_style = {
        **common_style,
        "path": {
            "bold": True,
            "align": "left",
            "underline": True,
            "width": {
                "truncate": "left",
                "min": 20,
            },
            "aggregate": lambda _: "Summary:",
        },
    }

    if hide_if_missing:
        # To just quickly switch for testing released or not released
        # (with hide) pyout
        if "hide" in pyout.elements.schema["definitions"]:
            lg.debug("pyout with 'hide' support detected")
            for style in (main_style, summary_style):
                style["default_"]["hide"] = "if_missing"
                # to avoid https://github.com/pyout/pyout/pull/102
                for f in style:
                    if not f.endswith("_"):
                        style[f]["hide"] = "if_missing"
            # but make always visible for some
            main_style["modality"]["hide"] = False
            summary_style["path"]["hide"] = False
        else:
            lg.warning(
                "pyout without 'hide' support. Expect too many columns"
            )

    main = MemorizingTabular(main_columns, memorize="error", style=main_style)
    summary = LogSafeTabular(summary_columns, style=summary_style)

    # 1. Use main tabular context
    with main:
        yield main

    # 2. Use summary tabular context to display memorized statuses
    if any(map(len, main.memorized.values())):
        print('')
        with summary:
            for statuses in main.memorized.values():
                for status in statuses:
                    if not status.get('path', ''):
                        continue
                    summary(status)
