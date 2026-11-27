# -*- coding: utf-8 -*-

from importlib import import_module

from crawl.base import CHANNELS

for _name in (
    "github",
    "page",
    "repository",
    "script",
    "telegram",
):
    import_module(f"{__name__}.{_name}")

__all__ = ["CHANNELS"]
