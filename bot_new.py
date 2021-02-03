from __future__ import annotations

import asyncio
import collections
import datetime
import inspect
from typing import Optional, Callable, Any

import discord
import uvloop as uvloop
from fuzzywuzzy import fuzz

uvloop.install()
client = discord.Client()

# defaults
DEFAULT_COUNTDOWN = 60
DEFAULT_DELTA = datetime.timedelta(seconds=DEFAULT_COUNTDOWN)

client.countdown = DEFAULT_COUNTDOWN
client.delta = DEFAULT_DELTA
client.queued = None


class Data:
    @staticmethod
    def get_queued() -> Optional[asyncio.Task]:
        return client.queued


class Util:
    @staticmethod
    def is_dank(content: str) -> bool:
        return content.startswith("d") and fuzz.partial_ratio(content, "dank") > 70

    @staticmethod
    def run_func(func: Callable) -> Optional[Any]:
        if inspect.iscoroutinefunction(func):
            return await func()
        else:
            return func()


class Registrations:
    controls = {}

    @staticmethod
    def register(store, key, value, **kwargs):
        if len(kwargs) > 0:
            store[key] = {
                "callback": value
            }
            store[key].update(kwargs)
        else:
            store[key] = value

    @staticmethod
    def control(*args, **kwargs):
        if len(kwargs) < 1:
            Registrations.register(Registrations.controls, args[0].__name__, args[0])
            return args[0]

        def decorator(func):
            Registrations.register(Registrations.controls, kwargs.get("name", func.__name__), func,
                                   condition=kwargs.get("condition"))
            return func

        return decorator

    @staticmethod
    def try_invoke(store, key) -> bool:
        invoked = store.get(key)
        if not invoked:
            return False
        if isinstance(invoked, collections.Mapping):
            condition = invoked.get("condition")
            if condition:
                if not Util.run_func(condition):
                    return
            callback = invoked["callback"]
        else:
            callback = invoked
        Util.run_func(callback)



@Registrations.control
def control_cancel():
    Data.get_queued().cancel("User canceled")


@Registrations.control
def control_now():
    Data.get_queued().cancel("Running now")
    await Data.get_queued().get_coro()()


def try_control(control: str) -> bool:
    control_func = controls.get(control)
    if control_func:
        control_func()
        return True
    else:
        return False


@client.event
async def on_message(message):
    if message.author == client.user or message.author.bot:
        return

    # normalize
    message.content = message.content.lower().strip()

    # disallow deviations disregarding d
    if not is_dank(message.content):
        return

    # if we are danking already, allow for the control commands
    if client.queued:
        # control word succeeded
        if try_control(message.content.split(" ")[-1]):
            return







