from __future__ import annotations

import asyncio
import collections
import datetime
import inspect
import logging
import os

from threading import Lock
from typing import Optional, Callable, Any

import discord
import uvloop
import pytz

from fuzzywuzzy import fuzz

uvloop.install()
client = discord.Client()

# defaults
DEFAULT_COUNTDOWN = 60
TIMEZONE = pytz.timezone("America/New_York")
GAME_ROLES = {
    "dota": "261137719579770882"
}
GAMES = list(GAME_ROLES.keys())
DEFAULT_GAME = GAMES[0]
DATE_FORMAT = "%I:%M %p %Z"
GUILD_ID = os.getenv("DANK_GUILD")
CHANNEL_ID = os.getenv("DANK_CHANNEL")

client.countdown = DEFAULT_COUNTDOWN
client.datetime = None
client.queued = None
client.guild = None
client.channel = None
client.current_game = DEFAULT_GAME
client.dankers = []
client.lock = Lock()

log = logging.getLogger()


class Data:
    @staticmethod
    def get_datetime() -> datetime.datetime:
        return client.datetime

    @staticmethod
    def get_localtime():
        return Data.get_datetime().astimezone(tz=TIMEZONE)

    @staticmethod
    def get_formattime():
        return Data.get_localtime().strftime(DATE_FORMAT)

    @staticmethod
    def get_queued() -> Optional[asyncio.Task]:
        return client.queued

    @staticmethod
    def get_counter() -> str:
        return f"**({len(client.dankers)}/5)**"

    @staticmethod
    def get_guild() -> discord.Guild:
        return client.guild

    @staticmethod
    def get_channel() -> discord.TextChannel:
        return client.channel


class Util:
    @staticmethod
    def is_dank(content: str) -> bool:
        return content.startswith("d") and fuzz.partial_ratio(content, "dank") > 70

    @staticmethod
    def run_func(func: Callable, **kwargs) -> Optional[Any]:
        if inspect.iscoroutinefunction(func):
            return await func(kwargs)
        else:
            return func(kwargs)

    @staticmethod
    def represents_int(s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def get_possessive(user: discord.Member) -> str:
        name = user.display_name
        possess = "'" if name.endswith("s") else "'s"
        return f"{name}{possess}"


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
    def try_invoke(store, key, **kwargs) -> bool:
        invoked = store.get(key)
        if not invoked:
            return False
        if isinstance(invoked, collections.Mapping):
            condition = invoked.get("condition")
            if condition:
                if not Util.run_func(condition):
                    return False
            callback = invoked["callback"]
        else:
            callback = invoked
        Util.run_func(callback, kwargs)

    @staticmethod
    def try_control(key: str, user: discord.User) -> bool:
        return Registrations.try_invoke(Registrations.controls, key, user=user)


@Registrations.control("cancel")
def control_cancel():
    Data.get_queued().cancel("User canceled")


@Registrations.control("now")
def control_now():
    Data.get_queued().cancel("Running now")
    await Data.get_queued().get_coro()()


@Registrations.control("leave")
def control_leave(**kwargs):
    user = kwargs.get("user")
    client.dankers.remove(user)
    Data.get_channel().send(f"{user.display_name} left the dank. {Data.get_counter()}")


async def add_to_dank(user: discord.Member, channel: discord.TextChannel):
    # can't add twice
    if user in client.dankers:
        return

    # add to dankers
    client.dankers.append(user)

    name = user.display_name

    if client.countdown > DEFAULT_COUNTDOWN:
        # if it's not time to Dank Check yet, display the date.
        msg = f"{name} can dank at {Data.get_formattime()}. {Data.get_counter()}"
    else:
        # it's time to Dank Check!
        msg = f"{name} is ready to dank. {Data.get_counter()}"
        # refresh the countdown!
        client.countdown = DEFAULT_COUNTDOWN

    # send whatever message we need.
    await channel.send(msg)


@client.event
async def on_message(message):
    # must not be ourself or a bot
    if message.author == client.user or message.author.bot:
        return

    # must be in designated channel
    if message.channel.id != CHANNEL_ID:
        return

    # normalize
    message.content = message.content.lower().strip()

    if not Util.is_dank(message.content):
        return

    # do not let dank data to be concurrently accessed
    with client.lock:
        # if we are danking already, allow for the control commands
        if client.queued:
            # if the control word succeeded, nothing else to do
            if Registrations.try_control(message.content.split(" ")[-1], message.author):
                return
            # if not, opt into the dank
            await add_to_dank(message.author, message.channel)
            return

        # start a new dank


@client.event
async def on_ready():
    client.guild = discord.utils.get(client.guilds, id=GUILD_ID)
    if not client.guild:
        log.error("Guild not found")
        exit(1)

    client.channel = discord.utils.get(Data.get_guild().text_channels, id=CHANNEL_ID)
    if not client.channel:
        log.error("Channel not found")
        exit(1)
