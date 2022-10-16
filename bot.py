from __future__ import annotations

import asyncio
import dataclasses
import math
import os
import pathlib
import socket

from typing import Any, Callable

import aiohttp
import arrow
import dateparser
import datetime
import discord
import orjson

from BetterJSONStorage import BetterJSONStorage
from discord import Intents, AllowedMentions
from opendota.opendota import OpenDota
from pytz import timezone
from steam.steamid import SteamID, from_invite_code
from steam.webapi import WebAPI
from tinydb import TinyDB, Query
from thefuzz import fuzz

if os.name == "nt":
    # handle Windows imports
    # for colored terminal
    import colorama

    colorama.init()
else:
    # handle POSIX imports
    # for uvloop
    import uvloop

    uvloop.install()


def create_task(coro, *, name=None):
    task = asyncio.create_task(coro, name=name)
    return TaskWrapper(task)


def get_channel(channel: discord.TextChannel) -> discord.TextChannel:
    settings_channel = get_value("channel_id", table=client.settings_table)
    if settings_channel:
        guild = None
        for guild in client.guilds:
            break
        if guild is not None:
            settings_channel = guild.get_channel(settings_channel)
    return settings_channel or channel


class TaskWrapper:
    def __init__(self, task):
        self.task = task
        task.add_done_callback(self.on_task_done)

    def __getattr__(self, name):
        return getattr(self.task, name)

    def __await__(self):
        self.task.remove_done_callback(self.on_task_done)
        return self.task.__await__()

    def on_task_done(self, fut: asyncio.Future):
        if fut.cancelled() or not fut.done():
            return
        fut.result()

    def __str__(self):
        return f"TaskWrapper<task={self.task}>"


class GameClient(discord.Client):
    """
    The Discord client for this bot.
    """

    def __init__(self, *args, **kwargs):
        """
        Creates a new Discord client.
        """
        super().__init__(*args, **kwargs)

        self._resolver: aiohttp.AsyncResolver | None = None
        self._connector: aiohttp.TCPConnector | None = None

        self.current_game: Game | None = None
        self.current_match: Match | None = None
        self.now: datetime.datetime | None = None

        self.lock: asyncio.Lock | None = None

        self.db = TinyDB(
            pathlib.Path("./db.json"), access_mode="r+", storage=BetterJSONStorage
        )
        self.backup_table = self.db.table("backup")
        self.players_table = self.db.table("players")
        self.settings_table = self.db.table("settings")
        self.opendota = OpenDota(
            data_dir=".dota2",
            api_key=os.environ["GAME_BOT_OPENDOTA"]
        )
        self.steamapi = WebAPI(
            key=os.environ["GAME_BOT_STEAM"]
        )

    async def setup_hook(self) -> None:
        """
        Sets up the async resolver and
        """
        await super().setup_hook()

        self._resolver = aiohttp.AsyncResolver()
        self._connector = aiohttp.TCPConnector(
            resolver=self._resolver,
            family=socket.AF_INET,
            limit=0,
        )
        self.http.connector = self._connector

        self.lock = asyncio.Lock()

    async def on_ready(self):
        """
        Resumes a saved game.
        """
        guild = None
        for guild in self.guilds:
            break
        if guild is None:
            return
        self.now = utcnow()
        for save in self.backup_table.all():
            save = save["v"]
            channel = guild.get_channel(save["channel"])
            author = guild.get_member(save["author"])
            game_name = save["game_name"]
            restored_game = Game(channel, author, game_name)
            restored_game.group_buckets = {
                int(k): {guild.get_member(m) for m in v}
                for k, v in save["group_buckets"].items()
            }
            restored_game.gamer_buckets = {
                guild.get_member(int(k)): v for k, v in save["gamer_buckets"].items()
            }
            restored_game.future = datetime.datetime.fromisoformat(save["future"])
            restored_game.timestamp = save["timestamp"]
            restored_game.is_checking = save["is_checking"]
            restored_game.message = await channel.fetch_message(save["message"])
            restored_game.has_initial = save["has_initial"]
            restored_game.was_scheduled = save["was_scheduled"]
            restored_game.base_mention = save["base_mention"]
            self.current_game = restored_game
            self.current_game.start_countdown()
            break
        self.backup_table.truncate()

    async def on_message(self, message: discord.Message):
        """
        Handles new game messages.
        """
        try:
            # not a bot
            if message.author.bot:
                return

            # if only an embed, then there's no content to parse
            if not message.content:
                return

            # normalize
            message.content = message.content.lower()

            if is_game_command(message.content):
                async with self.lock:
                    # set our global now to when the message was made
                    self.now = message.created_at

                    # set up our arg parser
                    args = message.content.split()[1:]
                    gamer = message.author
                    options = GameOptions()

                    # consume all args
                    while args:
                        options = await consume_args(
                            args, gamer, message.created_at, get_channel(message.channel), options
                        )
                        # if we cleared options, then we stop here
                        if not options:
                            return

                    # are we going to start a game?
                    if not self.current_game:
                        # check if it's sufficiently in the future
                        if options.future:
                            delta = options.future - self.now
                            if delta < DEFAULT_DELTA:
                                options.future = None
                        # if didn't get a date, default to delta
                        if not options.future:
                            options.future = self.now + DEFAULT_DELTA
                        self.current_game = Game(get_channel(message.channel), gamer, options.game)
                        await self.current_game.start(options.future)

                    # add to game
                    await self.current_game.add_gamer(message.author, options.bucket)
        except Exception as e:
            print(e)
            await get_channel(message.channel).send("An unexpected error occurred.")


client: GameClient | None = None

Setting = Query()
Player = Query()
Store = Query()


def get_value(key: str, default: Any = None, table=None) -> Any:
    """
    Gets from the key value DB table.
    """
    table_interface = table or client.db
    res = table_interface.get(Store.k == key)
    if res:
        return res["v"]
    else:
        return default


def set_value(key: str, val: Any, table=None):
    """
    Sets to the key value DB table.
    """
    table_interface = table or client.db
    table_interface.upsert({"k": key, "v": val}, Store.k == key)


def update_value(update_fn: Callable[[Any], Any], key: str, default: Any = None, table=None) -> Any:
    """
    Gets an existing value in the key value DB table and updates it using update_fn.
    :return: The new value
    """
    table_interface = table or client.db
    old = get_value(key, default, table=table_interface)
    new = update_fn(old)
    set_value(key, new, table=table_interface)
    return new


TIMESTAMP_TIMEZONE = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=TIMESTAMP_TIMEZONE)
TIMESTAMP_GRANULARITY = datetime.timedelta(seconds=1)


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)


def generate_timestamp(dt: datetime.datetime) -> int:
    """
    Gets a UNIX timestamp representing a datetime.
    """
    delta = dt - EPOCH
    return delta // TIMESTAMP_GRANULARITY


def print_timestamp(timestamp: int, style: str | None = None) -> str:
    """
    Gets a Discord string representing a UNIX timestamp.
    """
    if style:
        return f"<t:{timestamp}:{style}>"
    else:
        return f"<t:{timestamp}>"


def generate_datetime(timestamp: int) -> datetime.datetime:
    """
    Gets a datetime from UNIX timestamp.
    """
    delta = datetime.timedelta(seconds=timestamp)
    return EPOCH + delta


BUCKET_MIN: int = 2

DEFAULT_COUNTDOWN = 120.0
MAX_CHECK_COUNTDOWN = 300.0
DEFAULT_DELTA = datetime.timedelta(seconds=DEFAULT_COUNTDOWN)


def increment(val: int) -> int:
    """
    Increments a given int by 1, functionally.
    """
    return val + 1


MATCH_POLL_INTERVAL = 5 * 60
MATCH_MAX_POLLS = 2 * 60 * 60 // MATCH_POLL_INTERVAL


class Match:
    pass


DOTA_RANKS = {
    0: "Uncalibrated",

    10: "Herald",
    11: "Herald [1]",
    12: "Herald [2]",
    13: "Herald [3]",
    14: "Herald [4]",
    15: "Herald [5]",
    16: "Herald [6]",
    17: "Herald [7]",

    20: "Guardian",
    21: "Guardian [1]",
    22: "Guardian [2]",
    23: "Guardian [3]",
    24: "Guardian [4]",
    25: "Guardian [5]",
    26: "Guardian [6]",
    27: "Guardian [7]",

    30: "Crusader",
    31: "Crusader [1]",
    32: "Crusader [2]",
    33: "Crusader [3]",
    34: "Crusader [4]",
    35: "Crusader [5]",
    36: "Crusader [6]",
    37: "Crusader [7]",

    40: "Archon",
    41: "Archon [1]",
    42: "Archon [2]",
    43: "Archon [3]",
    44: "Archon [4]",
    45: "Archon [5]",
    46: "Archon [6]",
    47: "Archon [7]",

    50: "Legend",
    51: "Legend [1]",
    52: "Legend [2]",
    53: "Legend [3]",
    54: "Legend [4]",
    55: "Legend [5]",
    56: "Legend [6]",
    57: "Legend [7]",

    60: "Ancient",
    61: "Ancient [1]",
    62: "Ancient [2]",
    63: "Ancient [3]",
    64: "Ancient [4]",
    65: "Ancient [5]",
    66: "Ancient [6]",
    67: "Ancient [7]",

    70: "Divine",
    71: "Divine [1]",
    72: "Divine [2]",
    73: "Divine [3]",
    74: "Divine [4]",
    75: "Divine [5]",
    76: "Divine [6]",
    77: "Divine [7]",

    80: "Immortal",
    81: "Immortal",
    82: "Immortal",
}


class DotaMatch(Match):
    steam_id: int
    party_size: int
    timestamp: int
    polls: int
    channel: discord.TextChannel
    task: TaskWrapper | None

    def __init__(self, steam_id: int, party_size: int, channel: discord.TextChannel):
        self.steam_id = steam_id
        self.party_size = party_size
        self.timestamp = generate_timestamp(utcnow())
        self.polls = 1
        self.channel = channel
        self.task = None
        self.start_check()

    def start_check(self):
        self.task = create_task(self.check_match(), name="Match Check")

    def close_match(self):
        self.task.cancel(msg="Closing match")

    def get_recent_match(self) -> dict[str, Any] | None:
        url = f"/players/{self.steam_id}/matches"
        data = {
            "limit": 1,
            "date": 1,
            "significant": 0
        }
        matches: list[dict[str, Any]] = client.opendota.get(url, data=data)
        if matches:
            match = matches[0]
            # if this match wasn't relevant for the game we started
            if match["party_size"] < self.party_size:
                return None
            # if this match ended prematurely
            if match["leaver_status"] != 0:
                return None
            # if this match started before the game started
            if match["start_time"] < self.timestamp:
                return None
            return match
        else:
            return None

    async def check_match(self):
        await asyncio.sleep(MATCH_POLL_INTERVAL)
        match = self.get_recent_match()
        if match:
            is_dire = match["player_slot"] > 127
            won = match["radiant_win"] ^ is_dire

            # create embed
            embed = discord.Embed(
                colour=discord.Colour.green() if won else discord.Colour.red(),
                title="Match Played",
            )

            # win or loss
            embed.add_field(name="Status", value="Win" if won else "Loss")

            # match type
            resources = client.opendota.get_constants(["lobby_type", "game_mode"])
            lobby_types = resources["lobby_type"]
            lobby_type: str = lobby_types[str(match["lobby_type"])]["name"]
            lobby_type = lobby_type.replace("lobby_type_", "").replace("_", " ").title()
            game_modes = resources["game_mode"]
            game_mode: str = game_modes[str(match["game_mode"])]["name"]
            game_mode = game_mode.replace("game_mode_", "").replace("_", " ").title()
            embed.add_field(name="Type", value=f"{lobby_type} {game_mode}")

            # timestamp
            embed.add_field(name="Time", value=print_timestamp(match["start_time"]))
            # rank
            embed.add_field(name="Rank", value=DOTA_RANKS.get(match["average_rank"]) or "Unknown")

            # duration
            minutes, seconds = divmod(match["duration"], 60)
            embed.add_field(name="Duration", value=f"{minutes}:{seconds:02}")

            # send
            await self.channel.send(embed=embed)
        self.polls += 1
        if not match and self.polls < MATCH_MAX_POLLS:
            self.start_check()
        else:
            client.current_match = None


class Game:
    """
    Represents a pending/active Game.
    """

    group_buckets: dict[int, set[discord.Member]]
    gamer_buckets: dict[discord.Member, int]
    future: datetime.datetime
    timestamp: int
    is_checking: bool
    author: discord.Member
    channel: discord.TextChannel
    game_name: str
    message: discord.Message | None
    task: TaskWrapper | None
    has_initial: bool
    was_scheduled: bool | None
    base_mention: str | None

    def __init__(
        self,
        channel: discord.TextChannel,
        author: discord.Member,
        game_name: str = None,
    ):
        """
        Creates a new Game, to be started with start().
        """
        self.game_name = game_name if game_name else DEFAULT_GAME

        self.reset()

        self.author = author
        self.channel = channel

        self.task = None

        self.has_initial = False
        self.was_scheduled = None

    def save(self):
        """
        Saves the Game to disk, so that it may be resumed in case of a crash/update/restart.

        Must be called any time any of the class properties change.
        """
        data = {
            "group_buckets": {
                str(k): [m.id for m in v] for k, v in self.group_buckets.items()
            },
            "gamer_buckets": {str(k.id): v for k, v in self.gamer_buckets.items()},
            "future": self.future.isoformat(),
            "timestamp": self.timestamp,
            "is_checking": self.is_checking,
            "author": self.author.id,
            "channel": self.channel.id,
            "game_name": self.game_name,
            "message": self.message.id,
            "has_initial": self.has_initial,
            "was_scheduled": self.was_scheduled,
            "base_mention": self.base_mention,
        }
        set_value("saved", data, table=client.backup_table)

    def reset(self):
        """
        Resets the game to require new confirmation, for starting a Game Check.
        """
        self.group_buckets = dict()

        game_min = GAME_DATA[self.game_name].get("min", BUCKET_MIN)
        game_max = GAME_DATA[self.game_name].get("max", game_min)
        bucket_range_max = game_max + 1
        for bucket in range(game_min, bucket_range_max):
            self.group_buckets[bucket] = set()
        self.gamer_buckets = dict()
        self.message = None
        self.base_mention = None

    async def start(self, future: datetime.datetime, mention: str = None):
        """
        Starts the game.
        """
        self.update_future(future)
        await self.initialize(mention=mention)
        self.save()

    def update_future(self, future: datetime.datetime):
        """
        Sets the time the game finishes.
        """
        self.future = future
        self.timestamp = generate_timestamp(future)

        countdown = max(DEFAULT_COUNTDOWN, self.get_delta_seconds())
        self.is_checking = countdown <= MAX_CHECK_COUNTDOWN
        if self.was_scheduled is None:
            self.was_scheduled = not self.is_checking

    async def initialize(self, mention: str = None):
        """
        Starts/schedules the game check.
        """
        if self.base_mention is None:
            self.base_mention = (
                f"<@&{GAME_DATA[self.game_name]['role']}>"
                if mention is None
                else mention
            )
        name = self.author.display_name
        relative_time = print_timestamp(self.timestamp, "R")
        if self.is_checking:
            msg = f"{self.base_mention} {name} requested a {KEYWORD_TITLE} Check. (expires {relative_time})"
        else:
            short_time = print_timestamp(self.timestamp, "t")
            msg = f"{self.base_mention} {name} scheduled a {KEYWORD} at {short_time} ({relative_time})."
        if self.message:
            await self.update_message(msg)
        else:
            self.message = await self.channel.send(msg)
        self.start_countdown()

    def get_delta(self) -> datetime.timedelta:
        """
        Gets the timedelta until the game finishes.
        """
        return self.future - client.now

    def get_delta_seconds(self) -> float:
        """
        Gets the number of seconds left until the game finishes.
        """
        return self.get_delta().total_seconds()

    async def update_message(self, content: str):
        """
        Directly edits the message and updates its reference.
        """
        self.message = await self.message.edit(content=content)

    async def replace_message(self, old: str, new: str):
        """
        Used to replace content within the message.
        """
        await self.update_message(self.message.content.replace(old, new))

    async def add_gamer(self, gamer: discord.Member, min_bucket: int):
        """
        Sets a gamer to the specified buckets.
        """
        game_min = GAME_DATA[self.game_name].get("min", BUCKET_MIN)
        game_max = GAME_DATA[self.game_name].get("max", game_min)
        bucket_range_max = game_max + 1

        # out of bounds buckets
        min_bucket = max(game_min, min(min_bucket, game_max))

        # remove them first if this is an update
        current_min_bucket = self.gamer_buckets.get(gamer)
        if current_min_bucket:
            if current_min_bucket != min_bucket:
                await self.remove_gamer(gamer, notify=False)
            else:
                return

        # add them from that bucket and beyond
        self.gamer_buckets[gamer] = min_bucket
        for bucket in range(min_bucket, bucket_range_max):
            self.group_buckets[bucket].add(gamer)

        if self.has_initial:
            name = gamer.display_name
            size = len(self.gamer_buckets)
            size = f"**({size}/5)**"
            with_str = (
                f" with {min_bucket} {KEYWORD}{KEYWORD_SUBJECT_SUFFIX}"
                if min_bucket > BUCKET_MIN
                else ""
            )
            if self.is_checking:
                msg = f"{name} is ready to {KEYWORD}{with_str}. {size}"
                countdown = self.get_delta_seconds()
                if 5 < countdown < DEFAULT_COUNTDOWN:
                    missing_countdown = datetime.timedelta(
                        seconds=DEFAULT_COUNTDOWN - self.get_delta_seconds()
                    )
                    await self.refresh(self.future + missing_countdown)
            else:
                short_time = print_timestamp(self.timestamp, "t")
                relative_time = print_timestamp(self.timestamp, "R")
                msg = f"{name} can {KEYWORD} at {short_time} ({relative_time}){with_str}. {size}"
            await self.channel.send(msg)
        else:
            self.has_initial = True

        self.save()

    async def remove_gamer(self, gamer: discord.Member, notify: bool = True):
        """
        Removes a gamer from all buckets.
        """
        # pop off the gamer lookup
        min_bucket = self.gamer_buckets.pop(gamer)

        game_min = GAME_DATA[self.game_name].get("min", BUCKET_MIN)
        game_max = GAME_DATA[self.game_name].get("max", game_min)
        bucket_range_max = game_max + 1

        # remove from all groups
        for bucket in range(min_bucket, bucket_range_max):
            self.group_buckets[bucket].remove(gamer)

        if notify:
            await self.channel.send(f"You left the {KEYWORD}.")

        self.save()

    def start_countdown(self):
        """
        Directly starts the asyncio countdown task.
        """
        self.task = create_task(self.countdown(), name="Countdown")

    async def refresh(self, future: datetime.datetime):
        """
        Refreshes the game with a new countdown.
        """
        self.cancel_task(reason="Refreshing")
        await self.start(future)

    async def countdown(self):
        """
        Sleeps for the countdown time, and then finishes the game.
        """
        delta = self.get_delta_seconds()
        # if our delta is less than 16 milliseconds, then we don't expect scheduling accuracy and thus don't sleep
        if delta >= 0.016:
            await asyncio.sleep(self.get_delta_seconds())
        await self.finish()

    async def finish(self):
        """
        Either finishes the game check, or starts one if it is scheduled.
        """
        gamers = self.get_gamers()
        num_gamers = len(gamers)
        if num_gamers > 1:
            # print out the message
            mention = " ".join([gamer.mention for gamer in gamers])
            if self.is_checking:
                # finish the game
                await self.channel.send(
                    f"{mention} {KEYWORD_TITLE} Check complete. **{num_gamers}/5** players ready to {KEYWORD}."
                )
                # we had a game
                set_value("no_gamers_consecutive", 0)
                # track dota matches
                if self.game_name == "dota":
                    steam_id = None
                    for gamer in gamers:
                        player = client.players_table.get(Player.id == gamer.id)
                        if player:
                            steam_id = player["steam"]
                            break
                    if steam_id:
                        client.current_match = DotaMatch(
                            steam_id=steam_id,
                            party_size=num_gamers,
                            channel=self.channel
                        )
            else:
                # start the game up again
                client.now = utcnow()
                self.reset()
                create_task(self.start(client.now + DEFAULT_DELTA, mention=mention))
                return
        else:
            no_gamers = update_value(increment, "no_gamers", 0)
            no_gamers_consecutive = update_value(increment, "no_gamers_consecutive", 0)
            await self.channel.send(
                f"No {KEYWORD}{KEYWORD_SUBJECT_SUFFIX} found for the {KEYWORD}. This server has gone {no_gamers} {KEYWORD}s without a {KEYWORD}. ({no_gamers_consecutive} in a row)."
            )
            if EXTRA_FAILURE_MESSAGE:
                await self.channel.send(EXTRA_FAILURE_MESSAGE)
        if self.is_checking:
            # make it past tense
            await self.replace_message("expires", "expired")
        client.current_game = None
        client.backup_table.truncate()

    def cancel_task(self, reason: str = "Cancelled"):
        """
        Directly cancels the asyncio countdown task.
        """
        self.task.cancel(msg=reason)

    async def update_timestamp(self, new_future: datetime.datetime):
        """
        Updates the timestamp in the message.
        """
        if self.message is None:
            return
        old_relative_time = str(self.timestamp)
        self.update_future(new_future)
        new_relative_time = str(self.timestamp)
        await self.replace_message(old_relative_time, new_relative_time)

    async def cancel(self, now: datetime.datetime):
        """
        Handles cancelling the game.
        """
        self.cancel_task()
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
            await self.replace_message("expires", "cancelled")
        client.current_game = None
        client.backup_table.truncate()
        await self.channel.send(f"{KEYWORD_TITLE} cancelled.")

    async def advance(self, now: datetime.datetime):
        """
        Skips ahead to finish the game now.
        """
        self.cancel_task(reason="Advancing")
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
        await self.finish()

    def get_gamers(self) -> set[discord.Member]:
        """
        Gets available gamers, according to the largest satisfied bucket.
        """
        bucket = set()

        game_min = GAME_DATA[self.game_name].get("min", BUCKET_MIN)
        game_max = GAME_DATA[self.game_name].get("max", game_min)
        bucket_range_max = game_max + 1

        # count down from the biggest bucket, so we can stop at the biggest that satisfies
        for i in reversed(range(game_min, bucket_range_max)):
            # get the bucket from index
            candidate_bucket = self.group_buckets[i]
            # get the bucket's size
            size = len(candidate_bucket)
            # is the size enough? ex: for bucket #5, we need at least 5 gamers
            if size >= i:
                bucket = candidate_bucket
                break
        return bucket


def get_int(s: str, default: int | None = 0) -> int | None:
    """
    Tries to get an int from a string, if fails, returns default.
    """
    try:
        val = int(s)
        return val
    except ValueError:
        return default


def get_float(s: str, default: float | None = 0.0) -> float:
    """
    Tries to get a float from a string, if fails, returns default.
    """
    try:
        val = float(s)
        return val
    except ValueError:
        return default


@dataclasses.dataclass(init=False)
class GameOptions:
    """
    Represents arguments to the game.
    """

    future: datetime.datetime | None
    bucket: int
    game: str | None

    def __init__(self):
        """
        Initializes a blank/default options.
        """
        self.future = None
        self.game = None
        self.bucket = BUCKET_MIN


CURRENT_GAME_ARGS = {"cancel", "now", "leave"}

HUMANIZE_VOWEL_WORDS = {"hour"}
HUMANIZE_MAPPING = {
    "years": (12.0, "months"),
    "months": (4.0, "weeks"),
    "weeks": (7.0, "days"),
    "days": (24.0, "hours"),
    "hours": (60.0, "minutes"),
    "minutes": (60.0, "seconds"),
}
HUMANIZE_SHORTHAND = {
    "mins": "minutes",
    "min": "minutes",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "secs": "seconds",
    "sec": "seconds",
    "s": "seconds",
}
NUMERIC = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "-", "."}


def convert_humanize_decimal(quantity: float, unit: str) -> str:
    frac, whole = math.modf(quantity)
    base = f"{int(quantity)} {unit}"
    # if this isn't a significant float, then we can just return it back
    if abs(frac) < 0.1:
        return base
    mapping = HUMANIZE_MAPPING.get(unit)
    # if there's no further conversion, we just deal with the lower precision
    if not mapping:
        return base
    conversion, lesser_unit = mapping
    lesser_quantity = frac * conversion
    return f"{base}, {convert_humanize_decimal(lesser_quantity, lesser_unit)}"


def try_steam_id(steam_id: str | int) -> SteamID | None:
    try:
        return SteamID(steam_id)
    except:
        return None


async def consume_args(
    args: list[str],
    gamer: discord.Member,
    created_at: datetime.datetime,
    channel: discord.TextChannel,
    options: GameOptions,
) -> GameOptions | None:
    """
    Handles building options from command arguments by parsing args.

    Returning None means we don't interact with the game.
    """
    control = args.pop(0)
    # if there's a game, try to control it
    if client.current_game:
        if control == "cancel":
            await client.current_game.cancel(created_at)
            return None
        if control == "now":
            await client.current_game.advance(created_at)
            return None
        if control == "leave":
            await client.current_game.remove_gamer(gamer)
            return None
    else:
        # sometimes users accidentally try to control a game when it doesn't exist
        if control in CURRENT_GAME_ARGS:
            return None
        # we need more args than the control for these
        if args:
            # future date handling
            if options.future is None:
                if control == "at":
                    date_string = ""
                    end = 0
                    new_start = 0
                    last = len(args)
                    confirmed_date = None
                    # go through until we get a date
                    while True:
                        word = args[end]
                        # combine if space
                        if last > end + 1:
                            period = args[end + 1]
                            if period.startswith("p") or period.startswith("a"):
                                word += period
                                end += 1
                        local_now = client.now.astimezone(LOCAL_TZINFO)
                        # use pm and am, not p or a
                        if word.endswith("p") or word.endswith("a"):
                            word += "m"
                        elif not word.endswith("pm") and not word.endswith("am"):
                            # if there's no period at all, just autodetect based on current
                            # TODO: probably want to detect if the time has past, so we can flip am or pm
                            word += "am" if local_now.hour < 12 else "pm"
                        just_time = word[:-2]
                        # if it's just a single int representing the hour, normalize it into a full time
                        if get_int(just_time, None) is not None:
                            word = just_time + ":00" + word[-2:]
                        date_string += " " + word if date_string else word
                        if LOCAL_TIMEZONE != TIMESTAMP_TIMEZONE:
                            settings = {
                                "TIMEZONE": LOCAL_TIMEZONE,
                                "TO_TIMEZONE": "UTC",
                            }
                            # if UTC time is in the next day, we need to act like we're getting a time in the past
                            # because our local time zone is in the previous day, likewise with future
                            if (
                                client.now.day > local_now.day
                                or client.now.month > local_now.month
                                or client.now.year > local_now.year
                            ):
                                settings["PREFER_DATES_FROM"] = "past"
                            elif (
                                client.now.day < local_now.day
                                or client.now.month < local_now.month
                                or client.now.year < local_now.year
                            ):
                                settings["PREFER_DATES_FROM"] = "future"
                        else:
                            settings = None
                        attempt_date = dateparser.parse(
                            date_string, languages=["en"], settings=settings
                        )
                        # go to next arg
                        end += 1
                        # we made a new date
                        if attempt_date and confirmed_date != attempt_date:
                            # now we know our new start
                            new_start = end
                            confirmed_date = attempt_date
                        # if we surpass the last arg, end
                        if end >= last:
                            break
                    # consume our peeked inputs to the date
                    del args[:new_start]
                    if confirmed_date:
                        if confirmed_date.tzinfo is None:
                            confirmed_date = confirmed_date.replace(
                                tzinfo=TIMESTAMP_TIMEZONE
                            )
                        options.future = confirmed_date
                    return options
                if control == "in":
                    arw = arrow.get(client.now)
                    date_string = "in"
                    end = 0
                    new_start = 0
                    last = len(args)
                    confirmed_date = None
                    # go through until we get a date
                    while True:
                        word = args[end]
                        # if it's a shorthand quantity, ex. 1h, 5m, separate them out to normalize for the parser
                        if word[0] in NUMERIC and word[len(word) - 1] not in NUMERIC:
                            i = 0
                            for i, c in enumerate(word):
                                if c not in NUMERIC:
                                    break
                            args.insert(end + 1, word[i:])
                            word = word[:i]
                            last = len(args)
                        if last > end + 1:
                            noun = args[end + 1]
                            # replace shorthand with longform unit, as required by the parser
                            longform_noun = HUMANIZE_SHORTHAND.get(noun)
                            if longform_noun is not None:
                                noun = longform_noun
                                args[end + 1] = noun
                            # using a quantity must require you to use plural units.
                            # if you don't for 1, you must use "a" or "an"
                            if word == "1":
                                if not noun.endswith("s"):
                                    if noun in HUMANIZE_VOWEL_WORDS:
                                        word = "an"
                                    else:
                                        word = "a"
                            else:
                                # if it's a decimal quantity, convert it to the combination of units needed
                                parsed_num = get_float(word, default=None)
                                if parsed_num is not None:
                                    word = convert_humanize_decimal(parsed_num, noun)
                                    # we also consumed the next word
                                    end += 1
                        date_string += " " + word
                        try:
                            attempt_date = arw.dehumanize(date_string, locale="en")
                        except ValueError:
                            # didn't work
                            attempt_date = None
                        # go to next arg
                        end += 1
                        # we made a new date
                        if attempt_date and confirmed_date != attempt_date.datetime:
                            # now we know our new start
                            new_start = end
                            confirmed_date = attempt_date.datetime
                        # if we surpass the last arg, end
                        if end >= last:
                            break
                    # consume our peeked inputs to the date
                    del args[:new_start]
                    options.future = confirmed_date
                    return options
        else:
            if control == "for":
                game = args.pop(0)
                if game in GAMES:
                    options.game = game
                    return options

    if args:
        if control == "register":
            id_arg = args.pop(0)
            # try to parse a Steam ID if there is one
            steam_id = try_steam_id(id_arg)
            # try friend code
            if not steam_id:
                steam_id = from_invite_code(id_arg)
            # try user profile
            if not steam_id:
                friend_code = id_arg
                if "steamcommunity.com/user/" in friend_code:
                    if not friend_code.startswith("http"):
                        friend_code = "https://" + friend_code
                    friend_code = friend_code.replace("steamcommunity.com/user", "https://s.team/p")
                    steam_id = from_invite_code(friend_code)
            # try profiles URL
            if not steam_id:
                profile_id = id_arg
                if "steamcommunity.com/profiles/" in profile_id:
                    if not profile_id.startswith("http"):
                        profile_id = "https://" + profile_id
                    profile_id = profile_id.replace("steamcommunity.com/profiles/", "")
                    profile_id = profile_id.replace("https://", "")
                    profile_id = profile_id.replace("http://", "")
                    steam_id = try_steam_id(profile_id)
            # try vanity URL
            if not steam_id:
                vanity = id_arg
                if "steamcommunity.com/id/" in vanity:
                    if not vanity.startswith("http"):
                        new_id_arg = "https://" + vanity
                    vanity = vanity.replace("steamcommunity.com/id/", "")
                    vanity = vanity.replace("https://", "")
                    vanity = vanity.replace("http://", "")
                # either a /id/ URL or a raw vanity
                resp = client.steamapi.ISteamUser.ResolveVanityURL(vanityurl=vanity)
                vanity = resp.get("response", {}).get("steamid")
                if vanity:
                    steam_id = try_steam_id(vanity)
            if not steam_id:
                await channel.send("Steam ID not found.")
                return None
            client.players_table.upsert({"id": gamer.id, "steam": steam_id.as_32}, Player.id == gamer.id)
            await channel.send("Steam ID linked. Matches will now be listed.")
            return None
        if control == "option":
            if not gamer.guild_permissions.administrator:
                await channel.send("Not permitted to set/get options.")
                return None
            option_mode = args.pop(0)
            option = args.pop(0)
            if option_mode == "set":
                new_value = args.pop(0)
                if "id" in option:
                    tmp = new_value
                    new_value = get_int(new_value, default=None)
                    if new_value is None:
                        new_value = tmp

                def set_new_value(old):
                    return new_value

                new_value = update_value(set_new_value, option, client.settings_table)
                await channel.send(f"{option}={new_value}")
            else:
                await channel.send(f"{option}={get_value(option, client.settings_table)}")
            return None
        # if buckets
        if control == "if":
            options.bucket = get_int(args.pop(0), BUCKET_MIN)
            return options

    # if we didn't find the control, just ignore
    return options


FUZZ_THRESHOLD = 70


def is_game_command(content: str) -> bool:
    """
    Checks if the message represents a game "command"
    """
    # if it is the word, passes
    word = content.split(maxsplit=1)[0]
    if word.startswith(KEYWORD):
        return True
    # has to start with the letter
    if not word.startswith(KEYWORD[0]):
        return False
    # fuzz it
    ratio = fuzz.ratio(word, KEYWORD)
    return ratio > 70


async def main():
    """
    Main function for running the client.
    """

    global client

    # limit the events we get to the ones required
    intents = Intents.none()
    intents.guild_messages = True
    intents.guilds = True
    intents.members = True
    intents.message_content = True

    # limit the mentions to the ones required
    mentions = AllowedMentions.none()
    mentions.users = True
    mentions.roles = True

    # create our client, limit messages to what we need to keep track of
    client = GameClient(
        max_messages=500,
        intents=intents,
        allowed_mentions=mentions,
    )

    # start the client
    async with client as _client:
        await _client.start(os.environ["GAME_BOT_TOKEN"])


if os.name == "nt":
    # On Windows, the selector event loop is required for aiodns and avoiding exceptions on exit
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

with open("settings.json", "rb") as f:
    config = orjson.loads(f.read())

    LOCAL_TIMEZONE = config.get("local_timezone", "US/Eastern")
    LOCAL_TZINFO = timezone(LOCAL_TIMEZONE)

    KEYWORD = config.get("keyword", "game")
    KEYWORD_SUBJECT_SUFFIX = "rs" if KEYWORD.endswith("e") else "ers"
    KEYWORD_TITLE = KEYWORD[0].upper() + KEYWORD[1:]

    EXTRA_FAILURE_MESSAGE = config.get("failure_message", None)

    GAME_DATA = config["games"]
    GAMES = list(GAME_DATA.keys())
    DEFAULT_GAME = GAMES[0]


discord.utils.setup_logging()

asyncio.run(main())
