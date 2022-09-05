import asyncio
import dataclasses
import math
import os
import pathlib
import socket

from typing import Dict, Set, List, Optional, Any, Callable

import aiohttp
import discord
import datetime
import dateparser
import arrow

from BetterJSONStorage import BetterJSONStorage
from discord import Intents, AllowedMentions
from tinydb import TinyDB, Query
from thefuzz import fuzz
from pytz import timezone

if os.name == "nt":
    import colorama

    colorama.init()
else:
    import uvloop

    uvloop.install()


class DankClient(discord.Client):
    """
    The Discord client for this bot.
    """
    def __init__(self, *args, **kwargs):
        """
        Creates a new Discord client.
        """
        super().__init__(*args, **kwargs)

        self._resolver: Optional[aiohttp.AsyncResolver] = None
        self._connector: Optional[aiohttp.TCPConnector] = None

        self.current_dank: Optional[Dank] = None
        self.now: Optional[datetime.datetime] = None

        self.lock: Optional[asyncio.Lock] = None

        self.db = TinyDB(
            pathlib.Path("./db.json"), access_mode="r+", storage=BetterJSONStorage
        )
        self.backup_table = self.db.table("backup")

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
        Resumes a saved dank.
        """
        guild = None
        for guild in self.guilds:
            break
        if guild is None:
            return
        self.now = datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)
        for save in self.backup_table.all():
            save = save["v"]
            channel = guild.get_channel(save["channel"])
            author = guild.get_member(save["author"])
            restored_dank = Dank(channel, author)
            restored_dank.group_buckets = {
                int(k): {guild.get_member(m) for m in v}
                for k, v in save["group_buckets"].items()
            }
            restored_dank.danker_buckets = {
                guild.get_member(int(k)): v for k, v in save["danker_buckets"].items()
            }
            restored_dank.future = datetime.datetime.fromisoformat(save["future"])
            restored_dank.timestamp = save["timestamp"]
            restored_dank.is_checking = save["is_checking"]
            restored_dank.role = save["role"]
            restored_dank.message = await channel.fetch_message(save["message"])
            restored_dank.has_initial = save["has_initial"]
            restored_dank.was_scheduled = save["was_scheduled"]
            restored_dank.base_mention = save["base_mention"]
            self.current_dank = restored_dank
            self.current_dank.start_countdown()
            break
        self.backup_table.truncate()

    async def on_message(self, message):
        """
        Handles new dank messages.
        """
        # not a bot
        if message.author.bot:
            return

        # if only an embed, then there's no content to parse
        if not message.content:
            return

        # normalize
        message.content = message.content.lower()

        if is_dank(message.content):
            async with self.lock:
                # set our global now to when the message was made
                self.now = message.created_at

                # set up our arg parser
                args = message.content.split()[1:]
                danker = message.author
                options = DankOptions()

                # consume all args
                while args:
                    options = await consume_args(
                        args, danker, message.created_at, options
                    )
                    # if we cleared options, then we stop here
                    if not options:
                        return

                # are we going to start a dank?
                if not self.current_dank:
                    # check if it's sufficiently in the future
                    if options.future:
                        delta = options.future - self.now
                        if delta < DEFAULT_DELTA:
                            options.future = None
                    # if didn't get a date, default to delta
                    if not options.future:
                        options.future = self.now + DEFAULT_DELTA
                    self.current_dank = Dank(message.channel, danker)
                    await self.current_dank.start(options.future)

                # add to dank
                await self.current_dank.add_danker(message.author, options.bucket)


client: Optional[DankClient] = None

Store = Query()


def get_value(key: str, default: Any = None) -> Any:
    """
    Gets from the key value DB table.
    """
    res = client.db.get(Store.k == key)
    if res:
        return res["v"]
    else:
        return default


def set_value(key: str, val: Any):
    """
    Sets to the key value DB table.
    """
    client.db.upsert({"k": key, "v": val}, Store.k == key)


def update_value(update_fn: Callable[[Any], Any], key: str, default: Any = None) -> Any:
    """
    Gets an existing value in the key value DB table and updates it using update_fn.
    :return: The new value
    """
    old = get_value(key, default)
    new = update_fn(old)
    set_value(key, new)
    return new


LOCAL_TIMEZONE = "US/Eastern"
LOCAL_TZINFO = timezone(LOCAL_TIMEZONE)
TIMESTAMP_TIMEZONE = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=TIMESTAMP_TIMEZONE)
TIMESTAMP_GRANULARITY = datetime.timedelta(seconds=1)


def generate_timestamp(dt: datetime.datetime) -> int:
    """
    Gets a UNIX timestamp representing a datetime.
    """
    delta = dt - EPOCH
    return delta // TIMESTAMP_GRANULARITY


def print_timestamp(timestamp: int, style: str) -> str:
    """
    Gets a Discord string representing a UNIX timestamp.
    """
    return f"<t:{timestamp}:{style}>"


def generate_datetime(timestamp: int) -> datetime.datetime:
    """
    Gets a datetime from UNIX timestamp.
    """
    delta = datetime.timedelta(milliseconds=timestamp)
    return EPOCH + delta


BUCKET_MIN: int = 2
BUCKET_MAX: int = 5
BUCKET_RANGE_MAX: int = BUCKET_MAX + 1
BUCKET_RANGE = range(BUCKET_MIN, BUCKET_RANGE_MAX)

DEFAULT_COUNTDOWN = 120.0
MAX_CHECK_COUNTDOWN = 300.0
DEFAULT_DELTA = datetime.timedelta(seconds=DEFAULT_COUNTDOWN)

GAME_ROLES = {"dota": 261137719579770882}
GAMES = list(GAME_ROLES.keys())
DEFAULT_GAME = GAMES[0]


def increment(val: int) -> int:
    """
    Increments a given int by 1, functionally.
    """
    return val + 1


class Dank:
    """
    Represents a pending/active Dank.
    """

    group_buckets: Dict[int, Set[discord.Member]]
    danker_buckets: Dict[discord.Member, int]
    future: datetime.datetime
    timestamp: int
    is_checking: bool
    author: discord.Member
    channel: discord.TextChannel
    role: int
    message: Optional[discord.Message]
    task: Optional[asyncio.Task]
    has_initial: bool
    was_scheduled: Optional[bool]
    base_mention: Optional[str]

    def __init__(self, channel: discord.TextChannel, author: discord.Member):
        """
        Creates a new Dank, to be started with start().
        """
        self.reset()

        self.author = author
        self.channel = channel
        self.role = GAME_ROLES[DEFAULT_GAME]

        self.task = None

        self.has_initial = False
        self.was_scheduled = None

    def save(self):
        """
        Saves the Dank to disk, so that it may be resumed in case of a crash.

        Must be called any time any of the class properties change.
        """
        data = {
            "group_buckets": {
                str(k): [m.id for m in v] for k, v in self.group_buckets.items()
            },
            "danker_buckets": {str(k.id): v for k, v in self.danker_buckets.items()},
            "future": self.future.isoformat(),
            "timestamp": self.timestamp,
            "is_checking": self.is_checking,
            "author": self.author.id,
            "channel": self.channel.id,
            "role": self.role,
            "message": self.message.id,
            "has_initial": self.has_initial,
            "was_scheduled": self.was_scheduled,
            "base_mention": self.base_mention,
        }
        client.backup_table.upsert({"k": "saved", "v": data}, Store.k == "saved")

    def reset(self):
        """
        Resets the dank to require new confirmation, for starting a Dank Check.
        """
        self.group_buckets = dict()
        for bucket in BUCKET_RANGE:
            self.group_buckets[bucket] = set()
        self.danker_buckets = dict()
        self.message = None
        self.base_mention = None

    async def start(self, future: datetime.datetime, mention: str = None):
        """
        Starts the dank.
        """
        self.update_future(future)
        await self.initialize(mention=mention)
        self.save()

    def update_future(self, future: datetime.datetime):
        """
        Sets the time the dank finishes.
        """
        self.future = future
        self.timestamp = generate_timestamp(future)

        countdown = max(DEFAULT_COUNTDOWN, self.get_delta_seconds())
        self.is_checking = countdown <= MAX_CHECK_COUNTDOWN
        if self.was_scheduled is None:
            self.was_scheduled = not self.is_checking

    async def initialize(self, mention: str = None):
        """
        Starts/schedules the dank check.
        """
        if self.base_mention is None:
            self.base_mention = f"<@&{self.role}>" if mention is None else mention
        name = self.author.display_name
        relative_time = print_timestamp(self.timestamp, "R")
        if self.is_checking:
            msg = f"{self.base_mention} {name} requested a Dank Check. (expires {relative_time})"
        else:
            short_time = print_timestamp(self.timestamp, "t")
            msg = f"{self.base_mention} {name} scheduled a dank at {short_time} ({relative_time})."
        if self.message:
            await self.update_message(msg)
        else:
            self.message = await self.channel.send(msg)
        self.start_countdown()

    def get_delta(self) -> datetime.timedelta:
        """
        Gets the timedelta until the dank finishes.
        """
        return self.future - client.now

    def get_delta_seconds(self) -> float:
        """
        Gets the number of seconds left until the dank finishes.
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

    async def add_danker(self, danker: discord.Member, min_bucket: int):
        """
        Sets a danker to the specified buckets.
        """
        # out of bounds buckets
        min_bucket = max(BUCKET_MIN, min(min_bucket, BUCKET_MAX))

        # remove them first if this is an update
        current_min_bucket = self.danker_buckets.get(danker)
        if current_min_bucket:
            if current_min_bucket != min_bucket:
                await self.remove_danker(danker, notify=False)
            else:
                return

        # add them from that bucket and beyond
        self.danker_buckets[danker] = min_bucket
        for bucket in range(min_bucket, BUCKET_RANGE_MAX):
            self.group_buckets[bucket].add(danker)

        if self.has_initial:
            name = danker.display_name
            size = len(self.danker_buckets)
            size = f"**({size}/5)**"
            with_str = f" with {min_bucket} dankers" if min_bucket > BUCKET_MIN else ""
            if self.is_checking:
                msg = f"{name} is ready to dank{with_str}. {size}"
                countdown = self.get_delta_seconds()
                if 5 < countdown < DEFAULT_COUNTDOWN:
                    missing_countdown = datetime.timedelta(
                        seconds=DEFAULT_COUNTDOWN - self.get_delta_seconds()
                    )
                    await self.refresh(self.future + missing_countdown)
            else:
                short_time = print_timestamp(self.timestamp, "t")
                relative_time = print_timestamp(self.timestamp, "R")
                msg = f"{name} can dank at {short_time} ({relative_time}){with_str}. {size}"
            await self.channel.send(msg)
        else:
            self.has_initial = True

        self.save()

    async def remove_danker(self, danker: discord.Member, notify: bool = True):
        """
        Removes a danker from all buckets.
        """
        # pop off the danker lookup
        min_bucket = self.danker_buckets.pop(danker)

        # remove from all groups
        for bucket in range(min_bucket, BUCKET_RANGE_MAX):
            self.group_buckets[bucket].remove(danker)

        if notify:
            await self.channel.send("You left the dank.")

        self.save()

    def start_countdown(self):
        """
        Directly starts the asyncio countdown task.
        """
        self.task = asyncio.create_task(self.countdown(), name="Countdown")

    async def refresh(self, future: datetime.datetime):
        """
        Refreshes the dank with a new countdown.
        """
        self.cancel_task(reason="Refreshing")
        await self.start(future)

    async def countdown(self):
        """
        Sleeps for the countdown time, and then finishes the dank.
        """
        delta = self.get_delta_seconds()
        # if our delta is less than 16 milliseconds, then we don't expect scheduling accuracy and thus don't sleep
        if delta >= 0.016:
            await asyncio.sleep(self.get_delta_seconds())
        await self.finish()

    async def finish(self):
        """
        Either finishes the dank check, or starts one if it is scheduled.
        """
        dankers = self.get_dankers()
        if len(dankers) > 1:
            # print out the message
            mention = " ".join([danker.mention for danker in dankers])
            if self.is_checking:
                # finish the dank
                await self.channel.send(
                    f"{mention} Dank Check complete. **{len(dankers)}/5** players ready to dank."
                )
                # we had a dank
                set_value("no_dankers_consecutive", 0)
            else:
                # start the dank up again
                client.now = datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)
                self.reset()
                asyncio.create_task(
                    self.start(client.now + DEFAULT_DELTA, mention=mention)
                )
                return
        else:
            no_dankers = update_value(increment, "no_dankers", 0)
            no_dankers_consecutive = update_value(
                increment, "no_dankers_consecutive", 0
            )
            await self.channel.send(
                f"No dankers found for the dank. This server has gone {no_dankers} danks without a dank. ({no_dankers_consecutive} in a row)."
            )
            await self.channel.send(
                "https://cdn.discordapp.com/attachments/195236615310934016/952745307509227592/cb3.jpg"
            )
        if self.is_checking:
            # make it past tense
            await self.replace_message("expires", "expired")
        client.current_dank = None
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
        Handles cancelling the dank.
        """
        self.cancel_task()
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
            await self.replace_message("expires", "cancelled")
        client.current_dank = None
        client.backup_table.truncate()
        await self.channel.send("Dank cancelled.")

    async def advance(self, now: datetime.datetime):
        """
        Skips ahead to finish the dank now.
        """
        self.cancel_task(reason="Advancing")
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
        await self.finish()

    def get_dankers(self) -> Set[discord.Member]:
        """
        Gets available dankers, according to the largest satisfied bucket.
        """
        bucket = set()
        # count down from the biggest bucket, so we can stop at the biggest that satisfies
        for i in reversed(BUCKET_RANGE):
            # get the bucket from index
            candidate_bucket = self.group_buckets[i]
            # get the bucket's size
            size = len(candidate_bucket)
            # is the size enough? ex: for bucket #5, we need at least 5 dankers
            if size >= i:
                bucket = candidate_bucket
                break
        return bucket


def get_int(s: str, default: Optional[int] = 0) -> int:
    """
    Tries to get an int from a string, if fails, returns default.
    """
    try:
        val = int(s)
        return val
    except ValueError:
        return default


def get_float(s: str, default: Optional[float] = 0.0) -> float:
    """
    Tries to get a float from a string, if fails, returns default.
    """
    try:
        val = float(s)
        return val
    except ValueError:
        return default


@dataclasses.dataclass(init=False)
class DankOptions:
    """
    Represents arguments to the dank.
    """
    future: Optional[datetime.datetime]
    bucket: int

    def __init__(self):
        """
        Initializes a blank/default options.
        """
        self.future = None
        self.bucket = BUCKET_MIN


CURRENT_DANK_ARGS = {"cancel", "now", "leave"}

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


async def consume_args(
    args: List[str],
    danker: discord.Member,
    created_at: datetime.datetime,
    options: DankOptions,
) -> Optional[DankOptions]:
    """
    Handles building options from command arguments by parsing args.

    Returning None means we don't interact with the dank.
    """
    control = args.pop(0)
    # if there's a dank, try to control it
    if client.current_dank:
        if control == "cancel":
            await client.current_dank.cancel(created_at)
            return None
        if control == "now":
            await client.current_dank.advance(created_at)
            return None
        if control == "leave":
            await client.current_dank.remove_danker(danker)
            return None
    else:
        # sometimes users accidentally try to control a dank when it doesn't exist
        if control in CURRENT_DANK_ARGS:
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
                        settings = {"TIMEZONE": LOCAL_TIMEZONE, "TO_TIMEZONE": "UTC"}
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

    if args:
        # if buckets
        if control == "if":
            options.bucket = get_int(args.pop(0), BUCKET_MIN)
            return options

    # if we didn't find the control, just ignore
    return options


FUZZ_THRESHOLD = 70
TOKEN_WORD = "dank"


def is_dank(content: str) -> bool:
    """
    Checks if the message represents a dank "command"
    """
    # if it is the word, passes
    word = content.split(maxsplit=1)[0]
    if word.startswith(TOKEN_WORD):
        return True
    # has to start with d
    if not word.startswith(TOKEN_WORD[0]):
        return False
    # fuzz it
    ratio = fuzz.ratio(word, TOKEN_WORD)
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
    client = DankClient(
        max_messages=500,
        intents=intents,
        allowed_mentions=mentions,
    )

    # start the client
    async with client as _client:
        await _client.start(os.environ["DANK_TOKEN"])

asyncio.run(main())
