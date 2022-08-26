import asyncio
import dataclasses
import math
import os

from typing import Dict, Set, List, Optional, Any

import discord
import datetime
import dateparser
import arrow

from discord import Intents
from tinydb import TinyDB, Query
from thefuzz import fuzz

if os.name == "nt":
    from colorama import init
    init()

intents = Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

client.current_dank = None
client.now = None

client.lock = asyncio.Lock()

client.db = TinyDB("./db.json")
Store = Query()


def get_value(key: str, default: Any = None) -> Dict[str, Any]:
    res = client.db.get(Store.k == key)
    if res:
        return res["v"]
    else:
        return default


def set_value(key: str, val: Any):
    client.db.upsert({"k": key, "v": val}, Store.k == key)


LOCAL_TIMEZONE = "US/Eastern"
TIMESTAMP_TIMEZONE = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=TIMESTAMP_TIMEZONE)
TIMESTAMP_GRANULARITY = datetime.timedelta(seconds=1)


def generate_timestamp(dt: datetime.datetime) -> int:
    delta = dt - EPOCH
    return delta // TIMESTAMP_GRANULARITY


def print_timestamp(timestamp: int, style: str) -> str:
    return f"<t:{timestamp}:{style}>"


def generate_datetime(timestamp: int) -> datetime.datetime:
    delta = datetime.timedelta(milliseconds=timestamp)
    return EPOCH + delta


BUCKET_MIN: int = 2
BUCKET_MAX: int = 5
BUCKET_RANGE_MAX: int = BUCKET_MAX + 1
BUCKET_RANGE = range(BUCKET_MIN, BUCKET_RANGE_MAX)

DEFAULT_COUNTDOWN = 120.0
MAX_CHECK_COUNTDOWN = 300.0
DEFAULT_DELTA = datetime.timedelta(seconds=DEFAULT_COUNTDOWN)

GAME_ROLES = {
    'dota': 261137719579770882
}
GAMES = list(GAME_ROLES.keys())
DEFAULT_GAME = GAMES[0]


class Dank:
    group_buckets: Dict[int, Set[discord.Member]]
    danker_buckets: Dict[discord.Member, int]
    future: datetime.datetime
    timestamp: int
    is_checking: bool
    channel: discord.TextChannel
    role: int
    message: Optional[discord.Message]
    task: Optional[asyncio.Task]
    has_initial: bool
    was_scheduled: Optional[bool]
    base_mention: Optional[str]

    def __init__(self, channel: discord.TextChannel, author: discord.User):
        self.reset()

        self.author = author
        self.channel = channel
        self.role = GAME_ROLES[DEFAULT_GAME]

        self.task = None

        self.has_initial = False
        self.was_scheduled = None

    def reset(self):
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
        relative_time = print_timestamp(self.timestamp, 'R')
        if self.is_checking:
            msg = f"{self.base_mention} {name} requested a Dank Check. (expires {relative_time})"
        else:
            short_time = print_timestamp(self.timestamp, 't')
            msg = f"{self.base_mention} {name} scheduled a dank at {short_time} ({relative_time})."
        if self.message:
            await self.message.edit(content=msg)
        else:
            self.message = await self.channel.send(msg)
        self.start_countdown()

    def get_delta(self) -> datetime.timedelta:
        return self.future - client.now

    def get_delta_seconds(self) -> float:
        return self.get_delta().total_seconds()

    async def add_danker(self, danker: discord.Member, min_bucket: int):
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
                if 1 < countdown < DEFAULT_COUNTDOWN:
                    missing_countdown = datetime.timedelta(seconds=DEFAULT_COUNTDOWN - self.get_delta_seconds())
                    await self.refresh(self.future + missing_countdown)
            else:
                short_time = print_timestamp(self.timestamp, 't')
                relative_time = print_timestamp(self.timestamp, 'R')
                msg = f"{name} can dank at {short_time} ({relative_time}){with_str}. {size}"
            await self.channel.send(msg)
        else:
            self.has_initial = True

    async def remove_danker(self, danker: discord.Member, notify: bool = True):
        # pop off the danker lookup
        min_bucket = self.danker_buckets.pop(danker)

        # remove from all groups
        for bucket in range(min_bucket, BUCKET_RANGE_MAX):
            self.group_buckets[bucket].remove(danker)

        if notify:
            await self.channel.send("You left the dank.")

    def start_countdown(self):
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
                await self.channel.send(f"{mention} Dank Check complete. **{len(dankers)}/5** players ready to dank.")
                # we had a dank
                set_value("no_dankers_consecutive", 0)
            else:
                # start the dank up again
                client.now = datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)
                self.reset()
                asyncio.create_task(self.start(client.now + DEFAULT_DELTA, mention=mention))
                return
        else:
            no_dankers = get_value("no_dankers", 0)
            no_dankers += 1
            set_value("no_dankers", no_dankers)
            no_dankers_consecutive = get_value("no_dankers_consecutive", 0)
            no_dankers_consecutive += 1
            set_value("no_dankers_consecutive", no_dankers_consecutive)
            await self.channel.send(f"No dankers found for the dank. This server has gone {no_dankers} danks without a dank. ({no_dankers_consecutive} in a row).")
            await self.channel.send("https://cdn.discordapp.com/attachments/195236615310934016/952745307509227592/cb3.jpg")
        client.current_dank = None

    def cancel_task(self, reason: str = "Cancelled"):
        self.task.cancel(msg=reason)

    async def cancel(self):
        self.cancel_task()
        client.current_dank = None
        await self.channel.send("Dank cancelled.")

    async def advance(self):
        self.cancel_task(reason="Advancing")
        self.is_checking = True
        await self.finish()

    def get_dankers(self) -> Set:
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


def get_int(s: str, default: Optional[int]=0) -> int:
    try:
        val = int(s)
        return val
    except ValueError:
        return default


def get_float(s: str, default: Optional[float]=0.0) -> float:
    try:
        val = float(s)
        return val
    except ValueError:
        return default


@dataclasses.dataclass(init=False)
class DankOptions:
    future: Optional[datetime.datetime]
    bucket: int

    def __init__(self):
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
    "minutes": (60.0, "seconds")
}
HUMANIZE_SHORTHAND = {
    "mins": "minutes",
    "min": "minutes",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "secs": "seconds",
    "sec": "seconds",
    "s": "seconds"
}
NUMERIC = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "-", "."}


def convert_humanize_decimal(quantity: float, unit: str) -> str:
    frac, whole = math.modf(quantity)
    base = f"{int(quantity)} {unit}"
    # if this isn't a significant float, then we can just return it back
    if abs(frac) <= 0.01:
        return base
    mapping = HUMANIZE_MAPPING.get(unit)
    # if there's no further conversion, we just deal with the lower precision
    if not mapping:
        return base
    conversion, lesser_unit = mapping
    lesser_quantity = frac * conversion
    return f"{base}, {convert_humanize_decimal(lesser_quantity, lesser_unit)}"


async def consume_args(args: List[str], danker: discord.Member, options: DankOptions) -> Optional[DankOptions]:
    control = args.pop(0)
    if client.current_dank:
        if control == "cancel":
            await client.current_dank.cancel()
            return None
        if control == "now":
            await client.current_dank.advance()
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
                # TODO: deduplicate
                if control == "at":
                    datestring = ""
                    end = 0
                    new_start = 0
                    last = len(args)
                    confirmed_date = None
                    # go through until we get a date
                    while True:
                        datestring += " " + args[end] if datestring else args[end]
                        settings = {
                            'TIMEZONE': LOCAL_TIMEZONE,
                            'TO_TIMEZONE': 'UTC'
                        }
                        attempt_date = dateparser.parse(datestring, languages=["en"], settings=settings)
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
                            confirmed_date = confirmed_date.replace(tzinfo=TIMESTAMP_TIMEZONE)
                        options.future = confirmed_date
                    return options
                if control == "in":
                    arw = arrow.get(client.now)
                    datestring = "in"
                    end = 0
                    new_start = 0
                    last = len(args)
                    confirmed_date = None
                    # go through until we get a date
                    while True:
                        word = args[end]
                        # if it's a shorthand quantity, ex. 1h, 5m
                        if word[0] in NUMERIC and word[len(word) - 1] not in NUMERIC:
                            i = 0
                            for i, c in enumerate(word):
                                if c not in NUMERIC:
                                    break
                            args.insert(end + 1, word[i:])
                            word = word[:i]
                            last = len(args)
                        # need to replace 1 with "a" or "an" depending on the next word if there is one
                        if last > end + 1:
                            noun = args[end + 1]
                            longform_noun = HUMANIZE_SHORTHAND.get(noun)
                            if longform_noun is not None:
                                noun = longform_noun
                                args[end + 1] = noun
                            if word == "1":
                                if not noun.endswith("s"):
                                    if noun in HUMANIZE_VOWEL_WORDS:
                                        word = "an"
                                    else:
                                        word = "a"
                            else:
                                parsed_num = get_float(word, default=None)
                                if parsed_num is not None:
                                    word = convert_humanize_decimal(parsed_num, noun)
                                    # we also consumed the next word
                                    end += 1
                        datestring += " " + word
                        try:
                            attempt_date = arw.dehumanize(datestring, locale="en")
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
    word = content.split()[0]
    if word.startswith(TOKEN_WORD):
        return True
    if not word.startswith(TOKEN_WORD[0]):
        return False
    ratio = fuzz.ratio(word, TOKEN_WORD)
    return ratio > 70


@client.event
async def on_message(message):
    if message.author == client.user or message.author.bot:
        return

    message.content = message.content.lower()

    if is_dank(message.content):
        async with client.lock:
            if not client.current_dank:
                # set our global now to when the message was made
                client.now = message.created_at

            # set up our arg parser
            args = message.content.split()[1:]
            danker = message.author
            options = DankOptions()

            # consume all args
            while args:
                options = await consume_args(args, danker, options)
                # if we cleared options, then we stop here
                if not options:
                    return

            # are we going to start a dank?
            if not client.current_dank:
                # check if it's sufficiently in the future
                if options.future:
                    delta = options.future - client.now
                    if delta < DEFAULT_DELTA:
                        options.future = None
                # if didn't get a date, default to delta
                if not options.future:
                    options.future = client.now + DEFAULT_DELTA
                client.current_dank = Dank(message.channel, danker)
                await client.current_dank.start(options.future)

            # add to dank
            await client.current_dank.add_danker(message.author, options.bucket)


client.run(os.environ['DANK_TOKEN'])
