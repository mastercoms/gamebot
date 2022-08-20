import asyncio
import dataclasses
import os

from typing import Dict, Set, List, Optional, Any

import discord
import datetime
import dateparser
import arrow

from discord import Intents
from tinydb import TinyDB, Query

intents = Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

client.current_dank = None
client.now = None
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
DEFAULT_GAME = 'dota'


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

    def __init__(self, channel: discord.TextChannel, author: discord.User):
        self.group_buckets = dict()
        for bucket in BUCKET_RANGE:
            self.group_buckets[bucket] = set()
        self.danker_buckets = dict()

        self.author = author
        self.channel = channel
        self.role = GAME_ROLES[DEFAULT_GAME]

        self.message = None
        self.task = None

        self.has_initial = False

    async def start(self, future: datetime.datetime, mention: str = None):
        """
        Starts the dank.
        """
        self.update_future(future)
        await self.initialize(mention)

    def update_future(self, future: datetime.datetime):
        """
        Sets the time the dank finishes.
        """
        self.future = future
        self.timestamp = generate_timestamp(future)

        countdown = max(DEFAULT_COUNTDOWN, self.get_delta_seconds())
        self.is_checking = countdown <= MAX_CHECK_COUNTDOWN

    async def initialize(self, mention: str = None):
        """
        Starts/schedules the dank check.
        """
        name = self.author.display_name
        mention = f"<@&{self.role}>" if mention is None else mention
        relative_time = print_timestamp(self.timestamp, 'R')
        if self.is_checking:
            msg = f"{mention} {name} requested a Dank Check. (expires {relative_time})"
        else:
            short_time = print_timestamp(self.timestamp, 't')
            msg = f"{mention} {name} scheduled a dank {relative_time} ({short_time})."
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
                    self.refresh(self.future + missing_countdown)
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

    def refresh(self, future: datetime.datetime):
        """
         Refreshes the dank with a new countdown.
        """
        self.cancel_task(reason="Refreshing")
        self.update_future(future)
        self.start_countdown()

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
                client.now = datetime.datetime.utcnow()
                await self.start(client.now + DEFAULT_DELTA, mention=mention)
        else:
            no_dankers = get_value("no_dankers", 0)
            no_dankers += 1
            set_value("no_dankers", no_dankers)
            no_dankers_consecutive = get_value("no_dankers_consecutive", 0)
            no_dankers_consecutive += 1
            set_value("no_dankers_consecutive", no_dankers_consecutive)
            await self.channel.send(f"No dankers found for the dank. This server has gone {no_dankers} danks with a dank. ({no_dankers_consecutive} in a row).")
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


def get_int(s, default=0) -> int:
    try:
        val = int(s)
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
                        if attempt_date:
                            # now we know our new start
                            new_start = end
                        # if we surpass the last arg, end
                        if end >= last:
                            break
                    # consume our peeked inputs to the date
                    del args[:new_start]
                    options.future = attempt_date
                    if options.future.tzinfo is None:
                        options.future = options.future.replace(tzinfo=TIMESTAMP_TIMEZONE)
                    return options
                if control == "in":
                    arw = arrow.get(client.now)
                    datestring = "in"
                    end = 0
                    new_start = 0
                    last = len(args)
                    attempt_date = None
                    # go through until we get a date
                    while True:
                        datestring += " " + args[end]
                        try:
                            attempt_date = arw.dehumanize(datestring, locale="en")
                        except ValueError:
                            # didn't work
                            pass
                        # go to next arg
                        end += 1
                        # we made a new date
                        if attempt_date:
                            # now we know our new start
                            new_start = end
                        # if we surpass the last arg, end
                        if end >= last:
                            break
                    # consume our peeked inputs to the date
                    del args[:new_start]
                    if attempt_date:
                        options.future = attempt_date.datetime
                    return options

    if args:
        # if buckets
        if control == "if":
            options.bucket = get_int(args.pop(0), BUCKET_MIN)
            return options

    # if we didn't find the control, just ignore
    return options


@client.event
async def on_message(message):
    if message.author == client.user or message.author.bot:
        return

    message.content = message.content.lower()

    if message.content.split(" ")[0].startswith("dank"):
        # set our global now to when the message was made
        client.now = message.created_at

        # set up our arg parser
        args = message.content.split(" ")[1:]
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
