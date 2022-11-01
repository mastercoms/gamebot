from __future__ import annotations

import gevent.monkey
gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
gevent.monkey.patch_dns()

import asyncio
import dataclasses
import logging
import math
import os
import pathlib
import random
import socket

from copy import deepcopy
from typing import Any, Callable

import aiohttp
import arrow
import asyncio_gevent
import dateparser
import datetime
import discord
import httpx
import orjson

from BetterJSONStorage import BetterJSONStorage
from discord import Intents, AllowedMentions
from httpx_auth import QueryApiKey
from pytz import timezone
from steam.steamid import SteamID, from_invite_code, make_steam64
from steam.webapi import WebAPI
from steam.client import SteamClient
from steam.client.user import SteamUser
from dota2.client import Dota2Client
from dota2.proto_enums import EDOTAGCMsg
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
    if False:
        import uvloop

        uvloop.install()

asyncio.set_event_loop_policy(asyncio_gevent.EventLoopPolicy())


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


MAX_BACKOFF = 2.0
BASE_BACKOFF = 0.005


def get_backoff(failures: int) -> float:
    return random.uniform(0, min(MAX_BACKOFF, BASE_BACKOFF * 2 ** failures))


def wait_backoff(failures: int):
    gevent.sleep(get_backoff(failures))


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


class DotaAPI:
    @staticmethod
    async def get(*args, **kwargs) -> dict[str, Any] | list[dict[str, Any]]:
        resp = await client.opendota.get(*args, **kwargs)
        return resp.json()

    @staticmethod
    async def query_constants(*resources: str) -> dict[str, dict[str, Any]]:
        global DOTA_CACHED_CONSTANTS
        for res in resources:
            url = f"/constants/{res}"
            DOTA_CACHED_CONSTANTS[res] = await DotaAPI.get(url)
        return DOTA_CACHED_CONSTANTS

    @staticmethod
    def get_constants(*resources: str) -> dict[str, dict[str, Any]]:
        global DOTA_CACHED_CONSTANTS
        for res in resources:
            DOTA_CACHED_CONSTANTS[res] = DOTA_CACHED_CONSTANTS.get(res)
        return DOTA_CACHED_CONSTANTS

    @staticmethod
    def query_constant_name(constants: dict[str, dict[str, Any]], resource: str, idx: int) -> str:
        constant = constants[resource]
        data = constant[str(idx)]
        return data["name"].replace(f"{resource}_", "").replace("_", " ").title()

    @staticmethod
    def query_match_constant(constants: dict[str, dict[str, Any]], match: dict[str, Any], resource: str):
        return DotaAPI.query_constant_name(constants, resource, match[resource])

    @staticmethod
    async def get_matches(steam_id: int, **params) -> list[dict[str, Any]]:
        url = f"/players/{steam_id}/matches"
        return await DotaAPI.get(url, params=params)

    @staticmethod
    def get_match(match_id: int) -> dict[str, Any] | None:
        if not client.steamapi:
            return None
        tries = 0
        while True:
            # this endpoint regularly fails, so we retry a few times
            tries += 1
            try:
                resp = client.steamapi.IDOTA2Match_570.GetMatchDetails(match_id=str(match_id))
                break
            except Exception as e:
                if tries >= 10:
                    print("Failed to get match details:", e)
                    return None
                wait_backoff(tries)
        return resp["result"]


class SteamWorker:
    def __init__(self):
        self.logged_on_once = False

        self.steam = worker = SteamClient()
        worker.set_credential_location(".")

        @worker.on("error")
        def handle_error(result):
            print("Logon result:", repr(result))

        @worker.on("connected")
        def handle_connected():
            print("Connected to", worker.current_server_addr)

        @worker.on("channel_secured")
        def send_login():
            if self.logged_on_once and self.steam.relogin_available:
                self.steam.relogin()

        @worker.on("logged_on")
        def handle_after_logon():
            self.logged_on_once = True

            print("-"*30)
            print("Logged on as:", worker.user.name)
            print("Community profile:", worker.steam_id.community_url)
            print("Last logon:", worker.user.last_logon)
            print("Last logoff:", worker.user.last_logoff)
            print("-"*30)

        @worker.on("disconnected")
        def handle_disconnect():
            print("Disconnected.")

            if self.logged_on_once:
                print("Reconnecting...")
                worker.reconnect(maxdelay=30)

        @worker.on("reconnect")
        def handle_reconnect(delay):
            print(f"Reconnect in {delay}...", )

        @worker.friends.on("friend_invite")
        def handle_friend_invite(user: SteamUser):
            mutual = get_value("mutual_steam_id", table=client.settings_table)
            should_add = True
            if mutual:
                if not client.steamapi:
                    return
                should_add = False
                user_steam_id = user.steam_id.as_64
                # search for the mutual user
                search_for = mutual
                # first try getting the friends list of the requesting user
                try:
                    resp = client.steamapi.ISteamUser.GetFriendList(steamid=user_steam_id)
                except:
                    try:
                        # if it's private, try getting the friends list of the mutual target
                        resp = client.steamapi.ISteamUser.GetFriendList(steamid=mutual)
                    except:
                        return
                    # search for the requesting user
                    search_for = user_steam_id
                friends = resp["friendslist"]["friends"]
                search_for = str(search_for)
                for friend in friends:
                    if friend["steamid"] == search_for:
                        should_add = True
                        break
            if should_add:
                self.steam.friends.add(user)

    def login(self, username, password, no_2fa):
        if no_2fa:
            two_factor_code = None
        else:
            two_factor_code = input("Enter 2FA code: ")
        if two_factor_code or os.path.exists(self.steam._get_sentry_path(username)):
            self.steam.login(username, password, two_factor_code=two_factor_code)
        else:
            self.steam.cli_login(username, password)

    def close(self):
        if client.dotaclient:
            client.dotaclient.exit()
        if self.steam.logged_on:
            self.logged_on_once = False
            print("Logout")
            self.steam.logout()
        if self.steam.connected:
            self.steam.disconnect()


class GameClient(discord.Client):
    """
    The Discord client for this bot.
    """

    def __init__(self, *args, **kwargs):
        """
        Creates a new Discord client.
        """
        global client
        self.opendota = kwargs.pop("opendota")
        self.debug = kwargs.pop("debug", False)
        self.no_2fa = kwargs.pop("no_2fa", True)
        client = self

        super().__init__(*args, **kwargs)

        self._resolver: aiohttp.AsyncResolver | None = None
        self._connector: aiohttp.TCPConnector | None = None

        self.current_game: Game | None = None
        self.current_match: DotaMatch | Match | None = None
        self.now: datetime.datetime | None = None

        self.lock: asyncio.Lock | None = None

        self.db = TinyDB(
            pathlib.Path("./db.json"), access_mode="r+", storage=BetterJSONStorage
        )
        self.backup_table = self.db.table("backup")
        self.players_table = self.db.table("players")
        self.settings_table = self.db.table("settings")
        steam_api_key = os.getenv("GAME_BOT_STEAM_KEY")
        if steam_api_key is not None:
            self.steamapi = WebAPI(
                key=steam_api_key
            )
        else:
            self.steamapi = None
        steam_username = os.getenv("GAME_BOT_STEAM_USER")
        steam_password = os.getenv("GAME_BOT_STEAM_PASS")
        if steam_username:
            self.steamclient = SteamWorker()
            if self.debug:
                self.steamclient.steam.verbose_debug = True
            self.steamclient.login(steam_username, steam_password, self.no_2fa)
            self.dotaclient = Dota2Client(self.steamclient.steam)
            if self.debug:
                self.dotaclient.verbose_debug = True

            def launch_dota():
                self.dotaclient.launch()

            if self.steamclient.logged_on_once:
                launch_dota()
            else:
                self.steamclient.steam.once("logged_on", launch_dota)
        else:
            self.steamclient = None
            self.dotaclient = None

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
        save = get_value("saved", table=self.backup_table)
        if save:
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

TableValueType = int | float | str | bool
TableContainerType = list[TableValueType | TableContainerType] | dict[str, TableValueType | TableContainerType]
TableType = TableValueType | TableContainerType


def get_value(key: str, *, default: TableType = None, table=None) -> TableType:
    """
    Gets from the key value DB table.
    """
    table_interface = table if table is not None else client.db
    res = table_interface.get(Store.k == key)
    if res:
        return res["v"]
    else:
        return default


def set_value(key: str, val: TableType, *, table=None):
    """
    Sets to the key value DB table.
    """
    table_interface = table if table is not None else client.db
    table_interface.upsert({"k": key, "v": val}, Store.k == key)


def update_value(update_fn: Callable[[TableType], TableType], key: str, *, default: TableType = None, table=None) -> TableType:
    """
    Gets an existing value in the key value DB table and updates it using update_fn.
    :return: The new value
    """
    table_interface = table if table is not None else client.db
    old = get_value(key, default=default, table=table_interface)
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


MATCH_POLL_INTERVALS = [10 * 60, 10 * 60, 5 * 60]
MATCH_POLL_INTERVAL_COUNT = len(MATCH_POLL_INTERVALS)
MATCH_POLL_INTERVAL_LAST = MATCH_POLL_INTERVALS[MATCH_POLL_INTERVAL_COUNT - 1]
MATCH_MAX_POLLS = 2 * 60 * 60 // MATCH_POLL_INTERVAL_LAST


class Match:
    pass


DOTA_RANKS = {
    0: ("Unknown", "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/e7/SeasonalRank0-0.png/revision/latest?cb=20171124184310"),

    10: ("Herald", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/Emoticon_Ranked_Herald.png/revision/latest?cb=20190212051846"),
    11: ("Herald [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/85/SeasonalRank1-1.png/revision/latest?cb=20190130002445"),
    12: ("Herald [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/ee/SeasonalRank1-2.png/revision/latest?cb=20190130002448"),
    13: ("Herald [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/0/05/SeasonalRank1-3.png/revision/latest?cb=20190130002457"),
    14: ("Herald [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6d/SeasonalRank1-4.png/revision/latest?cb=20190130002500"),
    15: ("Herald [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2b/SeasonalRank1-5.png/revision/latest?cb=20190130002504"),
    16: ("Herald [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/9/94/SeasonalRank1-6.png/revision/latest?cb=20190130002437"),
    17: ("Herald [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/12/SeasonalRank1-7.png/revision/latest?cb=20190130002441"),

    20: ("Guardian", "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/43/Emoticon_Ranked_Guardian.png/revision/latest?cb=20190212051853"),
    21: ("Guardian [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c7/SeasonalRank2-1.png/revision/latest?cb=20190130002542"),
    22: ("Guardian [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2c/SeasonalRank2-2.png/revision/latest?cb=20190130002545"),
    23: ("Guardian [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/f5/SeasonalRank2-3.png/revision/latest?cb=20190130002548"),
    24: ("Guardian [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b4/SeasonalRank2-4.png/revision/latest?cb=20190130002552"),
    25: ("Guardian [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/32/SeasonalRank2-5.png/revision/latest?cb=20190130002555"),
    26: ("Guardian [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/72/SeasonalRank2-6.png/revision/latest?cb=20190130002558"),
    27: ("Guardian [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c6/SeasonalRank2-7.png/revision/latest?cb=20190130002601"),

    30: ("Crusader", "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2d/Emoticon_Ranked_Crusader.png/revision/latest?cb=20190212051912"),
    31: ("Crusader [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/82/SeasonalRank3-1.png/revision/latest?cb=20190130002626"),
    32: ("Crusader [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6e/SeasonalRank3-2.png/revision/latest?cb=20190130002629"),
    33: ("Crusader [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/67/SeasonalRank3-3.png/revision/latest?cb=20190130002632"),
    34: ("Crusader [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/SeasonalRank3-4.png/revision/latest?cb=20190130002635"),
    35: ("Crusader [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b1/SeasonalRank3-5.png/revision/latest?cb=20190130002639"),
    36: ("Crusader [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank3-6.png/revision/latest?cb=20190130002611"),
    37: ("Crusader [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank3-6.png/revision/latest?cb=20190130002611"),

    40: ("Archon", "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/13/Emoticon_Ranked_Archon.png/revision/latest?cb=20190130004535"),
    41: ("Archon [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/76/SeasonalRank4-1.png/revision/latest?cb=20190130002704"),
    42: ("Archon [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/SeasonalRank4-2.png/revision/latest?cb=20190130002707"),
    43: ("Archon [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/60/SeasonalRank4-3.png/revision/latest?cb=20190130002710"),
    44: ("Archon [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/4a/SeasonalRank4-4.png/revision/latest?cb=20190130002714"),
    45: ("Archon [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/a3/SeasonalRank4-5.png/revision/latest?cb=20190130002718"),
    46: ("Archon [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/7e/SeasonalRank4-6.png/revision/latest?cb=20190130002651"),
    47: ("Archon [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/9/95/SeasonalRank4-7.png/revision/latest?cb=20190130002654"),

    50: ("Legend", "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/18/Emoticon_Ranked_Legend.png/revision/latest?cb=20190212051924"),
    51: ("Legend [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/79/SeasonalRank5-1.png/revision/latest?cb=20190130002757"),
    52: ("Legend [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/5/52/SeasonalRank5-2.png/revision/latest?cb=20190130002839"),
    53: ("Legend [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/88/SeasonalRank5-3.png/revision/latest?cb=20190130002819"),
    54: ("Legend [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/25/SeasonalRank5-4.png/revision/latest?cb=20190130002822"),
    55: ("Legend [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8e/SeasonalRank5-5.png/revision/latest?cb=20190130002826"),
    56: ("Legend [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2f/SeasonalRank5-6.png/revision/latest?cb=20190130002742"),
    57: ("Legend [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c7/SeasonalRank5-7.png/revision/latest?cb=20190130002745"),

    60: ("Ancient", "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/d8/Emoticon_Ranked_Ancient.png/revision/latest?cb=20190216113137"),
    61: ("Ancient [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/e0/SeasonalRank6-1.png/revision/latest?cb=20190130002941"),
    62: ("Ancient [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/1c/SeasonalRank6-2.png/revision/latest?cb=20190130002945"),
    63: ("Ancient [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/da/SeasonalRank6-3.png/revision/latest?cb=20190130002948"),
    64: ("Ancient [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/db/SeasonalRank6-4.png/revision/latest?cb=20190130002951"),
    65: ("Ancient [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/47/SeasonalRank6-5.png/revision/latest?cb=20190130002955"),
    66: ("Ancient [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/bd/SeasonalRank6-6.png/revision/latest?cb=20190130002958"),
    67: ("Ancient [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b8/SeasonalRank6-7.png/revision/latest?cb=20190130003003"),

    70: ("Divine", "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6d/Emoticon_Ranked_Divine.png/revision/latest?cb=20190130004646"),
    71: ("Divine [ 1 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b7/SeasonalRank7-1.png/revision/latest?cb=20190130003022"),
    72: ("Divine [ 2 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8f/SeasonalRank7-2.png/revision/latest?cb=20190130003026"),
    73: ("Divine [ 3 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/fd/SeasonalRank7-3.png/revision/latest?cb=20190130003029"),
    74: ("Divine [ 4 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/13/SeasonalRank7-4.png/revision/latest?cb=20190130003033"),
    75: ("Divine [ 5 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank7-5.png/revision/latest?cb=20190130003041"),
    76: ("Divine [ 6 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/a1/SeasonalRank7-6.png/revision/latest?cb=20190130003039"),
    77: ("Divine [ 7 ]", "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c1/SeasonalRank7-7.png/revision/latest?cb=20190130003043"),

    80: ("Immortal", "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/f2/SeasonalRankTop0.png/revision/latest?cb=20180606220529"),
    81: ("Immortal", "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/df/SeasonalRankTop1.png/revision/latest?cb=20180606220541"),
    82: ("Immortal", "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/ad/SeasonalRankTop2.png/revision/latest?cb=20180606220545"),
    83: ("Immortal", "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8e/SeasonalRankTop3.png/revision/latest?cb=20180606220548"),
    84: ("Immortal", "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/46/SeasonalRankTop4.png/revision/latest?cb=20180606220552"),
}

DOTA_STATE = {
    0: "Confirming match",
    1: "Waiting for loaders",
    2: "Hero Selection",
    3: "Strategy Time",
    4: "Pre Game",
    5: "Playing",
    6: "Post Game",
    7: "Ended",
    8: "Team Showcase",
    9: "Custom Game Setup",
    10: "Waiting for game to load",
    11: "Scenario Setup",
}

DOTA_ADV_LABELS = {
    "xp_per_min": "XP",
    "level": "XP",
    "net_worth": "Net Worth",
    "last_hits": "Last Hits",
    "lh_count": "Last Hits",
    "denies": "Denies",
    "denies_count": "Denies",
    "hero_damage": "Hero Damage",
    "tower_damage": "Tower Damage",
    "hero_healing": "Healing"
}

DOTA_EXPECTED_BUILDINGS = [
    {
        2: [0]
    },
    {
        0: [1, 2, 3],
        1: [1, 1]
    },
    {
        0: [1, 2, 3, 4, 4],
        1: [1, 1]
    },
    {
        0: [1, 2, 3],
        1: [1, 1]
    }
]

DOTA_CACHED_CONSTANTS = {}


class DotaMatch(Match):
    known_matches: set[int] = set()
    steam_id: int
    gamer_ids: set[int]
    party_size: int
    timestamp: int
    polls: int
    channel: discord.TextChannel
    task: TaskWrapper | None

    def __init__(self, steam_id: int, gamers: set[discord.Member], channel: discord.TextChannel):
        self.steam_id = steam_id
        self.gamer_ids = {gamer.id for gamer in gamers}
        self.party_size = len(gamers)
        self.timestamp = generate_timestamp(utcnow() - datetime.timedelta(seconds=60))
        self.polls = 0
        self.channel = channel
        self.task = None
        self.start_check()

    def start_check(self):
        self.task = create_task(self.check_match(), name="Match Check")

    def close_match(self):
        self.task.cancel(msg="Closing match")

    def get_poll_interval(self) -> int | float:
        if self.polls < MATCH_POLL_INTERVAL_COUNT:
            return MATCH_POLL_INTERVALS[self.polls]
        else:
            return MATCH_POLL_INTERVAL_LAST

    @staticmethod
    def get_duration(time: int) -> str:
        minutes, seconds = divmod(time, 60)
        return f"{minutes}:{seconds:02}"

    @staticmethod
    def get_type(match: dict[str, Any]) -> str:
        # match type
        resources = DotaAPI.get_constants("lobby_type", "game_mode")
        lobby_type = DotaAPI.query_match_constant(resources, match, "lobby_type")
        game_mode = DotaAPI.query_match_constant(resources, match, "game_mode")
        return f"{lobby_type} {game_mode}"

    def query_realtime(self, channel: discord.TextChannel, gamer_id: int):
        if not client.steamapi:
            return

        # request spectate for steam server ID
        client.dotaclient.send(EDOTAGCMsg.EMsgGCSpectateFriendGame, {
            "steam_id": make_steam64(self.steam_id)
        })

        def handle_resp(message):
            live_result = message.watch_live_result if message else 0
            server_steamid = message.server_steam_id if message else 0
            if live_result == 0 and server_steamid:
                tries = 0
                while True:
                    # this endpoint regularly fails, so we retry a few times
                    tries += 1
                    try:
                        resp = client.steamapi.IDOTA2MatchStats_570.GetRealtimeStats(server_steam_id=str(server_steamid))
                        break
                    except Exception as e:
                        if tries >= 10:
                            print("Failed to get realtime stats:", e)
                            msg_task = create_task(channel.send("Match not started yet."))
                            return
                        wait_backoff(tries)
                match = resp["match"]
                teams = resp.get("teams")
                team_id = 0

                per_player_stats = {
                    0: {
                        "level": 0,
                        "lh_count": 0,
                        "denies_count": 0,
                    },
                    1: {
                        "level": 0,
                        "lh_count": 0,
                        "denies_count": 0,
                    }
                }
                adv_map = None
                if teams:
                    level_to_xp = DotaAPI.get_constants("xp_level")["xp_level"]
                    for team in teams:
                        team_idx = team["team_number"] - 2
                        team_adv = per_player_stats[team_idx]
                        for player in team["players"]:
                            for key in team_adv.keys():
                                val = player[key]
                                if key == "level":
                                    val = level_to_xp[val - 1]
                                team_adv[key] += val
                            if player["accountid"] == self.steam_id:
                                team_id = team_idx
                    other_team = (team_id + 1) % 2
                    net_worth_adv = teams[team_id]["net_worth"] - teams[other_team]["net_worth"]
                    adv_map = {"level": 0, "net_worth": net_worth_adv, "\u200B": "\u200B"}
                    for key in per_player_stats[team_id].keys():
                        adv_map[key] = per_player_stats[team_id][key] - per_player_stats[other_team][key]
                    adv_map["\u200B\u200B"] = "\u200B"

                buildings = resp.get("buildings")
                buildings_populated = False
                destroyed_buildings = None

                if buildings and len(buildings) > 2:
                    # team -> lane[] -> type{} -> tier{}
                    destroyed_buildings = {
                        2: deepcopy(DOTA_EXPECTED_BUILDINGS),
                        3: deepcopy(DOTA_EXPECTED_BUILDINGS)
                    }
                    for building in buildings:
                        team = building["team"]
                        if team == 0:
                            continue
                        lane = building["lane"]
                        if lane == 0:
                            continue
                        btype = building["type"]
                        # ancient
                        if btype == 2:
                            continue
                        destroyed = building["destroyed"]
                        if destroyed:
                            continue
                        buildings_populated = True
                        destroyed_buildings[team][lane][btype].remove(building["tier"])

                highest_towers = {2: 0, 3: 0}
                rax_count = {2: 0, 3: 0}

                if buildings_populated:
                    for team, lanes in destroyed_buildings.items():
                        for lane in range(1, 4):
                            btypes = lanes[lane]
                            # get lowest tower destroyed
                            tower = btypes[0][-1] if len(btypes[0]) else 0
                            if highest_towers[team] < tower:
                                highest_towers[team] = tower
                            rax = len(btypes[1])
                            rax_count[team] += rax

                # match type
                match_type = DotaMatch.get_type(match)

                # match ID
                match_id = match["match_id"]

                # match time
                match_time = generate_datetime(match["start_timestamp"])

                # game state
                state = match["game_state"]
                state_name = DOTA_STATE.get(state, "Unknown")

                # get the full lobby time if the game isn't in progress yet
                if state in (5, 6, 7):
                    duration = DotaMatch.get_duration(match["game_time"])
                    duration_title = "Game Time"
                else:
                    duration = DotaMatch.get_duration(match["timestamp"])
                    duration_title = "Pre-Game Time"

                embed = discord.Embed(
                    colour=discord.Colour.blue(),
                    title=f"Match {match_id}",
                    timestamp=match_time,
                )

                embed.add_field(name="Type", value=match_type, inline=False)

                embed.add_field(name="State", value=state_name, inline=False)

                if teams:
                    radiant_score = teams[0]["score"]
                    dire_score = teams[1]["score"]
                    embed.add_field(name="Score", value=f"{radiant_score}-{dire_score}", inline=False)

                    embed.add_field(name="Team", value="Dire" if team_id == 1 else "Radiant", inline=False)

                    embed.add_field(name="Team Advantage", value="─"*40, inline=False)

                    for k, v in adv_map.items():
                        label = DOTA_ADV_LABELS.get(k, k)
                        embed.add_field(name=label, value=v)

                if buildings_populated:
                    embed.add_field(name="Building Status", value="─"*40, inline=False)

                    if highest_towers[2]:
                        embed.add_field(name="Radiant Towers", value=f"Tier {highest_towers[2]} destroyed")
                    else:
                        embed.add_field(name="Radiant Towers", value=f"No towers destroyed")
                    if highest_towers[3]:
                        embed.add_field(name="Dire Towers", value=f"Tier {highest_towers[3]} destroyed")
                    else:
                        embed.add_field(name="Dire Towers", value=f"No towers destroyed")
                    embed.add_field(name="\u200B", value="\u200B")
                    embed.add_field(name="Radiant Barracks", value=f"{rax_count[2]} destroyed")
                    embed.add_field(name="Dire Barracks", value=f"{rax_count[3]} destroyed")
                    embed.add_field(name="\u200B", value="\u200B")

                embed.set_footer(text=f"{duration_title}: {duration}")

                msg_task = create_task(channel.send(embed=embed))
            elif live_result == 4:
                msg_task = create_task(channel.send("No live match found."))
            else:
                msg_task = create_task(channel.send(f"Failed to get live match data: error code {live_result}"))

        client.dotaclient.once(EDOTAGCMsg.EMsgGCSpectateFriendGameResponse, handle_resp)

    async def get_recent_match(self) -> dict[str, Any] | None:
        matches: list[dict[str, Any]] = await DotaAPI.get_matches(
            self.steam_id,
            limit=1,
            date=1,
            significant=0
        )
        if matches:
            match = matches[0]
            # we've seen this match before
            match_id = match["match_id"]
            if match_id in DotaMatch.known_matches:
                return None
            DotaMatch.known_matches.add(match_id)
            # if this match wasn't relevant for the game we started
            party_size = match["party_size"] or 5
            if party_size < self.party_size:
                return None
            # if this match started before the game started
            if match["start_time"] < self.timestamp:
                return None
            return match
        else:
            return None

    async def check_match(self):
        await asyncio.sleep(self.get_poll_interval())
        match = await self.get_recent_match()
        self.polls += 1
        if match:
            is_dire = match["player_slot"] > 127
            team_num = 1 if is_dire else 0
            won = match["radiant_win"] ^ is_dire

            # match ID
            match_id = match["match_id"]

            match_details = DotaAPI.get_match(match_id)

            # match time
            match_time = generate_datetime(match["start_time"])

            # create embed
            embed = discord.Embed(
                colour=discord.Colour.green() if won else discord.Colour.red(),
                title=f"Match {match_id}",
                url=f"https://www.dotabuff.com/matches/{match_id}",
                timestamp=match_time,
            )

            # force max width embed
            embed.set_image(url="https://i.stack.imgur.com/Fzh0w.png")

            # match type
            embed.add_field(name="Type", value=DotaMatch.get_type(match), inline=False)

            # score
            if match_details:
                radiant_score = match_details["radiant_score"]
                dire_score = match_details["dire_score"]
                embed.add_field(name="Score", value=f"{radiant_score}-{dire_score}", inline=False)

            # team
            embed.add_field(name="Team", value="Dire" if is_dire else "Radiant", inline=False)

            if match_details:
                adv_map = {
                    "xp_per_min": 0,
                    "net_worth": 0,
                    "hero_damage": 0,
                    "tower_damage": 0,
                    "hero_healing": 0,
                    "last_hits": 0,
                    "denies": 0,
                }
                for player in match_details["players"]:
                    player_team = player["team_number"]
                    for key in adv_map.keys():
                        if player_team == team_num:
                            adv_map[key] += player[key]
                        else:
                            adv_map[key] -= player[key]

                adv_map["xp_per_min"] *= match["duration"] / 60.0
                adv_map["xp_per_min"] = math.floor(adv_map["xp_per_min"])

                embed.add_field(name="Team Advantage", value="─"*40, inline=False)

                for k, v in adv_map.items():
                    label = DOTA_ADV_LABELS[k]
                    embed.add_field(name=label, value=v)

            # rank
            rank, rank_icon = DOTA_RANKS.get(match["average_rank"])
            embed.set_author(name=rank, icon_url=rank_icon)

            # duration
            embed.set_footer(text=f"Duration: {DotaMatch.get_duration(match['duration'])}")

            # send
            await self.channel.send(embed=embed)
            client.current_match = None
        elif self.polls < MATCH_MAX_POLLS:
            self.start_check()


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
        if num_gamers >= GAME_DATA[self.game_name].get("min", BUCKET_MIN):
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
                            gamers=gamers,
                            channel=self.channel
                        )
            else:
                # start the game up again
                client.now = utcnow()
                self.reset()
                check_task = create_task(self.start(client.now + DEFAULT_DELTA, mention=mention))
                return
        else:
            no_gamers = update_value(increment, "no_gamers", default=0)
            no_gamers_consecutive = update_value(increment, "no_gamers_consecutive", default=0)
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
                friend_code = id_arg.rstrip("/")
                if "steamcommunity.com/user/" in friend_code:
                    if not friend_code.startswith("http"):
                        friend_code = "https://" + friend_code
                    friend_code = friend_code.replace("steamcommunity.com/user", "s.team/p")
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
                    profile_id = profile_id.rstrip("/")
                    steam_id = try_steam_id(profile_id)
            # try vanity URL
            if not steam_id and client.steamapi:
                vanity = id_arg
                if "steamcommunity.com/id/" in vanity:
                    if not vanity.startswith("http"):
                        vanity = "https://" + vanity
                    vanity = vanity.replace("steamcommunity.com/id/", "")
                    vanity = vanity.replace("https://", "")
                    vanity = vanity.replace("http://", "")
                    vanity = vanity.rstrip("/")
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
                if len(args):
                    new_value = args.pop(0)
                    if "id" in option:
                        tmp = new_value
                        new_value = get_int(new_value, default=None)
                        if new_value is None:
                            new_value = tmp

                    set_value(option, new_value, table=client.settings_table)
                    await channel.send(f"{option}={new_value}")
                else:
                    client.settings_table.remove(Setting.k == option)
                    await channel.send(f"{option}=null")
            else:
                await channel.send(f"{option}={get_value(option, table=client.settings_table)}")
            return None
        # if buckets
        if control == "if":
            options.bucket = get_int(args.pop(0), BUCKET_MIN)
            return options
    else:
        if control == "status":
            async with channel.typing():
                if client.current_match and client.steamapi:
                    client.current_match.query_realtime(channel, gamer.id)
                else:
                    await channel.send("No live match found.")
            return None

    # if we didn't find the control, just ignore
    return options


FUZZ_THRESHOLD = 75


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
    return ratio > FUZZ_THRESHOLD


async def main(debug, no_2fa):
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

    opendota_api_key = os.getenv("GAME_BOT_OPENDOTA")
    opendota_api_key = QueryApiKey(opendota_api_key) if opendota_api_key else None

    # create opendota API HTTP client
    async with (
        httpx.AsyncClient(base_url="https://api.opendota.com/api", timeout=10.0, http2=True, auth=opendota_api_key) as opendota
    ):
        # create our client, limit messages to what we need to keep track of
        client = GameClient(
            opendota=opendota,
            debug=debug,
            no_2fa=no_2fa,
            max_messages=500,
            intents=intents,
            allowed_mentions=mentions,
        )

        # cache constants
        await DotaAPI.query_constants("lobby_type", "game_mode", "xp_level")

        # start the client
        async with client as _client:
            await _client.start(os.environ["GAME_BOT_TOKEN"])

    client.steamclient.close()


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


def start_bot(debug, no_2fa):
    discord.utils.setup_logging(level=logging.DEBUG if debug else logging.INFO)
    asyncio.run(main(debug, no_2fa))


if __name__ == "__main__":
    # TODO: arg parse
    start_bot(False, True)
