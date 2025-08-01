from __future__ import annotations

import gevent

gevent.config.loop = "libuv"

import gevent.monkey

gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
gevent.monkey.patch_dns()

import asyncio
import base64
import dataclasses
import datetime
import hashlib
import io
import itertools
import logging
import math
import os
import random
import re
import socket
import string
import traceback
from collections import defaultdict
from copy import deepcopy
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, available_timezones

import aiohttp
import arrow
import asyncio_gevent
import dateparser
import discord
import httpx
import orjson
from BetterJSONStorage import BetterJSONStorage
from datetimerange import DateTimeRange
from discord import AllowedMentions, Intents
from discord.ext.commands import MemberNotFound
from dota2.client import Dota2Client
from dota2.proto_enums import EDOTAGCMsg
from httpx_auth import QueryApiKey
from icu import TimeZone
from numpy import interp
from steam.client import SteamClient
from steam.enums.common import EFriendRelationship
from steam.steamid import SteamID, from_invite_code, make_steam64
from steam.webapi import WebAPI
from thefuzz import fuzz
from tinydb import Query, TinyDB

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from steam.client.user import SteamUser

if os.name == "nt":
    # handle Windows imports
    # for colored terminal
    import colorama

    colorama.init()
else:
    # handle POSIX imports
    # for uvloop
    # while we have steam client, we cannot use uvloop due to gevent
    import uvloop

    uvloop.install()

asyncio.set_event_loop_policy(asyncio_gevent.EventLoopPolicy())

DEBUGGING = int(os.environ.get("GAME_BOT_DEBUG", 0))


def crypt_str(text: str):
    data = text.encode("utf-8")
    return hashlib.blake2b(data).hexdigest()


def print_debug(*args: Any, **kwargs: Any) -> None:
    if DEBUGGING > 0:
        print(*args, **kwargs)


def create_task(coro: Coroutine, *, name: str = None) -> TaskWrapper:
    task = asyncio.create_task(coro, name=name)
    return TaskWrapper(task)


def dispatch_task(coro: Coroutine):
    asyncio.run_coroutine_threadsafe(coro, client.loop)


def get_channel(
    channel: discord.TextChannel | discord.Thread | None,
    guild: discord.Guild | None = None,
) -> discord.TextChannel | discord.Thread:
    if not channel and not guild:
        return None
    guild = (guild or channel.guild) if channel else guild
    if not guild:
        return None
    guild_handler = client.guild_handlers.get(guild.id)
    if not guild_handler:
        return None
    settings_channel = get_value("channel_id", table=guild_handler.settings_table)
    if settings_channel:
        settings_channel = guild.get_channel_or_thread(settings_channel)
    return settings_channel or channel


MAX_BACKOFF = 2.0
BASE_BACKOFF = 0.016


def get_backoff(failures: int) -> float:
    return random.uniform(0, min(MAX_BACKOFF, BASE_BACKOFF * 2**failures))


def wait_backoff(failures: int):
    gevent.sleep(get_backoff(failures))


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
    return f"<t:{timestamp}>"


def generate_datetime(timestamp: int) -> datetime.datetime:
    """
    Gets a datetime from UNIX timestamp.
    """
    delta = datetime.timedelta(seconds=timestamp)
    return EPOCH + delta


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


class DiscordUtil:
    # from https://github.com/Rapptz/discord.py/blob/master/discord/ext/commands/converter.py
    # Copyright (c) 2015-present Rapptz
    # MIT License

    _ID_REGEX = re.compile(r"(\d{15,20})$")

    @staticmethod
    def _get_id_match(argument):
        return DiscordUtil._ID_REGEX.match(argument)

    @staticmethod
    async def query_member_named(
        guild: discord.Guild,
        argument: str,
    ) -> discord.Member | None:
        cache = guild._state.member_cache_flags.joined
        if len(argument) > 5 and argument[-5] == "#":
            username, _, discriminator = argument.rpartition("#")
            members = await guild.query_members(username, limit=100, cache=cache)
            return discord.utils.get(
                members,
                name=username,
                discriminator=discriminator,
            )
        else:
            members = await guild.query_members(argument, limit=100, cache=cache)
            return discord.utils.find(
                lambda m: m.name == argument or m.nick == argument,
                members,
            )

    @staticmethod
    async def query_member_by_id(
        guild: discord.Guild,
        user_id: int,
    ) -> discord.Member | None:
        ws = client._get_websocket(shard_id=guild.shard_id)
        cache = guild._state.member_cache_flags.joined
        if ws.is_ratelimited():
            # If we're being rate limited on the WS, then fall back to using the HTTP API
            # So we don't have to wait ~60 seconds for the query to finish
            try:
                member = await guild.fetch_member(user_id)
            except discord.HTTPException:
                return None

            if cache:
                guild._add_member(member)
            return member

        # If we're not being rate limited then we can use the websocket to actually query
        members = await guild.query_members(limit=1, user_ids=[user_id], cache=cache)
        if not members:
            return None
        return members[0]

    @staticmethod
    async def convert_user_arg(
        message: discord.Message,
        argument: str,
    ) -> discord.Member | None:
        match = DiscordUtil._get_id_match(argument) or re.match(
            r"<@!?(\d{15,20})>$",
            argument,
        )
        result = None
        guild = message.guild
        user_id = None

        if match is None:
            # not a mention...
            if guild:
                result = guild.get_member_named(argument)
        else:
            user_id = int(match.group(1))
            if guild:
                result = guild.get_member(user_id) or discord.utils.get(
                    message.mentions,
                    id=user_id,
                )

        if not isinstance(result, discord.Member):
            if guild is None:
                raise MemberNotFound(argument)

            if user_id is not None:
                result = await DiscordUtil.query_member_by_id(guild, user_id)
            else:
                result = await DiscordUtil.query_member_named(guild, argument)

            if not result:
                raise MemberNotFound(argument)

        return result


class DotaAPI:
    resources = ["cluster", "lobby_type", "game_mode", "region", "xp_level"]
    last_constants_query: datetime.datetime = EPOCH

    @staticmethod
    async def get(*args, **kwargs) -> dict[str, Any] | list[dict[str, Any]]:
        try:
            resp = await client.opendota.get(*args, **kwargs)
        except Exception:
            print("Failed to get:", traceback.format_exc())
            return None
        if resp.status_code != 200:
            print("Failed to get:", resp.status_code, resp.text)
            return None
        return resp.json()

    @staticmethod
    async def post(*args, **kwargs) -> dict[str, Any] | list[dict[str, Any]]:
        try:
            resp = await client.opendota.post(*args, **kwargs)
        except Exception:
            print("Failed to post:", traceback.format_exc())
            return None
        if resp.status_code != 200:
            print("Failed to post:", resp.status_code, resp.text)
            return None
        return resp.json()

    @staticmethod
    async def query_constants():
        now = utcnow()
        if (now - DotaAPI.last_constants_query) < datetime.timedelta(hours=12):
            return DOTA_CACHED_CONSTANTS
        async with httpx.AsyncClient(
            base_url="https://api.github.com/repos/odota/dotaconstants/contents/build/",
            timeout=10.0,
            http2=True,
            headers={
                "User-Agent": "mastercoms/gamebot",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        ) as odotagh:
            for res in DotaAPI.resources:
                url = f"{res}.json"
                resp = await odotagh.get(url)
                if resp.status_code != 200:
                    print("Failed to get:", resp.status_code, resp.text)
                    DotaAPI.last_constants_query = EPOCH
                    continue
                data = resp.json()
                if data["encoding"] != "base64":
                    raise ValueError("GitHub contents API not returning base64!")
                content = data["content"]
                DOTA_CACHED_CONSTANTS[res] = orjson.loads(base64.b64decode(content).decode("utf-8"))
        DotaAPI.last_constants_query = now
        # print_debug(f"Queried {DotaAPI.resources} constants: {DOTA_CACHED_CONSTANTS}")

    @staticmethod
    def get_constants() -> dict[str, dict[str, Any]]:
        return DOTA_CACHED_CONSTANTS

    @staticmethod
    def get_constant_name(
        constants: dict[str, dict[str, Any]],
        resource: str,
        idx: int,
    ) -> str:
        constant = constants[resource]
        data = constant.get(str(idx), constant["0"])
        return data["name"].replace(f"{resource}_", "").replace("_", " ").title()

    @staticmethod
    def get_match_constant(
        match: dict[str, Any],
        resource: str,
    ):
        return DotaAPI.get_constant_name(
            DOTA_CACHED_CONSTANTS, resource, match[resource]
        )

    @staticmethod
    def get_region(cluster: int) -> str:
        region = DOTA_CACHED_CONSTANTS["cluster"][str(cluster)]
        return DOTA_CACHED_CONSTANTS["region"][str(region)]

    @staticmethod
    async def get_matches(account_id: int, **params) -> list[dict[str, Any]] | None:
        if not client.steamapi:
            return None
        tries = 0
        while True:
            # this endpoint regularly fails, so we retry a few times
            tries += 1
            try:
                resp = client.steamapi.IDOTA2Match_570.GetMatchHistory(
                    account_id=str(account_id),
                    **params,
                )
                break
            except Exception:
                if tries >= 3:
                    print("Failed to get match details:", traceback.format_exc())
                    return None
                await asyncio.sleep(get_backoff(tries))
        return resp["result"]["matches"]

    @staticmethod
    async def get_basic_matches(steam_id: int, **params) -> list[dict[str, Any]]:
        url = f"/players/{steam_id}/matches"
        resp = await DotaAPI.get(url, params=params)
        return resp

    @staticmethod
    async def get_recent_matches(steam_id: int) -> list[dict[str, Any]]:
        url = f"/players/{steam_id}/recentMatches"
        resp = await DotaAPI.get(url)
        return resp

    @staticmethod
    async def get_match_steam(match_id: int) -> dict[str, Any] | None:
        if not client.steamapi:
            return None
        tries = 0
        while True:
            # this endpoint regularly fails, so we retry a few times
            tries += 1
            try:
                resp = client.steamapi.IDOTA2Match_570.GetMatchDetails(
                    match_id=str(match_id),
                    include_persona_names=True,
                )
                print_debug(f"get_match: {resp}")
                break
            except Exception:
                if tries >= 10:
                    print("Failed to get match details:", traceback.format_exc())
                    return None
                await asyncio.sleep(get_backoff(tries))
        return resp["result"]

    @staticmethod
    async def get_match_by_seq_num(seq_num: int) -> dict[str, Any] | None:
        if not client.steamapi:
            return None
        tries = 0
        while True:
            # this endpoint regularly fails, so we retry a few times
            tries += 1
            try:
                resp = client.steamapi.IDOTA2Match_570.GetMatchHistoryBySequenceNum(
                    start_at_match_seq_num=str(seq_num),
                    matches_requested=1,
                )
                if resp["status"] != 1:
                    raise ValueError(
                        f"Match seq {seq_num} error: {resp['statusDetail']}"
                    )
                if len(resp["matches"]) < 1:
                    raise ValueError(f"Match seq {seq_num} not found")
                print_debug(f"get_match_by_seq_num: {resp}")
                break
            except Exception:
                if tries >= 10:
                    print("Failed to get match details:", traceback.format_exc())
                    return None
                await asyncio.sleep(get_backoff(tries))
        return resp["result"]["matches"][0]

    @staticmethod
    async def request_parse(match_id: int) -> None:
        return await DotaAPI.post(f"/request/{match_id}")

    @staticmethod
    async def get_parsed_match(match_id: int) -> dict[str, Any] | None:
        url = f"/matches/{match_id}"
        match = await DotaAPI.get(url)
        return match

    @staticmethod
    async def get_match(
        match_id: int, match_seq_num: int | None
    ) -> dict[str, Any] | None:
        match = None
        if match_seq_num is not None:
            match = await DotaAPI.get_match_by_seq_num(match_seq_num)
        if not match:
            match = await DotaAPI.get_parsed_match(match_id)
        return match


class SteamWorker:
    def __init__(self):
        self.logged_on_once = False

        self.steam = worker = SteamClient()
        worker.set_credential_location(".")

        @worker.on("error")
        def handle_error(result):
            print("Logon result:", traceback.format_exc())

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

            print("⎯" * 30)
            print("Logged on as:", worker.user.name)
            print("Community profile:", worker.steam_id.community_url)
            print("Last logon:", worker.user.last_logon)
            print("Last logoff:", worker.user.last_logoff)
            print("⎯" * 30)

        @worker.on("disconnected")
        def handle_disconnect():
            print("Disconnected.")

            if self.logged_on_once:
                print("Reconnecting...")
                worker.reconnect(maxdelay=30)

        @worker.on("reconnect")
        def handle_reconnect(delay):
            print(
                f"Reconnect in {delay}...",
            )
            # reconnect dota client as well
            client.dotaclient.exit()
            client.dotaclient.launch()

        @worker.friends.on("friend_invite")
        def handle_friend_invite(user: SteamUser):
            mutual = MUTUAL_STEAM_ID
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
                    resp = client.steamapi.ISteamUser.GetFriendList(
                        steamid=user_steam_id,
                    )
                except Exception:
                    try:
                        # if it's private, try getting the friends list of the mutual target
                        resp = client.steamapi.ISteamUser.GetFriendList(steamid=mutual)
                    except Exception:
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
        two_factor_code = None if no_2fa else input("Enter 2FA code: ")
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


PUNCTUATION_TRANS = str.maketrans(string.punctuation, " " * len(string.punctuation))
WHITESPACE_TRANS = str.maketrans(string.whitespace, " " * len(string.whitespace))


def preprocess_text(text):
    """Method for pre-processing the given response text.

    It:
    * replaces all punctuations with spaces
    * replaces all whitespace characters (tab, newline etc) with spaces
    * removes trailing and leading spaces
    * removes double spaces
    * changes to lowercase
    :param text: the text to be cleaned
    :return: cleaned text"""

    text = text.translate(PUNCTUATION_TRANS)
    text = text.translate(WHITESPACE_TRANS)
    text = text.strip().lower()
    text = re.sub(" +", " ", text)
    return text


class GameGuildHandler:
    def __init__(self, *args, **kwargs):
        self.current_game: Game | None = None
        self.current_marks: dict[
            str,
            dict[discord.Member, tuple[DateTimeRange, int]],
        ] = {}
        for game in GAMES:
            self.current_marks[game] = {}
        self.current_match: DotaMatch | Match | None = None

        self.lock: asyncio.Lock | None = None

        self.play_queue: list[Path] = []

        self.guild: discord.Guild = kwargs["guild"]

        guild_id = self.guild.id
        guild_id_hash = crypt_str(str(guild_id))

        self.guild_db = TinyDB(Path(f"./db_{guild_id_hash}.json"))
        self.backup_table = self.guild_db.table("backup")
        self.settings_table = self.guild_db.table("settings")
        self.server_settings = self.guild_db.table("server_settings")
        self.local_timezone = (
            get_value("timezone", table=self.server_settings) or LOCAL_TIMEZONE
        )
        self.local_tzinfo = ZoneInfo(self.local_timezone)
        self.keyword = get_value("keyword", table=self.server_settings) or KEYWORD
        self.keyword_subject_suffix = "rs" if self.keyword.endswith("e") else "ers"
        self.keyword_title = self.keyword[0].upper() + self.keyword[1:]
        self.extra_failure_message = (
            get_value("failure_message", table=self.server_settings)
            or EXTRA_FAILURE_MESSAGE
        )
        self.extra_win_message = (
            get_value("win_message", table=self.server_settings) or EXTRA_WIN_MESSAGE
        )
        self.extra_loss_message = (
            get_value("loss_message", table=self.server_settings) or EXTRA_LOSS_MESSAGE
        )
        self.server_game_data = get_value("game_data", table=self.server_settings)

    def get_server_game_data(self, game: str, data: str, default: Any = None) -> Any:
        val = self.server_game_data[game].get(data, default)
        if val is None:
            raise ValueError(f"Server game data {data} not found for {game}.")
        return val

    async def restore_backup(self):
        save = get_value("saved", table=self.backup_table)
        if not save or self.current_game or self.current_match:
            return
        try:
            guild = self.guild
            channel = guild.get_channel_or_thread(save["channel"])
            author = guild.get_member(save["author"])
            game_name = save["game_name"]
            future = datetime.datetime.fromisoformat(save["future"])
            if utcnow() - future > MAX_CHECK_DELTA:
                return
            print_debug("Resuming saved", save)
            restored_game = Game(channel, author, game_name)
            restored_game.future = future
            restored_game.group_buckets = {
                int(k): {guild.get_member(m) for m in v}
                for k, v in save["group_buckets"].items()
            }
            restored_game.gamer_buckets = {
                guild.get_member(int(k)): v for k, v in save["gamer_buckets"].items()
            }
            restored_game.timestamp = save["timestamp"]
            restored_game.is_checking = save["is_checking"]
            restored_game.message = await channel.fetch_message(save["message"])
            restored_game.has_initial = save["has_initial"]
            restored_game.was_scheduled = save["was_scheduled"]
            restored_game.base_mention = save["base_mention"]
            restored_game.check_delta = datetime.timedelta(seconds=save["check_delta"])
            if save["scheduled_event"]:
                restored_game.scheduled_event = guild.get_scheduled_event(
                    save["scheduled_event"]
                )
            self.current_game = restored_game
            self.current_game.start_countdown()
        except Exception:
            print("Failed to restore game.", traceback.format_exc())
            self.current_game = None

    async def restore_match(self):
        save = get_value("match", table=self.backup_table)
        if not save or self.current_game or self.current_match:
            return
        try:
            guild = self.guild
            timestamp = save["timestamp"]
            if generate_timestamp(utcnow()) - timestamp >= MATCH_MAX_POLL_LENGTH:
                return
            print_debug("Resuming match", save)
            account_ids = set(save["account_ids"])
            gamers = {guild.get_member(gamer_id) for gamer_id in save["gamer_ids"]}
            channel = guild.get_channel_or_thread(save["channel"])
            restored_match = DotaMatch(
                account_ids, gamers, channel, should_check=False, serialize=False
            )
            restored_match.last_match = save.get("last_match", 0)
            restored_match.polls = save["polls"]
            restored_match.timestamp = timestamp
            restored_match.serialize = True
            restored_match.save()
            self.current_match = restored_match
            self.current_match.start_check()
        except Exception:
            print("Failed to restore match.", traceback.format_exc())
            self.current_match = None

    def restore_marks(self):
        marks = get_value("marks", table=self.backup_table)
        if not marks:
            return
        now = utcnow()
        for game, game_marks in marks.items():
            for gamer_id, params in game_marks.items():
                end = datetime.datetime.fromisoformat(params[1])
                if now > end:
                    continue
                if end - now <= MAX_CHECK_DELTA:
                    continue
                self.current_marks[game][self.guild.get_member(int(gamer_id))] = (
                    DateTimeRange(datetime.datetime.fromisoformat(params[0]), end),
                    params[2],
                )

    async def resume(self):
        """
        Resumes from backups.
        """
        self.restore_marks()
        await self.restore_match()
        await self.restore_backup()

    async def handle_game_command(self):
        pass

    async def handle_voiceline_command(
        self,
        author: discord.Member,
        channel: discord.TextChannel | discord.Thread,
        content: str,
    ):
        # if in voice
        voice_state = author.voice
        if not voice_state or not voice_state.channel:
            return
        voice_channel = voice_state.channel
        if content.lower() == "random voiceline":
            responses = client.responses_table.all()
        else:
            response_text = preprocess_text(content)
            responses = client.responses_table.search(
                Response.processed_text == response_text,
            )
        if len(responses):
            response = random.choice(responses)
            link = response["response_link"]
            file_name = link.split("/")[-1]
            cache_path = self.responses_cache / file_name
            tries = 0
            while True:
                tries += 1
                try:
                    if not cache_path.exists():
                        with open(cache_path, "wb") as download_file:
                            async with self.http_client.stream("GET", link) as stream:
                                async for chunk in stream.aiter_bytes():
                                    download_file.write(chunk)
                    if cache_path.stat().st_size < 2048 or not cache_path.exists():
                        msg = "Corrupted file download"
                        raise ValueError(msg)
                    break
                except Exception:
                    cache_path.unlink(missing_ok=True)
                    if tries >= 3:
                        print(
                            "Failed to download voice response:", traceback.format_exc()
                        )
                        await get_channel(channel).send(
                            "Error: failed to download response, please try again",
                        )
                        return
                    await asyncio.sleep(get_backoff(tries))

            voice_client: discord.VoiceClient | None = None
            for voice_client in self.voice_clients:
                if voice_client.channel == voice_channel:
                    break
                if voice_client.guild == voice_channel.guild:
                    await voice_client.disconnect()

            if not voice_client:
                self.play_queue.clear()
                voice_client = await voice_channel.connect(self_deaf=True)

            def play(path: Path):
                voice_client.play(
                    discord.FFmpegOpusAudio(str(path)),
                    after=lambda err: dispatch_task(disconnect()),
                )

            async def disconnect():
                await asyncio.sleep(0.2)
                if self.play_queue:
                    path = self.play_queue.pop(0)
                    play(path)
                else:
                    await voice_client.disconnect()

            if voice_client.is_playing() or not voice_client.is_connected():
                self.play_queue.append(cache_path)
            else:
                play(cache_path)

    async def on_scheduled_event_user_add(
        self, event: discord.ScheduledEvent, user: discord.User
    ):
        if not self.current_game:
            return

        if self.current_game.is_checking:
            return

        if event.id != self.current_game.scheduled_event.id:
            return

        member = event.guild.get_member(user.id)

        await self.current_game.add_gamer(member, BUCKET_MIN, skip_exist=True)

    async def on_scheduled_event_user_remove(
        self, event: discord.ScheduledEvent, user: discord.User
    ):
        if not self.current_game:
            return

        if self.current_game.is_checking:
            return

        if event.id != self.current_game.scheduled_event.id:
            return

        member = event.guild.get_member(user.id)

        await self.current_game.remove_gamer(member)

    async def on_scheduled_event_delete(self, event: discord.ScheduledEvent):
        if not self.current_game:
            return

        if not self.current_game.scheduled_event:
            return

        if event.id != self.current_game.scheduled_event.id:
            return

        self.current.game.scheduled_event = None
        await self.current_game.cancel(utcnow())

    async def on_scheduled_event_update(
        self, before: discord.ScheduledEvent, after: discord.ScheduledEvent
    ):
        if not self.current_game:
            return

        if self.current_game.is_checking:
            return

        if not before or not after:
            return

        if after.id != self.current_game.scheduled_event.id:
            return

        if after.status == discord.EventStatus.cancelled:
            await self.current_game.cancel(utcnow())

        if before.start_time != after.start_time:
            await after.edit(
                start_time=before.start_time,
                reason="Game start time cannot be changed.",
            )

    async def on_message(self, message: discord.Message):
        """
        Handles new game messages.
        """
        # if not valid, don't do anything
        if not client.is_valid_message(message):
            return

        msg_channel = message.thread or message.channel
        channel = msg_channel

        if get_value("require_channel", default=False, table=self.server_settings):
            if msg_channel.id != get_channel(msg_channel).id:
                return

        original_content = message.content
        without_mention = message.content.replace(f"<@{client.user.id}>", "")
        has_mention = without_mention != message.content
        message.content = without_mention.strip()

        try:
            is_cmd = is_game_command(message.content.lower(), self.keyword)
            if not is_cmd and has_mention:
                message.content = original_content.replace(
                    f"<@{client.user.id}>", self.keyword
                ).strip()
                is_cmd = is_game_command(message.content.lower(), self.keyword)
            if is_cmd:
                async with self.lock:
                    # set our global now to when the message was made
                    # TODO: guild now
                    client.now = message.created_at

                    # set up our arg parser
                    print_debug(message.content)

                    args = message.content.split()[1:]
                    arg_str = " ".join(args)
                    gamer = message.author
                    options = GameOptions()
                    channel = get_channel(channel)

                    # consume all args
                    while args:
                        options = await consume_args(args, gamer, message, options)
                        # if we cleared options, then we stop here
                        if not options:
                            return

                    # it's a mark command
                    if options.remove_mark:
                        remove_mark(options.game, gamer)
                        await channel.send("Removed your availability marker.")
                        return

                    # check if it's sufficiently in the future
                    if options.future:
                        # TODO: guild now
                        delta = options.future - client.now
                        if options.start and delta <= MAX_CHECK_DELTA:
                            options.future = None
                        elif delta < MIN_CHECK_DELTA:
                            options.future = None

                    if options.start:
                        if not options.future:
                            await channel.send(
                                "Cannot mark availability so close to now."
                            )
                            return
                        self.current_marks[options.game][gamer] = (
                            DateTimeRange(options.start, options.future),
                            options.bucket,
                        )
                        old_marks = defaultdict(list)
                        game_intersections = defaultdict(list)
                        min_game_bucket = {}
                        max_game_bucket = {}
                        saved_marks = {}
                        for game, game_marks in self.current_marks.items():
                            saved_marks[game] = {}
                            for marker, params in game_marks.items():
                                dtrange, min_bucket = params
                                # TODO: guild now
                                if dtrange.end_datetime < client.now:
                                    old_marks[game].append(marker)
                                    continue
                                game_intersections[game].append(dtrange)
                                (
                                    current_min_game_bucket,
                                    _min_marker,
                                ) = min_game_bucket.get(game, (math.inf, None))
                                if min_bucket < current_min_game_bucket:
                                    min_game_bucket[game] = (min_bucket, marker)
                                (
                                    current_max_game_bucket,
                                    _max_marker,
                                ) = max_game_bucket.get(game, (0, None))
                                if min_bucket > current_max_game_bucket:
                                    max_game_bucket[game] = (min_bucket, marker)
                                saved_marks[game][str(marker.id)] = [
                                    dtrange.start_datetime.isoformat(),
                                    dtrange.end_datetime.isoformat(),
                                    min_bucket,
                                ]
                        for game, game_marks in old_marks.items():
                            for marker in game_marks:
                                remove_mark(game, marker)
                        set_value("marks", saved_marks, table=self.backup_table)

                        starting_game = False

                        # see if we can automatically start one
                        if not self.current_game:
                            for game, dtranges in game_intersections.items():
                                print_debug("dtranges", game, dtranges)
                                if len(dtranges) > 1:
                                    max_count = 1
                                    best_intersection = None
                                    comparisons = list(itertools.permutations(dtranges))
                                    print_debug(
                                        "dtrange comparisons", game, comparisons
                                    )
                                    for comparison in comparisons:
                                        count = 1
                                        intersection = comparison[0]
                                        print_debug(
                                            "dtrange intersection", game, intersection
                                        )
                                        for dtrange in comparison[1:]:
                                            intersection = intersection.intersection(
                                                dtrange
                                            )
                                            print_debug(
                                                "dtrange intersection",
                                                game,
                                                intersection,
                                            )
                                            if not intersection:
                                                break
                                            count += 1
                                        if count > max_count:
                                            max_count = count
                                            best_intersection = intersection

                                    if best_intersection:
                                        print_debug("starting", game, best_intersection)
                                        starting_game = True
                                        options.game = game
                                        options.future = (
                                            best_intersection.start_datetime
                                        )
                                        min_bucket, min_marker = min_game_bucket[game]
                                        options.bucket = min_bucket
                                        gamer = min_marker
                                        # check if it's sufficiently in the future
                                        if options.future:
                                            # TODO: guild now
                                            delta = options.future - client.now
                                            if delta <= MAX_CHECK_DELTA:
                                                options.future = (
                                                    client.now + MIN_SCHEDULED_DELTA
                                                )

                        if not starting_game:
                            min_bucket, max_bucket = get_bucket_bounds(
                                options.bucket,
                                options.game,
                            )
                            with_str = get_bucket_str(
                                min_bucket,
                                f"{self.keyword}{self.keyword_subject_suffix}",
                            )
                            msg = f"{gamer.display_name} can {self.keyword} between {print_timestamp(generate_timestamp(options.start), 't')} and {print_timestamp(generate_timestamp(options.future), 't')}{with_str}."
                            await channel.send(msg)
                            return

                    # are we going to start a game?
                    if not self.current_game:
                        # TODO: guild now
                        # if didn't get a date, default to delta
                        if not options.future:
                            options.future = client.now + MIN_CHECK_DELTA
                        # check for game update for constants
                        try:
                            await DotaAPI.query_constants()
                        except Exception:
                            print("Querying constants failed:", traceback.format_exc())
                        print_debug("Starting game", options.future, gamer)
                        self.current_game = Game(channel, gamer, options.game)
                        await self.current_game.start(options.future)
                        await self.current_game.add_initials(gamer, options.bucket)
                    elif options.future:
                        if True:
                            await channel.send(
                                f"You can't change the time of a {self.keyword_title}"
                            )
                            return
                        # if we're already checking, we can't change the time
                        if self.current_game.is_checking:
                            await channel.send(
                                f"You can't change the time of a {self.keyword_title} Check."
                            )
                            return
                        # if we're not checking, we can change the time
                        else:
                            print_debug("Changing game time", options.future, gamer)
                            await self.current_game.change_time(options.future)
                    else:
                        # add to game
                        await self.current_game.add_gamer(gamer, options.bucket)
            else:
                await self.handle_voiceline_command(
                    message.author,
                    msg_channel,
                    message.content,
                )
        except Exception:
            print("Unexpected error:", traceback.format_exc())
            await get_channel(msg_channel).send("An unexpected error occurred.")


class GameClient(discord.ext.commands.Bot):
    """The Discord client for this bot."""

    steamapi: WebAPI | None
    steamclient: SteamWorker | None
    dotaclient: Dota2Client | None

    def __init__(self, *args, **kwargs):
        """
        Creates a new Discord client.
        """
        global client
        self.opendota: httpx.AsyncClient = kwargs.pop("opendota")
        self.http_client: httpx.AsyncClient = kwargs.pop("http_client")
        self.debug: bool = kwargs.pop("debug", False)
        self.no_2fa: bool = kwargs.pop("no_2fa", True)
        client = self

        super().__init__(*args, **kwargs)

        self._resolver: aiohttp.AsyncResolver | None = None
        self._connector: aiohttp.TCPConnector | None = None

        self.now: datetime.datetime | None = None

        self.ready = False

        self.guild_handlers: dict[int, GameGuildHandler] = {}

        self.db = TinyDB(Path(f"./db.json"))
        self.players_table = self.db.table("players")
        self.timezone_table = self.db.table("timezone")
        self.responses_table = TinyDB(Path("responses.db"), storage=BetterJSONStorage)
        self.responses_cache = Path("./responses_cache")
        self.responses_cache.mkdir(exist_ok=True)
        steam_api_key = os.getenv("GAME_BOT_STEAM_KEY")
        if steam_api_key is not None:
            self.steamapi = WebAPI(key=steam_api_key)
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

    async def close(self) -> None:
        """
        Closes the client.
        """
        if self._resolver:
            await self._resolver.close()
        if self._resolver:
            await self._connector.close()
        if self.steamclient:
            self.steamclient.close()
        await super().close()

    async def on_ready(self):
        now = utcnow()
        for guild in self.guilds:
            game_handler = GameGuildHandler(guild=guild)
            game_handler.now = now
            game_handler.lock = asyncio.Lock()
            self.guild_handlers[guild.id] = game_handler
            await game_handler.resume()
        self.now = now
        self.ready = True
        print("Ready.")

    async def handle_game_command(self):
        # TODO
        guild_handler = self.guild_handlers.get(None)
        if not guild_handler:
            return

        await guild_handler.handle_game_command()

    async def handle_voiceline_command(
        self,
        author: discord.Member,
        channel: discord.TextChannel,
        content: str,
    ):
        guild_handler = self.guild_handlers.get(channel.guild.id)
        if not guild_handler:
            return

        await guild_handler.handle_voiceline_command(author, channel, content)

    async def on_app_command_game(self, interaction: discord.Interaction):
        # TODO
        await self.handle_game_command()

    async def on_app_command_voiceline(
        self,
        interaction: discord.Interaction,
        voiceline: str,
    ):
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            channel = get_channel(None, interaction.guild)
        if channel is None:
            return
        await self.handle_voiceline_command(interaction.user, channel, voiceline)

    def is_valid_message(self, message: discord.Message) -> bool:
        """
        Checks if the message is valid.
        """
        if not self.ready:
            return False
        if message.author.bot:
            return False
        if not message.content:
            return False
        return True

    async def on_scheduled_event_user_add(
        self, event: discord.ScheduledEvent, user: discord.User
    ):
        guild_handler = self.guild_handlers.get(event.guild.id)
        if not guild_handler:
            return

        await guild_handler.on_scheduled_event_user_add(event, user)

    async def on_scheduled_event_user_remove(
        self, event: discord.ScheduledEvent, user: discord.User
    ):
        guild_handler = self.guild_handlers.get(event.guild.id)
        if not guild_handler:
            return

        await guild_handler.on_scheduled_event_user_remove(event, user)

    async def on_scheduled_event_delete(self, event: discord.ScheduledEvent):
        guild_handler = self.guild_handlers.get(event.guild.id)
        if not guild_handler:
            return

        await guild_handler.on_scheduled_event_delete(event)

    async def on_scheduled_event_update(
        self, before: discord.ScheduledEvent, after: discord.ScheduledEvent
    ):
        if not before or not after:
            return

        guild_handler = self.guild_handlers.get(before.guild.id)
        if not guild_handler:
            return

        await guild_handler.on_scheduled_event_update(before, after)

    async def on_message(self, message: discord.Message):
        guild_handler = self.guild_handlers.get(message.guild.id)
        if not guild_handler:
            return

        await guild_handler.on_message(message)


client: GameClient | None = None

Player = Query()
Store = Query()
Response = Query()

TableValueType = int | float | str | bool
TableContainerValueType = "TableValueType | TableContainerType"
TableContainerType = list[TableContainerValueType] | dict[str, TableContainerValueType]
TableType = TableValueType | TableContainerType


def get_value(key: str, *, default: TableType = None, table=None) -> TableType:
    """
    Gets from the key value DB table.
    """
    table_interface = table if table is not None else client.db
    res = table_interface.get(Store.k == key)
    if res:
        return res["v"]
    return default


def set_value(key: str, val: TableType, *, table=None):
    """
    Sets to the key value DB table.
    """
    table_interface = table if table is not None else client.db
    print_debug(
        "set",
        key,
        val,
        table_interface.upsert({"k": key, "v": val}, Store.k == key),
    )


def del_value(key: str, *, table=None):
    """
    Deletes from the key value DB table.
    """
    table_interface = table if table is not None else client.db
    print_debug("del", key, table_interface.remove(Store.k == key))


def update_value(
    update_fn: Callable[[TableType], TableType],
    key: str,
    *,
    default: TableType = None,
    table=None,
) -> TableType:
    """
    Gets an existing value in the key value DB table and updates it using update_fn.
    :return: The new value
    """
    table_interface = table if table is not None else client.db
    old = get_value(key, default=default, table=table_interface)
    new = update_fn(old)
    set_value(key, new, table=table_interface)
    return new


@cache
def get_timezones() -> set[str]:
    timezones = available_timezones()
    timezones.remove("localtime")
    return timezones


@cache
def get_timezones_lower() -> set[str]:
    return {x.lower() for x in get_timezones()}


@cache
def get_timezone_list_str() -> str:
    list_str = ""
    timezones = sorted(list(get_timezones()))
    last_region = None
    for timezone in timezones:
        split = timezone.split("/")
        region = split[0]
        if last_region == None:
            last_region = region
        elif region != last_region:
            list_str = list_str[:-2] + "\n"
            last_region = region
        list_str += f"{timezone}, "
    return list_str[:-2]


@cache
def get_timezone_list_bytes() -> bytes:
    return get_timezone_list_str().encode("utf-8")


BUCKET_MIN: int = 2

MIN_CHECK_COUNTDOWN = 2.0 * 60.0
MAX_CHECK_COUNTDOWN = 5.0 * 60.0
MIN_CHECK_DELTA = datetime.timedelta(seconds=MIN_CHECK_COUNTDOWN)
MAX_CHECK_DELTA = datetime.timedelta(seconds=MAX_CHECK_COUNTDOWN)
MIN_SCHEDULED_DELTA = datetime.timedelta(seconds=MAX_CHECK_COUNTDOWN + 1.0)

MIN_CHECK_SCALING = 10.0 * 60.0
MAX_CHECK_SCALING = 60.0 * 60.0


def increment(val: int) -> int:
    """
    Increments a given int by 1, functionally.
    """
    return val + 1


# TODO: include estimated queue time?
# we assume the minimum game time is around 10 minutes
# then we assume the next appropriate time is 15 minutes
# then 20 minutes
# after 20 minutes, we keep trying to check every 1.5 minutes because it's likely the game will end at any moment
# keep in mind this includes turbo
MATCH_POLL_INTERVALS = [10 * 60, 5 * 60, 5 * 60, 1.5 * 60]
MATCH_POLL_INTERVAL_COUNT = len(MATCH_POLL_INTERVALS)
MATCH_POLL_INTERVAL_FIRST = MATCH_POLL_INTERVALS[0]
MATCH_POLL_INTERVAL_LAST = MATCH_POLL_INTERVALS[MATCH_POLL_INTERVAL_COUNT - 1]
MATCH_MAX_POLL_LENGTH = 2 * 60 * 60
MATCH_MAX_POLLS = MATCH_MAX_POLL_LENGTH // MATCH_POLL_INTERVAL_LAST
MATCH_WAIT_TIME = 15


class Match:
    pass


DOTA_RANKS = {
    None: (
        "Unknown",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/e7/SeasonalRank0-0.png/revision/latest?cb=20171124184310",
    ),
    0: (
        "Unknown",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/e7/SeasonalRank0-0.png/revision/latest?cb=20171124184310",
    ),
    10: (
        "Herald",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/Emoticon_Ranked_Herald.png/revision/latest?cb=20190212051846",
    ),
    11: (
        "Herald [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/85/SeasonalRank1-1.png/revision/latest?cb=20190130002445",
    ),
    12: (
        "Herald [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/ee/SeasonalRank1-2.png/revision/latest?cb=20190130002448",
    ),
    13: (
        "Herald [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/0/05/SeasonalRank1-3.png/revision/latest?cb=20190130002457",
    ),
    14: (
        "Herald [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6d/SeasonalRank1-4.png/revision/latest?cb=20190130002500",
    ),
    15: (
        "Herald [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2b/SeasonalRank1-5.png/revision/latest?cb=20190130002504",
    ),
    16: (
        "Herald [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/9/94/SeasonalRank1-6.png/revision/latest?cb=20190130002437",
    ),
    17: (
        "Herald [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/12/SeasonalRank1-7.png/revision/latest?cb=20190130002441",
    ),
    20: (
        "Guardian",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/43/Emoticon_Ranked_Guardian.png/revision/latest?cb=20190212051853",
    ),
    21: (
        "Guardian [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c7/SeasonalRank2-1.png/revision/latest?cb=20190130002542",
    ),
    22: (
        "Guardian [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2c/SeasonalRank2-2.png/revision/latest?cb=20190130002545",
    ),
    23: (
        "Guardian [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/f5/SeasonalRank2-3.png/revision/latest?cb=20190130002548",
    ),
    24: (
        "Guardian [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b4/SeasonalRank2-4.png/revision/latest?cb=20190130002552",
    ),
    25: (
        "Guardian [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/32/SeasonalRank2-5.png/revision/latest?cb=20190130002555",
    ),
    26: (
        "Guardian [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/72/SeasonalRank2-6.png/revision/latest?cb=20190130002558",
    ),
    27: (
        "Guardian [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c6/SeasonalRank2-7.png/revision/latest?cb=20190130002601",
    ),
    30: (
        "Crusader",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2d/Emoticon_Ranked_Crusader.png/revision/latest?cb=20190212051912",
    ),
    31: (
        "Crusader [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/82/SeasonalRank3-1.png/revision/latest?cb=20190130002626",
    ),
    32: (
        "Crusader [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6e/SeasonalRank3-2.png/revision/latest?cb=20190130002629",
    ),
    33: (
        "Crusader [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/67/SeasonalRank3-3.png/revision/latest?cb=20190130002632",
    ),
    34: (
        "Crusader [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/SeasonalRank3-4.png/revision/latest?cb=20190130002635",
    ),
    35: (
        "Crusader [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b1/SeasonalRank3-5.png/revision/latest?cb=20190130002639",
    ),
    36: (
        "Crusader [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank3-6.png/revision/latest?cb=20190130002611",
    ),
    37: (
        "Crusader [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank3-6.png/revision/latest?cb=20190130002611",
    ),
    40: (
        "Archon",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/13/Emoticon_Ranked_Archon.png/revision/latest?cb=20190130004535",
    ),
    41: (
        "Archon [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/76/SeasonalRank4-1.png/revision/latest?cb=20190130002704",
    ),
    42: (
        "Archon [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/87/SeasonalRank4-2.png/revision/latest?cb=20190130002707",
    ),
    43: (
        "Archon [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/60/SeasonalRank4-3.png/revision/latest?cb=20190130002710",
    ),
    44: (
        "Archon [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/4a/SeasonalRank4-4.png/revision/latest?cb=20190130002714",
    ),
    45: (
        "Archon [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/a3/SeasonalRank4-5.png/revision/latest?cb=20190130002718",
    ),
    46: (
        "Archon [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/7e/SeasonalRank4-6.png/revision/latest?cb=20190130002651",
    ),
    47: (
        "Archon [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/9/95/SeasonalRank4-7.png/revision/latest?cb=20190130002654",
    ),
    50: (
        "Legend",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/18/Emoticon_Ranked_Legend.png/revision/latest?cb=20190212051924",
    ),
    51: (
        "Legend [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/7/79/SeasonalRank5-1.png/revision/latest?cb=20190130002757",
    ),
    52: (
        "Legend [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/5/52/SeasonalRank5-2.png/revision/latest?cb=20190130002839",
    ),
    53: (
        "Legend [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/88/SeasonalRank5-3.png/revision/latest?cb=20190130002819",
    ),
    54: (
        "Legend [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/25/SeasonalRank5-4.png/revision/latest?cb=20190130002822",
    ),
    55: (
        "Legend [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8e/SeasonalRank5-5.png/revision/latest?cb=20190130002826",
    ),
    56: (
        "Legend [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/2/2f/SeasonalRank5-6.png/revision/latest?cb=20190130002742",
    ),
    57: (
        "Legend [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c7/SeasonalRank5-7.png/revision/latest?cb=20190130002745",
    ),
    60: (
        "Ancient",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/d8/Emoticon_Ranked_Ancient.png/revision/latest?cb=20190216113137",
    ),
    61: (
        "Ancient [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/e/e0/SeasonalRank6-1.png/revision/latest?cb=20190130002941",
    ),
    62: (
        "Ancient [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/1c/SeasonalRank6-2.png/revision/latest?cb=20190130002945",
    ),
    63: (
        "Ancient [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/da/SeasonalRank6-3.png/revision/latest?cb=20190130002948",
    ),
    64: (
        "Ancient [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/db/SeasonalRank6-4.png/revision/latest?cb=20190130002951",
    ),
    65: (
        "Ancient [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/47/SeasonalRank6-5.png/revision/latest?cb=20190130002955",
    ),
    66: (
        "Ancient [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/bd/SeasonalRank6-6.png/revision/latest?cb=20190130002958",
    ),
    67: (
        "Ancient [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b8/SeasonalRank6-7.png/revision/latest?cb=20190130003003",
    ),
    70: (
        "Divine",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/6/6d/Emoticon_Ranked_Divine.png/revision/latest?cb=20190130004646",
    ),
    71: (
        "Divine [ 1 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/b/b7/SeasonalRank7-1.png/revision/latest?cb=20190130003022",
    ),
    72: (
        "Divine [ 2 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8f/SeasonalRank7-2.png/revision/latest?cb=20190130003026",
    ),
    73: (
        "Divine [ 3 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/fd/SeasonalRank7-3.png/revision/latest?cb=20190130003029",
    ),
    74: (
        "Divine [ 4 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/1/13/SeasonalRank7-4.png/revision/latest?cb=20190130003033",
    ),
    75: (
        "Divine [ 5 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/3/33/SeasonalRank7-5.png/revision/latest?cb=20190130003041",
    ),
    76: (
        "Divine [ 6 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/a1/SeasonalRank7-6.png/revision/latest?cb=20190130003039",
    ),
    77: (
        "Divine [ 7 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/c/c1/SeasonalRank7-7.png/revision/latest?cb=20190130003043",
    ),
    80: (
        "Immortal",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/f/f2/SeasonalRankTop0.png/revision/latest?cb=20180606220529",
    ),
    81: (
        "Immortal",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/d/df/SeasonalRankTop1.png/revision/latest?cb=20180606220541",
    ),
    82: (
        "Immortal [ Top 1000 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/a/ad/SeasonalRankTop2.png/revision/latest?cb=20180606220545",
    ),
    83: (
        "Immortal [ Top 100 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/8/8e/SeasonalRankTop3.png/revision/latest?cb=20180606220548",
    ),
    84: (
        "Immortal [ Top 10 ]",
        "https://static.wikia.nocookie.net/dota2_gamepedia/images/4/46/SeasonalRankTop4.png/revision/latest?cb=20180606220552",
    ),
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
    "hero_damage": "Raw Hero Damage",
    "scaled_hero_damage": "Hero Damage",
    "tower_damage": "Tower Damage",
    "scaled_tower_damage": "Real Tower Damage",
    "hero_healing": "Raw Healing",
    "scaled_hero_healing": "Healing",
}

DOTA_EXPECTED_BUILDINGS = [
    {2: [0]},
    {0: [1, 2, 3], 1: [1, 1]},
    {0: [1, 2, 3, 4, 4], 1: [1, 1]},
    {0: [1, 2, 3], 1: [1, 1]},
]

DOTA_CACHED_CONSTANTS = {}

handled = 1


async def race_realtime(channel: discord.TextChannel, gamer: discord.Member):
    global handled
    await asyncio.sleep(60.0)
    if handled == 0:
        handled = 2
        await channel.send(f"No live match found for {gamer.display_name}.")


async def res_realtime(channel: discord.TextChannel, embed: discord.Embed):
    global handled
    if handled == 0:
        handled = 1
        await channel.send(embed=embed)


async def fail_realtime(channel: discord.TextChannel, gamer: discord.Member, fmt: str):
    global handled
    if handled == 0:
        handled = 2
        await channel.send(fmt.format(gamer=gamer.display_name))


NUM_STARS = 5


def average_medal(values):
    num_stars = [(int(str(value)[0]) * NUM_STARS) + (value % 10) for value in values]
    avg_stars = sum(num_stars) / len(num_stars)
    return math.floor(avg_stars / NUM_STARS) * 10 + max(1, round(avg_stars % NUM_STARS))


def calc_pct_adv(us: float | int, them: float | int) -> str:
    diff = us - them
    mag = abs(diff)
    avg = (us + them) / 2
    pct = mag / avg if avg else 0
    return f"{diff:+d} ({pct:.1%})"


class DotaMatch(Match):
    last_match: int = 0
    account_ids: set[int]
    account_id: int
    gamer_ids: set[int]
    party_size: int
    timestamp: int
    polls: int
    channel: discord.TextChannel | discord.Thread
    serialize: bool
    task: TaskWrapper | None
    guild_handler: GameGuildHandler

    def __init__(
        self,
        account_ids: set[int],
        gamers: set[discord.Member],
        channel: discord.TextChannel | discord.Thread,
        should_check: bool = True,
        serialize: bool = True,
    ):
        self.serialize = serialize
        self.account_ids = account_ids
        self.guild_handler = client.guild_handlers[channel.guild.id]
        if self.account_ids:
            raw_friend_ids = [
                try_steam_id(account_id) for account_id in list(account_ids)
            ]
            if client.steamclient:
                friend_ids = [
                    friend_id.account_id
                    for friend_id in raw_friend_ids
                    if friend_id
                    and friend_id in client.steamclient.steam.friends
                    and client.steamclient.steam.friends[friend_id].relationship
                    == EFriendRelationship.Friend
                ]
                if not friend_ids:
                    friend_ids = raw_friend_ids
            else:
                friend_ids = raw_friend_ids
            self.account_id = random.choice(friend_ids)
            self.party_size = len(account_ids)
        else:
            self.account_id = None
            self.party_size = 6
        self.gamer_ids = {gamer.id for gamer in gamers}
        self.polls = 0
        self.channel = channel
        self.update_timestamp()
        self.task = None
        self.guild_handler = client.guild_handlers[channel.guild.id]
        if should_check:
            self.start_check()

    def save(self):
        """
        Saves the Match to disk, so that it may be resumed in case of a crash/update/restart.

        Must be called any time any of the class properties change.
        """
        if not self.serialize:
            return
        data = {
            "last_match": self.last_match,
            "account_ids": list(self.account_ids),
            "gamer_ids": list(self.gamer_ids),
            "timestamp": self.timestamp,
            "polls": self.polls,
            "channel": self.channel.id,
            "guild": self.channel.guild.id,
        }
        set_value("match", data, table=self.guild_handler.backup_table)

    def update_timestamp(self):
        self.timestamp = generate_timestamp(
            utcnow() - datetime.timedelta(seconds=MATCH_POLL_INTERVAL_FIRST),
        )
        self.save()

    def start_check(self):
        self.task = create_task(self.check_match(), name="Match Check")

    def close_match(self):
        self.task.cancel(msg="Closing match")

    def get_poll_interval(self) -> int | float:
        if self.polls < MATCH_POLL_INTERVAL_COUNT:
            poll_interval = MATCH_POLL_INTERVALS[self.polls]
        else:
            poll_interval = MATCH_POLL_INTERVAL_LAST
        return poll_interval + random.normalvariate(0, math.sqrt(poll_interval * 0.1))

    @staticmethod
    def get_duration(time: int) -> str:
        minutes, seconds = divmod(time, 60)
        return f"{minutes}:{seconds:02}"

    @staticmethod
    def get_type(match: dict[str, Any]) -> str:
        # match type
        lobby_type = DotaAPI.get_match_constant(match, "lobby_type")
        game_mode = DotaAPI.get_match_constant(match, "game_mode")
        return f"{lobby_type} {game_mode}"

    async def query_realtime(self, channel: discord.TextChannel, gamer: discord.Member):
        global handled
        if not client.steamapi:
            return

        # request spectate for steam server ID
        client.dotaclient.send(
            EDOTAGCMsg.EMsgGCSpectateFriendGame,
            {"steam_id": make_steam64(self.account_id), "live": False},
        )

        print_debug(f"Querying realtime match stats for {self.account_id}...")

        handled = 0

        def handle_resp(message):
            server_steamid = message.server_steamid if message else 0
            live_result = message.watch_live_result if message else 1
            print_debug(f"Got spectate response! {live_result} {server_steamid}")
            if server_steamid == "0":
                server_steamid = None
            if live_result == 0 and server_steamid:
                tries = 0
                while True:
                    # this endpoint regularly fails, so we retry a few times
                    tries += 1
                    try:
                        resp = client.steamapi.IDOTA2MatchStats_570.GetRealtimeStats(
                            server_steam_id=str(server_steamid),
                        )
                        print_debug(f"GetRealtimeStats: {resp}")
                        break
                    except Exception:
                        if tries >= 10:
                            print(
                                "Failed to get realtime stats:", traceback.format_exc()
                            )
                            dispatch_task(
                                fail_realtime(
                                    channel,
                                    gamer,
                                    "Match for {{gamer}} not started yet.",
                                )
                            )
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
                    },
                }
                adv_map = None
                if teams:
                    level_to_xp = DotaAPI.get_constants()["xp_level"]
                    for team in teams:
                        team_idx = team["team_number"] - 2
                        if team_idx not in per_player_stats:
                            per_player_stats[team_idx] = {
                                "level": 0,
                                "lh_count": 0,
                                "denies_count": 0,
                            }
                        team_adv = per_player_stats[team_idx]
                        for player in team["players"]:
                            for key in team_adv:
                                val = player[key]
                                if key == "level":
                                    val = level_to_xp[val - 1]
                                team_adv[key] += val
                            if player["accountid"] == self.account_id:
                                team_id = team_idx
                    if len(per_player_stats) == 2:
                        other_team = (team_id + 1) % 2
                        net_worth_adv = calc_pct_adv(
                            teams[team_id]["net_worth"], teams[other_team]["net_worth"]
                        )
                        adv_map = {
                            "net_worth": net_worth_adv,
                            "level": 0,
                            "\u200b": "\u200b",
                        }
                        for key in per_player_stats[team_id]:
                            adv_map[key] = calc_pct_adv(
                                per_player_stats[team_id][key],
                                per_player_stats[other_team][key],
                            )
                        adv_map["\u200b\u200b"] = "\u200b"

                buildings = resp.get("buildings")
                buildings_populated = False
                destroyed_buildings = None

                # match type
                match_type = DotaMatch.get_type(match)

                if buildings and len(buildings) > 2 and match_type != "Practice Custom":
                    # team -> lane[] -> type{} -> tier{}
                    destroyed_buildings = {
                        2: deepcopy(DOTA_EXPECTED_BUILDINGS),
                        3: deepcopy(DOTA_EXPECTED_BUILDINGS),
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
                            # get the lowest tower destroyed
                            tower = btypes[0][-1] if len(btypes[0]) else 0
                            if highest_towers[team] < tower:
                                highest_towers[team] = tower
                            rax = len(btypes[1])
                            rax_count[team] += rax

                # match ID
                match_id = match["match_id"]
                if not match_id or match_id == "0":
                    match_id = ""

                # check for uninitialized match time
                if abs(match["start_timestamp"] - self.timestamp) > 2592000:
                    match_time = None
                else:
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

                if teams and adv_map:
                    radiant_score = teams[0]["score"]
                    dire_score = teams[1]["score"]
                    embed.add_field(
                        name="Score",
                        value=f"{radiant_score}-{dire_score}",
                        inline=False,
                    )

                    embed.add_field(
                        name="Team",
                        value="Dire" if team_id == 1 else "Radiant",
                        inline=False,
                    )

                    embed.add_field(name="Team Advantage", value="⎯" * 40, inline=False)

                    for k, v in adv_map.items():
                        label = DOTA_ADV_LABELS.get(k, k)
                        embed.add_field(name=label, value=v)

                if buildings_populated:
                    embed.add_field(
                        name="Building Status",
                        value="⎯" * 40,
                        inline=False,
                    )

                    if highest_towers[2]:
                        embed.add_field(
                            name="Radiant Towers",
                            value=f"Tier {highest_towers[2]} destroyed",
                        )
                    else:
                        embed.add_field(
                            name="Radiant Towers",
                            value="No towers destroyed",
                        )
                    if highest_towers[3]:
                        embed.add_field(
                            name="Dire Towers",
                            value=f"Tier {highest_towers[3]} destroyed",
                        )
                    else:
                        embed.add_field(name="Dire Towers", value="No towers destroyed")
                    embed.add_field(name="\u200b", value="\u200b")
                    embed.add_field(
                        name="Radiant Barracks",
                        value=f"{rax_count[2]} destroyed",
                    )
                    embed.add_field(
                        name="Dire Barracks",
                        value=f"{rax_count[3]} destroyed",
                    )
                    embed.add_field(name="\u200b", value="\u200b")

                embed.set_footer(text=f"{duration_title}: {duration}")

                dispatch_task(res_realtime(channel, embed))
            elif live_result == 4 or live_result == 0:
                dispatch_task(
                    fail_realtime(channel, gamer, "No live match found for {gamer}.")
                )
            else:
                dispatch_task(
                    fail_realtime(
                        channel,
                        gamer,
                        "Failed to get live match data for {{gamer}}: error code {code}".format(
                            code=live_result
                        ),
                    ),
                )

        client.dotaclient.once(EDOTAGCMsg.EMsgGCSpectateFriendGameResponse, handle_resp)
        await race_realtime(channel, gamer)

    async def get_recent_match(self) -> dict[str, Any] | None:
        steam_matches: list[dict[str, Any]] = await DotaAPI.get_matches(
            self.account_id,
            matches_requested="1",
        )
        matches = steam_matches
        odota_matches = await DotaAPI.get_recent_matches(self.account_id)
        using_odota = False
        if odota_matches:
            if (
                matches is None
                or odota_matches[0]["match_id"] == matches[0]["match_id"]
            ):
                matches = odota_matches
                using_odota = True
        if matches:
            match = matches[0]
            # we've seen this match before
            match_id = match["match_id"]
            print_debug(f"last match: {DotaMatch.last_match}, this match: {match_id}")
            if match_id == DotaMatch.last_match:
                return None
            DotaMatch.last_match = match_id
            # if this match wasn't relevant for the game we started
            party_size = match.get("party_size", -1)
            if party_size == -1:
                party_size = 0
                fallback_team = None
                for player in match["players"]:
                    player_account = player["account_id"]
                    if player_account in self.account_ids:
                        party_size += 1
                        if not fallback_team:
                            fallback_team = player["team_number"]
                    if player_account == self.account_id:
                        match["player_team"] = player["team_number"]
                print_debug(
                    f"account: {self.account_id}, team {match.get('player_team')}, fallback: {fallback_team}"
                )
                if not match.get("player_team"):
                    if fallback_team:
                        match["player_team"] = fallback_team
                    else:
                        match["player_team"] = 0
            party_size = party_size or 5
            print_debug(f"party_size: {party_size}")
            match["party_size"] = party_size
            if party_size < self.party_size:
                return None
            # if this match started before the game started
            start_time = match["start_time"]
            print_debug(f"check times: {start_time} < {self.timestamp}")
            if start_time < self.timestamp:
                return None

            # prepare this match for return
            # odota excludes the seq num
            if using_odota and steam_matches:
                steam_match = next(
                    (x for x in steam_matches if x["match_id"] == match_id), None
                )
                if steam_match:
                    match["match_seq_num"] = steam_match["match_seq_num"]

            return match
        return None

    async def get_basic_match(self, match_id: int) -> dict[str, Any] | None:
        matches: list[dict[str, Any]] = await DotaAPI.get_basic_matches(
            self.account_id,
            significant=0,
        )
        if matches:
            for match in matches:
                if match["match_id"] == match_id:
                    return match
        return None

    async def check_match(self):
        # TODO: track each interval so we can know how long to wait for match details
        interval = self.get_poll_interval()
        await asyncio.sleep(interval)
        match = await self.get_recent_match()
        self.polls += 1
        print_debug(f"Match: {match}")
        if match:
            await self.post_match(match, extras=True)
        if self.polls < MATCH_MAX_POLLS:
            self.save()
            self.start_check()
        else:
            self.guild_handler.current_match = None
            del_value("match", table=self.guild_handler.backup_table)
            print_debug("Current match ended")

    async def post_match(self, match, extras=False):
        team_num = match.get("player_team")
        if team_num is None:
            is_dire = match.get("player_slot", 0) > 127
            team_num = 1 if is_dire else 0
        else:
            is_dire = team_num == 1

        print_debug(f"is_dire: {is_dire}")

        # match ID
        match_id = match["match_id"]
        match_seq_num = match.get("match_seq_num", None)

        print_debug(f"posting match_id: {match_id}")

        if extras:
            # get how close to our 15 seconds after match end waiting time we are
            initial_duration = match.get("duration")
            if initial_duration is None:
                # we're doing the expected value here because doing anything else would consistently overshoot or undershoot.
                initial_duration = 0
                for i in range(0, min(self.polls, MATCH_POLL_INTERVAL_COUNT - 1)):
                    initial_duration += MATCH_POLL_INTERVALS[i]
                left = self.polls - MATCH_POLL_INTERVAL_COUNT - 1
                initial_duration += MATCH_POLL_INTERVAL_LAST * left
            match_end_time = generate_datetime(match["start_time"] + initial_duration)
            detail_wait_time = datetime.timedelta(seconds=MATCH_WAIT_TIME) - (
                utcnow() - match_end_time
            )
            detail_wait = min(MATCH_WAIT_TIME, detail_wait_time.total_seconds())

            # reset to keep looking for games
            self.polls = 0
            self.update_timestamp()

            print_debug(f"detail_wait: {detail_wait}")

        # wait for match details to be available
        if extras and detail_wait > 0:
            await asyncio.sleep(MATCH_WAIT_TIME)
        parse_request = await DotaAPI.request_parse(match_id)
        print_debug(f"parse: {parse_request}")
        if extras:
            await asyncio.sleep(MATCH_WAIT_TIME)
        if extras:
            match_details = await DotaAPI.get_match(match_id, match_seq_num)
        else:
            match_details = match

        print_debug(f"match_details: {match_details}")

        won = match_details["radiant_win"] ^ is_dire

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
        embed.add_field(
            name="Type",
            value=DotaMatch.get_type(match_details),
            inline=True,
        )

        # match region
        embed.add_field(
            name="Region",
            value=DotaAPI.get_region(match_details["cluster"]),
            inline=True,
        )

        # match links
        embed.add_field(
            name="Links",
            value=f"[Dotabuff](https://www.dotabuff.com/matches/{match_id}) | [OpenDota](https://www.opendota.com/matches/{match_id}) | [Stratz](https://stratz.com/matches/{match_id})",
            inline=False,
        )

        # score
        if match_details:
            radiant_score = match_details["radiant_score"]
            dire_score = match_details["dire_score"]
            embed.add_field(
                name="Score",
                value=f"{radiant_score}-{dire_score}",
                inline=False,
            )

        # team
        if extras:
            embed.add_field(
                name="Team",
                value="Dire" if is_dire else "Radiant",
                inline=False,
            )

        if match_details:
            adv_map = {
                "net_worth": [0, 0],
                "xp_per_min": [0, 0],
                "hero_damage": [0, 0],
                "hero_healing": [0, 0],
                "tower_damage": [0, 0],
                "last_hits": [0, 0],
                "denies": [0, 0],
            }
            for player in match_details["players"]:
                player_team = player.get(
                    "team_number", 1 if player["player_slot"] > 127 else 0
                )
                for key in adv_map:
                    if player_team == team_num:
                        adv_map[key][0] += player[key]
                    else:
                        adv_map[key][1] += player[key]

            adv_map["xp_per_min"] = tuple(
                math.floor(i * match_details["duration"] / 60.0)
                for i in adv_map["xp_per_min"]
            )

            embed.add_field(name="Team Advantage", value="⎯" * 40, inline=False)

            for k, v in adv_map.items():
                value = calc_pct_adv(v[0], v[1])
                label = DOTA_ADV_LABELS[k]
                embed.add_field(name=label, value=value)

        # rank
        rank, rank_icon = DOTA_RANKS.get(None)
        if extras:
            basic_match = await self.get_basic_match(match_id)
            if basic_match:
                rank, rank_icon = DOTA_RANKS.get(basic_match["average_rank"])
        if rank == "Unknown" and match_details:
            ranked_match_details = match_details
            if "rank_tier" not in match_details["players"][0]:
                ranked_match_details = await DotaAPI.get_parsed_match(match_id)
            ranks = []
            for player in ranked_match_details["players"]:
                if player["rank_tier"]:
                    ranks.append(player["rank_tier"])
            average_rank = average_medal(ranks)
            rank, rank_icon = DOTA_RANKS.get(average_rank)

        embed.set_author(name=rank, icon_url=rank_icon)

        # duration
        embed.set_footer(
            text=f"Duration: {DotaMatch.get_duration(match_details['duration'])}",
        )

        print_debug(f"embed: {embed}")

        # send
        await self.channel.send(embed=embed)
        if extras:
            if won:
                if self.guild_handler.extra_win_message:
                    await self.channel.send(self.guild_handler.extra_win_message)
            elif self.guild_handler.extra_loss_message:
                await self.channel.send(self.guild_handler.extra_loss_message)


def get_bucket_bounds(min_bucket: int, game_name: str) -> tuple[int, int]:
    game_min = get_game_data(game_name, "min", BUCKET_MIN)
    game_max = get_game_data(game_name, "max", game_min)

    # out of bounds buckets
    min_bucket = max(game_min, min(min_bucket, game_max))

    return min_bucket, game_max


def get_bucket_str(min_bucket: int, keyworders: str) -> str:
    return f" with {min_bucket} {keyworders}" if min_bucket > BUCKET_MIN else ""


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
    channel: discord.TextChannel | discord.Thread
    game_name: str
    message: discord.Message | None
    task: TaskWrapper | None
    has_initial: bool
    was_scheduled: bool | None
    base_mention: str | None
    check_delta: datetime.timedelta | None
    scheduled_event: discord.ScheduledEvent | None
    cancellable: bool

    def __init__(
        self,
        channel: discord.TextChannel | discord.Thread,
        author: discord.Member,
        game_name: str,
    ):
        """
        Creates a new Game, to be started with start().
        """
        print_debug("Created new game")
        self.game_name = game_name

        self.reset()

        self.author = author
        self.channel = channel

        self.task = None

        self.has_initial = False
        self.was_scheduled = None

        self.check_delta = MIN_CHECK_DELTA

        self.scheduled_event = None

        self.cancellable = False

        self.guild_handler = client.guild_handlers[channel.guild.id]

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
            "check_delta": self.check_delta.total_seconds(),
            "scheduled_event": (
                self.scheduled_event.id if self.scheduled_event else None
            ),
        }
        set_value("saved", data, table=self.guild_handler.backup_table)

    def clear_backup(self):
        print_debug("Deleting backup table")
        del_value("saved", table=self.guild_handler.backup_table)
        self.cancellable = False

    def reset(self):
        """
        Resets the game to require new confirmation, for starting a Game Check.
        """
        print_debug("Resetted game")
        self.group_buckets = {}

        game_min = get_game_data(self.game_name, "min", BUCKET_MIN)
        game_max = get_game_data(self.game_name, "max", game_min)

        bucket_range_max = game_max + 1
        for bucket in range(game_min, bucket_range_max):
            self.group_buckets[bucket] = set()
        self.gamer_buckets = {}
        self.message = None
        self.base_mention = None

    async def start(self, future: datetime.datetime, mention: str = None):
        """
        Starts the game.
        """
        print_debug("Started game")
        self.update_future(future)
        await self.initialize(mention=mention)
        self.save()

    async def add_initials(self, author: discord.Member, bucket: int):
        await self.add_gamer(author, bucket, skip_exist=True)
        if not self.is_checking:
            game_marks = self.guild_handler.current_marks[self.game_name]
            old_marks = []
            for gamer, params in game_marks.items():
                dtrange, min_bucket = params
                # TODO: guild now
                if dtrange.end_datetime < client.now:
                    old_marks.append(gamer)
                elif self.future in dtrange:
                    if gamer.id != author.id:
                        await self.add_gamer(gamer, min_bucket, skip_exist=True)
            for gamer in old_marks:
                remove_mark(self.game_name, gamer)

    async def change_time(self, future: datetime.datetime):
        """
        Changes the time the game finishes.
        """
        if self.is_checking:
            return
        self.cancel_task(reason="Changing time")
        await self.update_timestamp(future)
        self.start_countdown()

    def update_future(self, future: datetime.datetime):
        """
        Sets the time the game finishes.
        """
        self.future = future
        self.timestamp = generate_timestamp(future)

        countdown = max(MIN_CHECK_COUNTDOWN, self.get_delta_seconds())
        self.is_checking = countdown <= MAX_CHECK_COUNTDOWN
        if not self.is_checking:
            mapped = interp(
                countdown,
                (MIN_CHECK_SCALING, MAX_CHECK_SCALING),
                (MIN_CHECK_COUNTDOWN, MAX_CHECK_COUNTDOWN),
            )
            clamped = max(MIN_CHECK_COUNTDOWN, min(MAX_CHECK_COUNTDOWN, mapped))
            self.check_delta = datetime.timedelta(seconds=clamped)
        if self.was_scheduled is None:
            self.was_scheduled = not self.is_checking

    async def initialize(self, mention: str = None):
        """
        Starts/schedules the game check.
        """
        print_debug("Initialized game")
        if self.base_mention is None:
            self.base_mention = (
                f"<@&{self.guild_handler.get_server_game_data(self.game_name, 'role')}>"
                if mention is None
                else mention
            )
        name = self.author.display_name
        relative_time = print_timestamp(self.timestamp, "R")
        if self.is_checking:
            msg = f"{self.base_mention} {name} requested a {self.guild_handler.keyword_title} Check. (expires {relative_time})"
        else:
            short_time = print_timestamp(self.timestamp, "t")
            guild = self.channel.guild
            game_display = get_game_data(self.game_name, "display", self.game_name)
            if not get_value(
                "skip_events", default=False, table=self.guild_handler.server_settings
            ):
                self.scheduled_event = await guild.create_scheduled_event(
                    name=f"{game_display} {self.guild_handler.keyword_title}",
                    start_time=self.future,
                    channel=guild.get_channel(
                        self.guild_handler.get_server_game_data(
                            self.game_name, "voice", None
                        )
                    ),
                    privacy_level=discord.PrivacyLevel.guild_only,
                    entity_type=discord.EntityType.voice,
                    reason=f"Starting {self.guild_handler.keyword} for {name}",
                )
            msg = f"{self.base_mention} {name} scheduled a {self.guild_handler.keyword} at {short_time} ({relative_time})."
        if self.message:
            await self.update_message(msg)
        else:
            self.message = await self.channel.send(msg)
        self.start_countdown()

    def get_delta(self) -> datetime.timedelta:
        """
        Gets the timedelta until the game finishes.
        """
        # TODO: guild now
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

    async def add_gamer(
        self, gamer: discord.Member, min_bucket: int, skip_exist: bool = False
    ):
        """
        Sets a gamer to the specified buckets.
        """
        min_bucket, max_bucket = get_bucket_bounds(min_bucket, self.game_name)
        bucket_range_max = max_bucket + 1

        # remove them first if this is an update
        current_min_bucket = self.gamer_buckets.get(gamer)
        if current_min_bucket:
            if skip_exist:
                return
            if current_min_bucket != min_bucket:
                await self.remove_gamer(gamer, notify=False)
            else:
                return

        # if this is checking, then check for max before we add
        if self.is_checking:
            overfill = get_game_data(self.game_name, "overfill", 0)
            cap = max_bucket + overfill
            size = len(self.gamer_buckets)
            if size >= cap:
                await self.channel.send(
                    f"{self.guild_handler.keyword} is full. {gamer.display_name} cannot join."
                )
                return

        # add them from that bucket and beyond
        self.gamer_buckets[gamer] = min_bucket
        for bucket in range(min_bucket, bucket_range_max):
            self.group_buckets[bucket].add(gamer)

        if self.has_initial:
            name = gamer.display_name
            size_c = len(self.gamer_buckets)
            size = f"**({size_c}/{max_bucket})**"
            with_str = get_bucket_str(
                min_bucket,
                f"{self.guild_handler.keyword}{self.guild_handler.keyword_subject_suffix}",
            )
            if self.is_checking:
                # we're adding a gamer to overfill
                if size_c > max_bucket:
                    with_str += " (overfill)"
                msg = (
                    f"{name} is ready to {self.guild_handler.keyword}{with_str}. {size}"
                )
                countdown = self.get_delta_seconds()
                if 5 < countdown < MIN_CHECK_COUNTDOWN:
                    print_debug("Refreshing countdown from add_gamer")
                    missing_countdown = datetime.timedelta(
                        seconds=MIN_CHECK_COUNTDOWN - self.get_delta_seconds(),
                    )
                    await self.refresh(self.future + missing_countdown)
            else:
                print_debug("Adding gamer")
                short_time = print_timestamp(self.timestamp, "t")
                relative_time = print_timestamp(self.timestamp, "R")
                msg = f"{name} can {self.guild_handler.keyword} at {short_time} ({relative_time}){with_str}. {size}"
            await self.channel.send(msg)
        else:
            self.has_initial = True

        self.save()

    async def remove_gamer(self, gamer: discord.Member, notify: bool = True):
        """
        Removes a gamer from all buckets.
        """
        if gamer not in self.gamer_buckets:
            return
        # pop off the gamer lookup
        min_bucket = self.gamer_buckets.pop(gamer)

        game_min = get_game_data(self.game_name, "min", BUCKET_MIN)
        game_max = get_game_data(self.game_name, "max", game_min)
        bucket_range_max = game_max + 1

        # remove from all groups
        for bucket in range(min_bucket, bucket_range_max):
            self.group_buckets[bucket].remove(gamer)

        if notify:
            await self.channel.send(
                f"{gamer.display_name} left the {self.guild_handler.keyword}."
            )

        self.save()

    def start_countdown(self):
        """
        Directly starts the asyncio countdown task.
        """
        print_debug("Starting countdown")
        self.task = create_task(self.countdown(), name="Countdown")
        self.cancellable = True

    async def refresh(self, future: datetime.datetime):
        """
        Refreshes the game with a new countdown.
        """
        print_debug("Refreshing")
        self.cancel_task(reason="Refreshing")
        await self.start(future)

    async def countdown(self):
        """
        Sleeps for the countdown time, and then finishes the game.
        """
        print_debug("Counting down")
        delta = self.get_delta_seconds()
        # if our delta is less than 16 milliseconds, then we don't expect scheduling accuracy and thus don't sleep
        if delta >= 0.016:
            await asyncio.sleep(self.get_delta_seconds())
        await self.finish()

    async def finish_inner(self) -> bool:
        """
        Either finishes the game check, or starts one if it is scheduled.
        """
        gamers = self.get_gamers()
        num_gamers = len(gamers)
        print_debug("Finishing", gamers, self.game_name)
        game_min = get_game_data(self.game_name, "min", BUCKET_MIN)
        has_enough = num_gamers >= game_min
        success = has_enough or not self.is_checking
        if success:
            # print out the message
            mention = " ".join([gamer.mention for gamer in gamers])
            if self.is_checking:
                self.cancellable = False
                print_debug("Finishing check")
                # finish the game
                max_gamers = get_game_data(self.game_name, "max", game_min)
                await self.channel.send(
                    f"{mention} {self.guild_handler.keyword_title} Check complete. **{num_gamers}/{max_gamers}** players ready to {self.guild_handler.keyword}.",
                )
                # we had a game
                set_value("no_gamers_consecutive", 0, table=self.guild_handler.guild_db)
                # track dota matches
                if self.game_name == "dota" and not self.guild_handler.current_match:
                    account_ids = set()
                    for gamer in gamers:
                        player = client.players_table.get(Player.id == gamer.id)
                        if player:
                            account_ids.add(player["steam"])
                    if account_ids:
                        self.guild_handler.current_match = DotaMatch(
                            account_ids=account_ids,
                            gamers=gamers,
                            channel=self.channel,
                        )
            else:
                if not has_enough:
                    # only do a last call if it was a long term scheduling
                    if self.check_delta > MAX_CHECK_DELTA and get_value(
                        "is_last_call_enabled", table=self.guild_handler.settings_table
                    ):
                        mention = self.base_mention
                        mention += f" No {self.guild_handler.keyword}{self.guild_handler.keyword_subject_suffix} scheduled for the {self.guild_handler.keyword}. Last call!"
                    else:
                        success = False
                if success:
                    print_debug("Starting check")
                    # TODO: guild now
                    # start the game up again
                    client.now = (
                        self.future
                    )  # this is the time we should have landed on
                    self.reset()
                    create_task(
                        self.start(client.now + self.check_delta, mention=mention)
                    )
                    return False
        if not success:
            self.cancellable = False
            print_debug("No gamers")
            no_gamers = update_value(
                increment, "no_gamers", default=0, table=self.guild_handler.guild_db
            )
            no_gamers_consecutive = update_value(
                increment,
                "no_gamers_consecutive",
                default=0,
                table=self.guild_handler.guild_db,
            )
            await self.channel.send(
                f"No {self.guild_handler.keyword}{self.guild_handler.keyword_subject_suffix} found for the {self.guild_handler.keyword}. This server has gone {no_gamers} {self.guild_handler.keyword}s without a {self.guild_handler.keyword}. ({no_gamers_consecutive} in a row).",
            )
            if self.guild_handler.extra_failure_message:
                await self.channel.send(self.guild_handler.extra_failure_message)
            if self.scheduled_event:
                await self.scheduled_event.cancel(
                    reason=f"No {self.guild_handler.keyword}{self.guild_handler.keyword_subject_suffix} found, cancelling the {self.guild_handler.keyword}"
                )
        if self.is_checking:
            # make it past tense
            await self.replace_message("expires", "expired")
        return True

    async def finish(self):
        should_end = True
        try:
            should_end = await self.finish_inner()
        except Exception:
            print("Failed to finish match", traceback.format_exc())
        finally:
            if should_end:
                self.guild_handler.current_game = None
                self.clear_backup()

    def cancel_task(self, reason: str = "Cancelled"):
        """
        Directly cancels the asyncio countdown task.
        """
        print_debug("Cancelling task: ", reason)
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
        if not self.cancellable:
            return
        self.cancellable = False
        print_debug("Cancelling")
        self.cancel_task()
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
            await self.replace_message("expires", "cancelled")
        if self.scheduled_event:
            await self.scheduled_event.cancel(
                reason=f"Cancelling {self.guild_handler.keyword}"
            )
        self.guild_handler.current_game = None
        self.clear_backup()
        await self.channel.send(f"{self.guild_handler.keyword_title} cancelled.")

    async def advance(self, now: datetime.datetime):
        """
        Skips ahead to finish the game now.
        """
        print_debug("Advancing")
        self.cancel_task(reason="Advancing")
        # if checking, then we need to update things that are printed in the message
        if self.is_checking:
            await self.update_timestamp(now)
        else:
            # otherwise, we need to just update the future used for the next check
            old_check_delta = self.check_delta
            self.update_future(now)
            # restore old properties for the finish
            self.check_delta = old_check_delta
            self.is_checking = False
        await self.finish()

    def get_gamers(self) -> set[discord.Member]:
        """
        Gets available gamers, according to the largest satisfied bucket.
        """
        bucket = set()

        game_min = get_game_data(self.game_name, "min", BUCKET_MIN)
        game_max = get_game_data(self.game_name, "max", game_min)
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
    start: datetime.datetime | None
    remove_mark: bool
    bucket: int
    game: str

    def __init__(self):
        """
        Initializes a blank/default options.
        """
        self.future = None
        self.start = None
        self.remove_mark = False
        self.game = DEFAULT_GAME
        self.bucket = BUCKET_MIN


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
MAX_HUMANIZE_PRECISION = 0.1


def convert_humanize_decimal(quantity: float, unit: str) -> str:
    frac, whole = math.modf(quantity)
    base = f"{int(quantity)} {unit}"
    # if this isn't a significant float, then we can just return it back
    if abs(frac) < MAX_HUMANIZE_PRECISION:
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
    except AssertionError:
        return None


# to is a separator, rest are control words
NON_TIME_WORDS = {"to", "if", "at", "in", "on"}


def process_in(args: list[str]) -> datetime.datetime | None:
    # process now
    if args[0] == "now":
        args.pop(0)
        return utcnow()
    # TODO: guild now
    arw = arrow.get(client.now)
    date_string = "in"
    end = 0
    new_start = 0
    last = len(args)
    confirmed_date = None
    # go through until we get a date
    while True:
        word = args[end].lower()
        if word in NON_TIME_WORDS:
            break
        # if it's a shorthand quantity, ex. 1h, 5m, separate them out to normalize for the parser
        if word[0] in NUMERIC:
            # TODO: make this detection more generic so people can combine a shorthand with other quantities
            # if it's just a raw quanity, add a shorthand unit so it can be handled
            if word[len(word) - 1] in NUMERIC and (
                end + 1 >= last
                or args[end + 1].lower() in NON_TIME_WORDS
                or args[end + 1].lower() == "and"
            ):
                word += "m"
            # now check if there's a shorthand unit at the end of the quantity
            if word[len(word) - 1] not in NUMERIC:
                i = 0
                for idx, c in enumerate(word):
                    i = idx
                    if c not in NUMERIC:
                        break
                args.insert(end + 1, word[i:])
                word = word[:i]
                last = len(args)
        if last > end + 1:
            noun = args[end + 1].lower()
            # replace shorthand with longform unit, as required by the parser
            longform_noun = HUMANIZE_SHORTHAND.get(noun)
            if longform_noun is not None:
                noun = longform_noun
                args[end + 1] = noun
            # using a quantity must require you to use plural units.
            # if you don't for 1, you must use "a" or "an"
            if word == "1":
                if not noun.endswith("s"):
                    word = "an" if noun in HUMANIZE_VOWEL_WORDS else "a"
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
        except OverflowError:
            # overflowed, get largest date (9999-12-31T23:59:59.999969)
            attempt_date = arrow.get(253402300799.9999847)
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
    return confirmed_date


MONTHS = {
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "November",
    "december",
}
NEXT_PERIOD = datetime.timedelta(hours=12)


def process_at(args: list[str], gamer: discord.Member) -> datetime.datetime | None:
    # process now
    if args[0] == "now":
        args.pop(0)
        return utcnow()
    date_string = ""
    last_word = ""
    end = 0
    new_start = 0
    last = len(args)
    confirmed_date = None
    the_timezone = get_value(str(gamer.id), default=None, table=client.timezone_table)
    guild_handler = client.guild_handlers[gamer.guild.id]
    if the_timezone:
        the_timezone = ZoneInfo(the_timezone)
    if not the_timezone:
        the_timezone = guild_handler.local_tzinfo
    # TODO: guild now
    # guild_handler = client.guild_handlers[gamer.guild.id]
    # client_now = guild_handler.now
    client_now = client.now
    local_now = client_now.astimezone(the_timezone)
    # go through until we get a date
    while True:
        arg = args[end]
        word = arg.lower()
        if word in NON_TIME_WORDS:
            break
        # combine if space
        if last > end + 1:
            period = args[end + 1].lower()
            if (
                period == "pm"
                or period == "am"
                or period == "p"
                or period == "a"
                or period == "z"
            ):
                word += period
                end += 1
        is_tz_str = False
        if word[0] in NUMERIC:
            # have to take leading zero off
            if word[0] == "0":
                time_word = word[1:]
                has_leading_zero = True
            else:
                time_word = word
                has_leading_zero = False
            numeric = get_int(time_word, None)
            # whole number, it could be part of a date, not a time
            is_time = True
            is_24_hour = False
            if numeric is not None:
                # Can't have January 12am or 24:00
                if last_word in MONTHS or numeric > 23:
                    is_time = False
                elif numeric > 12:
                    is_24_hour = True
            if is_time:
                if is_24_hour:
                    just_time = word
                else:
                    if word.endswith("z"):
                        is_24_hour = True
                        word = word[:-1]
                    is_24_hour = is_24_hour or has_leading_zero
                    if word.endswith(("p", "a")):
                        # use pm and am, not p or a
                        word += "m"
                        is_24_hour = False
                    elif not word.endswith("pm") and not word.endswith("am"):
                        if not is_24_hour:
                            # if there's no period at all, just autodetect based on current (if not 24 hour)
                            # see the end of the function to see handling if we get it wrong
                            word += "am" if local_now.hour < 12 else "pm"
                    else:
                        is_24_hour = False
                    if is_24_hour:
                        just_time = word
                    else:
                        just_time = word[:-2]
                # if it's just a single int representing the hour, normalize it into a full time
                if get_int(just_time, None) is not None:
                    if is_24_hour:
                        word = just_time + ":00"
                    else:
                        word = just_time + ":00" + word[-2:]
        elif arg in get_timezones_lower():
            is_tz_str = True
            word = arg
        if is_tz_str:
            new_timezone = ZoneInfo(word)
            if new_timezone != the_timezone:
                new_now = client_now.astimezone(the_timezone)
                new_timezone_am = new_now.hour < 12
                local_timezone_am = local_now.hour < 12
                if new_timezone_am != local_timezone_am:
                    if new_timezone_am:
                        date_string.replace("pm", "am")
                    else:
                        date_string.replace("am", "pm")
                local_now = new_now
                the_timezone = new_timezone
        else:
            date_string += " " + word if date_string else word
        if the_timezone != TIMESTAMP_TIMEZONE:
            settings = {
                "TIMEZONE": the_timezone.tzname(local_now),
                "TO_TIMEZONE": "UTC",
            }
            # if UTC time is in the next day, we need to act like we're getting a time in the past
            # because our local time zone is in the previous day, likewise with future
            if (
                client_now.day > local_now.day
                or client_now.month > local_now.month
                or client_now.year > local_now.year
            ):
                settings["PREFER_DATES_FROM"] = "past"
            elif (
                client_now.day < local_now.day
                or client_now.month < local_now.month
                or client_now.year < local_now.year
            ):
                settings["PREFER_DATES_FROM"] = "future"
        else:
            settings = None
        attempt_date = dateparser.parse(
            date_string,
            languages=["en"],
            settings=settings,
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
        last_word = word
    # consume our peeked inputs to the date
    del args[:new_start]
    if confirmed_date and confirmed_date.tzinfo is None:
        confirmed_date = confirmed_date.replace(tzinfo=TIMESTAMP_TIMEZONE)
    # if we got it wrong, advance from AM -> PM (or PM -> AM)
    if confirmed_date and client_now > confirmed_date:
        confirmed_date += NEXT_PERIOD
        # if we got it wrong again, advance from AM -> PM (or PM -> AM)
        # this happens for "today's am" when you're currently in "today's pm" and want "tomorrow's am".
        if confirmed_date and client_now > confirmed_date:
            confirmed_date += NEXT_PERIOD
    return confirmed_date


TIME_CONTROLS = {"in", "at"}


def process_time_control(
    control: str, args: list[str], gamer: discord.Member
) -> tuple[datetime.datetime | None, str]:
    if control not in TIME_CONTROLS:
        args.insert(0, control)
        control = "in"
    attempt1 = None
    saved_args = args.copy()
    if control == "at":
        attempt1 = process_at(args, gamer)
    elif control == "in":
        attempt1 = process_in(args)
    no_time_args_left = not args or args[0] in NON_TIME_WORDS
    if attempt1 and no_time_args_left:
        return attempt1, control
    first_args = None
    if attempt1:
        first_args = args.copy()
    attempted = None
    args.clear()
    args.extend(saved_args)
    attempt2 = None
    if control == "at":
        attempt2 = process_in(args)
        attempted = "in"
    elif control == "in":
        attempt2 = process_at(args, gamer)
        attempted = "at"
    # if we didn't find an attempt1, then it's auto return for attempt2
    more_consumed = not attempt1
    if attempt1:
        # see if we consume more words for attempt2 (more fitting)
        # prefer "in" if there is equal consumption
        # "at" has to consume strictly more words to be preferred
        first_args_left = len(first_args)
        second_args_left = len(args)
        if attempted == "in":
            more_consumed = second_args_left <= first_args_left
        elif attempted == "at":
            more_consumed = second_args_left < first_args_left
    if more_consumed:
        return attempt2, attempted
    return attempt1, control


CURRENT_GAME_ARGS = {"cancel", "now", "leave", "but"}
START_GAME_ARGS = {"at", "in", "on"}


def remove_mark(game: str, gamer: discord.Member):
    guild_handler = client.guild_handlers[gamer.guild.id]
    if guild_handler.current_marks[game].pop(gamer, None):

        def clean_mark(old):
            old[game].pop(str(gamer.id), None)
            return old

        update_value(clean_mark, "marks", table=guild_handler.backup_table)


async def consume_args(
    args: list[str],
    gamer: discord.Member,
    message: discord.Message,
    options: GameOptions,
) -> GameOptions | None:
    global handled
    """
    Handles building options from command arguments by parsing args.

    Returning None means we don't interact with the game.
    """
    control = args.pop(0).lower()
    created_at = message.created_at
    channel = get_channel(message.thread or message.channel)
    guild_handler = client.guild_handlers[message.guild.id]
    keyword = guild_handler.keyword
    # if there's a game, try to control it
    if guild_handler.current_game:
        # sometimes users accidentally try to start a game when it's running
        if control in START_GAME_ARGS:
            await channel.send(
                f"Cannot start a {keyword} when there is already one currently active.",
            )
            return None
        if control == "but":
            await guild_handler.current_game.cancel(created_at)
            return options
        if control == "cancel":
            await guild_handler.current_game.cancel(created_at)
            return None
        if control == "now":
            await guild_handler.current_game.advance(created_at)
            return None
        if control == "leave":
            await guild_handler.current_game.remove_gamer(gamer)
            return None
    else:
        # sometimes users accidentally try to control a game when it doesn't exist
        if control in CURRENT_GAME_ARGS:
            await channel.send(
                f"Cannot control a {keyword} when there is none currently active.",
            )
            return None
        if control == "for":
            if not args:
                await channel.send(
                    f"{keyword} for <game>\nSet the name of the game to schedule.\n**Available Games:** {', '.join(GAMES)}\n\n**Example:** {keyword} for {GAMES[0]}",
                )
                return None
            game = args.pop(0).lower()
            if game in GAMES:
                options.game = game
                return options

    if control == "help":
        await channel.send(
            """Just say `game` to schedule a game, and say `game` to join up for a scheduled one. Only supports scheduling one at a time.

When starting it, you can also say some parameters:

* `at <time>`: set a specific date/time to check at. ex: `at 9`, `at 9pm`, `at 9pm PST`, `at 9:30`, etc.
* `in <time>`: set a length of time to check at. ex: `in 1h`, `in 1.5h`, `in 5m`, `in 5 mins`, `in 5 hours, 20 minutes, 30 seconds`, etc.
* `for <game>`: set the name of the game to schedule.

While it's started, anyone can say:

* `cancel`: cancels the scheduled game.
* `now`: skips ahead to finish the schedule now.
* `leave`: leaves a scheduled game you joined.
* `but`: restart the game with new starting parameters

Either when starting it, or while it's started, you can say:

* `if <number>`: set a minimum number of players you'd like to play with. You can say this again to change it. ex: `if 3`, `if 5`, etc.

In any case, anyone can say:

* `status [user]`: gives the status of a game currently running, or of any specified Discord user's game. Currently only supports Dota 2. 
* `register <steam account>`: associates a given Steam user to your Discord account for match handling.
* `unregister`: unassociates your Steam ID from your Discord account.
* `option set <key> [value]`: sets an option, or removes it if value not specified (admins only)
* `option get <key>`: gets an option (admins only)
* `mark [in/at] <time> to [in/at] <time>`: marks yourself as available for a scheduled game at a given time"""
        )
        return None
    # future date handling
    if options.future is None:
        if control == "at":
            if not args:
                await channel.send(
                    f"{keyword} at <time>\nSet a specific date/time to check at.\n\n**Example:** {keyword} at 5pm",
                )
                return None
            confirmed_date, _time_control = process_time_control(control, args, gamer)
            if confirmed_date:
                options.future = confirmed_date
            return options
        if control == "in":
            if not args:
                await channel.send(
                    f"{keyword} in <time>\nSet a length of time to check at.\n\n**Example:** {keyword} in 10 minutes",
                )
                return None
            confirmed_date, _time_control = process_time_control(control, args, gamer)
            if confirmed_date:
                options.future = confirmed_date
            return options
    if control == "match":
        if not args:
            await channel.send(
                f"{keyword} match <match id>\nSet the match ID to query.\n\n**Example:** {keyword} match 7208943764"
            )
            return None
        match_id = args.pop(0)
        match_id = get_int(match_id, default=None)
        if match_id == None:
            await channel.send(f"Match ID must be a number.")
            return None
        my_match = DotaMatch(set(), set(), channel, should_check=False, serialize=False)
        await my_match.post_match(await DotaAPI.get_parsed_match(match_id))
        return None
    if control == "register":
        if not args:
            await channel.send(
                f"{keyword} register <steam profile>\nRegisters your Steam account for game tracking.\n\n**Example:** {keyword} register username\n**Example:** {keyword} register https://steamcommunity.com/id/username\n**Example:** {keyword} register https://steamcommunity.com/profiles/12345678901234567",
            )
            return None
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
            try:
                resp = client.steamapi.ISteamUser.ResolveVanityURL(vanityurl=vanity)
                vanity = resp.get("response", {}).get("steamid")
                if vanity:
                    steam_id = try_steam_id(vanity)
            except Exception:
                # this is ok, vanity URL was incorrect
                pass
        if not steam_id:
            await channel.send("Steam ID not found.")
            return None
        client.players_table.upsert(
            {"id": gamer.id, "steam": steam_id.as_32},
            Player.id == gamer.id,
        )
        await channel.send("Steam ID linked. Matches will now be listed.")
        return None
    if control == "unregister"
      client.players_table.remove(Player.id == gamer.id)
      await channel.send("Steam ID unlinked. Matches will no longer be listed.")
      return None
    if control == "timezone":
        if args:
            my_tz = args.pop(0)
            if my_tz.lower() == "reset":
                del_value(str(gamer.id), table=client.timezone_table)
                await channel.send("Timezone reset.")
                return None
            if my_tz in get_timezones_lower():
                set_value(str(gamer.id), my_tz, table=client.timezone_table)
                icu_tz = TimeZone.createTimeZone(my_tz)
                tz_name = icu_tz.getDisplayName(False, TimeZone.LONG)
                await channel.send(f"Timezone set to {my_tz} ({tz_name}).")
                return None
            else:
                b_timezones = io.BytesIO(get_timezone_list_bytes())
                timezone_list = discord.File(b_timezones, "timezones.txt")
                await channel.send(
                    "Timezone not found. Please use an Olson/IANA timezone. Available timezones are attached.",
                    file=timezone_list,
                )
                return None
        await channel.send(
            f"{keyword} timezone <timezone>|reset\nSets your timezone preference for `{keyword} at`.\n\n**Example:** {keyword} timezone US/Eastern\n**Example:** {keyword} timezone EST",
        )
        return None
    if control == "option":
        if not gamer.guild_permissions.administrator:
            await channel.send("Not permitted to set/get options.")
            return None
        if not args:
            await channel.send(
                f"{keyword} option <set|get> <option> [value]\nSets/gets an option.\n\n**Example:** {keyword} option set channel_id 123456789",
            )
            return None
        option_mode = args.pop(0).lower()
        option = args.pop(0).lower()
        if option_mode == "set":
            if len(args):
                new_value = args.pop(0)
                if "id" in option:
                    tmp = new_value
                    new_value = get_int(new_value, default=None)
                    if new_value is None:
                        new_value = tmp
                elif "is" in option or "should" in option:
                    new_value = new_value.lower() in ("true", "1", "yes", "on")

                set_value(option, new_value, table=guild_handler.settings_table)
                await channel.send(f"{option}={new_value}")
            else:
                del_value(option, table=guild_handler.settings_table)
                await channel.send(f"{option}=null")
        else:
            await channel.send(
                f"{option}={get_value(option, table=guild_handler.settings_table)}",
            )
        return None
    # if buckets
    if control == "if":
        if not args:
            await channel.send(
                f"{keyword} if <number>\nSet a minimum number of players you'd like to play with. You can say this again to change it.\n\n**Example:** {keyword} if 3",
            )
            return None
        options.bucket = get_int(args.pop(0), BUCKET_MIN)
        return options
    # mark your scheduled availability
    if control == "mark":
        if args:
            time_control = args.pop(0).lower()
            if (
                time_control == "rm"
                or time_control == "remove"
                or time_control == "clear"
                or time_control == "delete"
                or time_control == "del"
            ):
                options.remove_mark = True
                return options
            start_datetime, start_time_control = process_time_control(
                time_control, args, gamer
            )
            if start_datetime and args:
                sep = args.pop(0).lower()
                end_datetime = None
                if sep == "to" and args:
                    new_arg = args[0].lower()
                    if new_arg in TIME_CONTROLS:
                        time_control = new_arg
                        args.pop(0)
                    else:
                        time_control = start_time_control
                    if args:
                        end_datetime, _end_time_control = process_time_control(
                            time_control, args, gamer
                        )
                if end_datetime:
                    options.start = start_datetime
                    options.future = end_datetime
                    return options
        await channel.send(
            f"{keyword} mark [in/at] <time> to [in/at] <time>\nMarks yourself as available for a scheduled game at a given time.\ndank mark remove|rm\nRemoves any marked availability.\n\n**Example:** {keyword} mark in 1 hour to 2 hours\n**Example:** {keyword} mark 3pm to 6pm",
        )
        return None

    if control == "status":
        if not client.steamapi:
            await channel.send("Steam API is not configured.")
            return None
        if not client.steamclient:
            await channel.send("Steam GC is not configured.")
            return None
        if handled == 0:
            await channel.send("Already querying a match, please try again later.")
            return None
        match = guild_handler.current_match
        member = None
        if args:
            match = None
            member_arg = args.pop(0)
            try:
                member = await DiscordUtil.convert_user_arg(message, member_arg)
            except MemberNotFound as e:
                await channel.send(str(e))
                return None
            player = client.players_table.get(Player.id == member.id)
            if player:
                account_id = player["steam"]
                match = DotaMatch(
                    {account_id},
                    {member},
                    channel,
                    should_check=False,
                    serialize=False,
                )
            if not match:
                await channel.send(f"No live match found for {member.display_name}.")
                return None
        elif not match:
            await channel.send(
                f"There is no active {keyword}. Please specify a player.\n\n{keyword} status <Discord user>\n\n**Example:** {keyword} status Nickname\n**Example:** {keyword} status @Name"
            )
            return None
        if not member:
            # if we're in this match, just use us
            if gamer.id in match.gamer_ids:
                member = gamer
            else:
                # otherwise, find the assigned account ID's Discord
                steam_id = match.account_id
                player = client.players_table.get(Player.steam == steam_id)
                if player:
                    member = gamer.guild.get_member(player["id"])
        await match.query_realtime(channel, member)
        return None

    # if we didn't find the control, it's an invalid command
    arguments = keyword + " " + control
    if args:
        arguments += " " + " ".join(args)
    await channel.send(
        f'Unrecognized input "{arguments}", please check the usage of the command.',
    )
    return None


FUZZ_THRESHOLD = 75


def get_game_data(game: str, data: str, default: Any = None) -> Any:
    """
    Gets the game data from settings
    """
    val = GAME_DATA[game].get(data, default)
    if val is None:
        raise ValueError(f"Game data {data} not found for {game}.")
    return val


def is_game_command(content: str, keyword: str) -> bool:
    """
    Checks if the message represents a game "command"
    """
    # if it is empty, fails
    if not content:
        return False
    # if it is the word, passes
    word = content.split(maxsplit=1)[0]
    if word.startswith(keyword):
        return True
    # has to start with the letter
    if not word.startswith(keyword[0]):
        return False
    # fuzz it
    ratio = fuzz.ratio(word, keyword)
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
    intents.guild_scheduled_events = True
    intents.members = True
    intents.message_content = True
    intents.voice_states = True

    # limit the mentions to the ones required
    mentions = AllowedMentions.none()
    mentions.users = True
    mentions.roles = True

    opendota_api_key = os.getenv("GAME_BOT_OPENDOTA")
    opendota_api_key = QueryApiKey(opendota_api_key) if opendota_api_key else None

    # create opendota API HTTP client
    async with (
        httpx.AsyncClient(
            base_url="https://api.opendota.com/api",
            timeout=10.0,
            http2=True,
            auth=opendota_api_key,
        ) as opendota,
        httpx.AsyncClient(timeout=10.0, http2=True) as http_client,
    ):
        # create our client, limit messages to what we need to keep track of
        client = GameClient(
            opendota=opendota,
            http_client=http_client,
            debug=debug,
            no_2fa=no_2fa,
            max_messages=500,
            command_prefix=KEYWORD,
            intents=intents,
            allowed_mentions=mentions,
        )

        # cache constants
        await DotaAPI.query_constants()

        # start the client
        async with client as _client:
            try:
                await _client.start(os.environ["GAME_BOT_TOKEN"])
            except:
                pass


if os.name == "nt":
    # On Windows, the selector event loop is required for aiodns and avoiding exceptions on exit
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

with open("settings.json", "rb") as f:
    config = orjson.loads(f.read())

    LOCAL_TIMEZONE = config.get("local_timezone", "US/Eastern")

    KEYWORD = config.get("keyword", "game")

    EXTRA_FAILURE_MESSAGE = config.get("failure_message", None)
    EXTRA_WIN_MESSAGE = config.get("win_message", None)
    EXTRA_LOSS_MESSAGE = config.get("loss_message", None)

    MUTUAL_STEAM_ID = config.get("mutual_steam_id", None)

    GAME_DATA = config["games"]
    GAMES = list(GAME_DATA.keys())
    DEFAULT_GAME = GAMES[0]


def start_bot(*, debug: bool, no_2fa: bool) -> None:
    discord.utils.setup_logging(level=logging.DEBUG if debug else logging.INFO)
    asyncio.run(main(debug, no_2fa))


PROFILING = os.environ.get("GAME_BOT_PROFILING", False)
USE_2FA = os.environ.get("GAME_BOT_2FA", False)

if __name__ == "__main__":
    # TODO: arg parse
    if PROFILING:
        import yappi

        yappi.set_context_backend("greenlet")
        yappi.set_clock_type("cpu")
        yappi.start(builtins=True)

    no_2fa = not USE_2FA

    if PROFILING:
        with yappi.run():
            start_bot(debug=DEBUGGING > 1, no_2fa=no_2fa)
        yappi.get_func_stats().print_all()
    else:
        start_bot(debug=DEBUGGING > 1, no_2fa=no_2fa)
