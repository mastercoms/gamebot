import asyncio
import discord
import datetime
import dateparser
import arrow
from fuzzywuzzy import fuzz, process
import os

client = discord.Client()

danking = False
dank_check_countdown = 31
default_delta = datetime.timedelta(seconds=dank_check_countdown)
current_delta = default_delta
tz = datetime.timezone(-datetime.timedelta(hours=4), name="ET")
current_datetime = datetime.datetime.now(tz=tz)
refresh_dank_countdown = True
dankers = []
cancel_dank = False

resolution = datetime.timedelta(seconds=1)

game_roles = {
    'dota': '261137719579770882',
    'fortnite': '393157155651452939'
}

games = list(game_roles.keys())

default_game = 'dota'
current_game = 'dota'


def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


@client.event
async def on_message(message):
    if message.author == client.user or message.author.bot:
        return

    global danking
    global dankers
    global dank_check_countdown
    global refresh_dank_countdown
    global current_datetime
    global current_delta
    global current_game
    global resolution
    global tz
    global cancel_dank

    message.content = message.content.lower()

    if message.content.startswith("d") and fuzz.partial_ratio(message.content,
                                                              "dank") > 70:
        if danking:
            if message.content.split(" ")[-1] == "cancel":
                dank_check_countdown = 0
                cancel_dank = True
                return
            if message.content.split(" ")[-1] == "now":
                dank_check_countdown = 0
                return
            await add_to_dank(message.author, message.channel)
        else:
            game = ''
            delta = None
            attempt_date = None
            dateparse_string = ''
            now = datetime.datetime.now(tz=tz)
            for parameter in message.content.split(" ")[1:]:
                attempt_game = process.extractOne(parameter, games)
                if not game and attempt_game[1] > 70:
                    game = attempt_game[0]
                elif dateparse_string is not None:
                    dateparse_string += parameter if not dateparse_string else " " + parameter
                    attempt_date = dateparser.parse(date_string=dateparse_string, languages=['en'],
                                                    region='US',
                                                    settings={'TIMEZONE': 'America/New_York',
                                                              'PREFER_DATES_FROM': 'future',
                                                              'RETURN_AS_TIMEZONE_AWARE': True})
                    if attempt_date and len(dateparse_string.split(" ")) >= len(message.content.split(" ")[1:]) - 1\
                            and len(dateparse_string.split(" ")) > 2:
                        if attempt_date > now:
                            dateparse_string = None
                            delta = attempt_date - now
                        else:
                            attempt_date = None
                    else:
                        attempt_date = None
                else:
                    break
            if delta is None:
                delta = default_delta
            current_delta = delta
            dank_check_countdown = int(round(current_delta.total_seconds()))
            if dank_check_countdown < 60:
                refresh_dank_countdown = True
            elif dank_check_countdown > 28800:
                return
            else:
                refresh_dank_countdown = False
            if not game:
                game = 'dota'
            if attempt_date is None:
                attempt_date = now + current_delta
            danking = True
            name = message.author.nick if message.author.nick else message.author.name
            dankers.append(message.author)
            current_game = game
            current_datetime = attempt_date
            if refresh_dank_countdown:
                await message.channel.send(f"<@&{game_roles[current_game]}> {name} requested a Dank Check. (expires in {dank_check_countdown} seconds)")
                asyncio.ensure_future(finish_dank(message.channel))
                return
            attempted_humanize_distance = arrow.get(attempt_date).humanize(other=now, only_distance=True)
            if len(attempted_humanize_distance.split(" ")) < 2 or attempted_humanize_distance == "just now":
                attempted_humanize_distance = dank_check_countdown.__str__() + " seconds"
                msg = f"<@&{game_roles[current_game]}> {name} scheduled a dank in {attempted_humanize_distance} ({current_datetime.strftime('%I:%M %p %Z')})."
            else:
                msg = f"<@&{game_roles[current_game]}> {name} scheduled a dank in about {attempted_humanize_distance} ({current_datetime.strftime('%I:%M %p %Z')})."
            await message.channel.send(msg)
            asyncio.ensure_future(finish_dank(message.channel))


async def add_to_dank(user, channel):
    global dankers
    global dank_check_countdown
    global refresh_dank_countdown
    global current_datetime

    if user not in dankers:
        name = user.nick if user.nick else user.name
        dankers.append(user)
        if refresh_dank_countdown:
            msg = f"{name} is ready to dank. **({len(dankers)}/5)**"
        else:
            msg = f"{name} can dank at {current_datetime.strftime('%I:%M %p %Z')}. **({len(dankers)}/5)**"
        await channel.send(msg)
        if refresh_dank_countdown:
            dank_check_countdown = 31


async def finish_dank(channel):
    global refresh_dank_countdown
    global dank_check_countdown
    global danking
    global dankers
    global cancel_dank

    while dank_check_countdown > 0 and len(dankers) < 5:
        await asyncio.sleep(1)
        dank_check_countdown -= 1

    danking = False

    if cancel_dank:
        dank_check_countdown = 31
        danker = dankers[0]
        danker_name = danker.nick if danker.nick else danker.name
        possess_string = "'" if danker_name.endswith("s") else "'s"
        cancel_dank = False
        await channel.send(f"{danker_name}{possess_string} dank cancelled.")
        return

    mentions_list = ''
    for danker in dankers:
        mentions_list += danker.mention + " "

    if refresh_dank_countdown:
        await channel.send(f"{mentions_list} Dank Check complete. **{len(dankers)}/5** players ready to dank.")
    elif len(dankers) > 1:
        refresh_dank_countdown = True
        dank_check_countdown = 31
        danker = dankers[0]
        danking = True
        danker_name = danker.nick if danker.nick else danker.name
        await channel.send(f"{mentions_list} {danker_name} requested a Dank Check. (expires in {dank_check_countdown} seconds).")
        dankers = []
        asyncio.ensure_future(finish_dank(channel))
    else:
        dank_check_countdown = 31
        danker = dankers[0]
        danker_name = danker.nick if danker.nick else danker.name
        possess_string = "'" if danker_name.endswith("s") else "'s"
        await channel.send(f"No candidates found for {danker_name}{possess_string} dank.")

    dankers = []


client.run(os.environ['DANK_TOKEN'])
