import asyncio
import discord
import datetime
import dateparser
import arrow
from fuzzywuzzy import fuzz, process
import os

client = discord.Client()

DEFAULT_COUNTDOWN = 120

danking = False
dank_check_countdown = DEFAULT_COUNTDOWN
default_delta = datetime.timedelta(seconds=DEFAULT_COUNTDOWN)
current_delta = default_delta
tz = datetime.timezone(-datetime.timedelta(hours=4), name="ET")
current_datetime = datetime.datetime.now(tz=tz)
refresh_dank_countdown = True
no_dankers = 0
dankers = []
cancel_dank = False

game_roles = {
    'dota': '261137719579770882'
}

games = list(game_roles.keys())

DEFAULT_GAME = 'dota'
current_game = DEFAULT_GAME


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
    global tz
    global cancel_dank

    message.content = message.content.lower()

    if message.content.split(" ")[0].startswith("dank"):
        if danking:
            args = message.content.split(" ")
            argl = len(args)
            control = args[1] if argl > 1 else None
            if control == "cancel":
                dank_check_countdown = 0
                cancel_dank = True
                return
            if control == "now":
                dank_check_countdown = 0
                return
            if control == "leave":
                await message.channel.send("You left this dank.")
                dankers.remove(message.author)
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
            dank_check_countdown = max(DEFAULT_COUNTDOWN, int(round(current_delta.total_seconds())))
            if dank_check_countdown < 600:
                refresh_dank_countdown = True
            else:
                refresh_dank_countdown = False
            if not game:
                game = DEFAULT_GAME
            if attempt_date is None:
                attempt_date = now + current_delta
            danking = True
            name = message.author.display_name
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
        name = user.display_name
        dankers.append(user)
        if refresh_dank_countdown:
            msg = f"{name} is ready to dank. **({len(dankers)}/5)**"
        else:
            msg = f"{name} can dank at {current_datetime.strftime('%I:%M %p %Z')}. **({len(dankers)}/5)**"
        await channel.send(msg)


async def finish_dank(channel):
    global refresh_dank_countdown
    global dank_check_countdown
    global danking
    global dankers
    global cancel_dank
    global no_dankers
    
    danker_count = len(dankers)

    while dank_check_countdown > 0 and danker_count < 5:
        await asyncio.sleep(1)
        if cancel_dank:
            break
        elif refresh_dank_countdown and danker_count != len(dankers):
            danker_count = len(dankers)
            dank_check_countdown = max(DEFAULT_COUNTDOWN, dank_check_countdown)
        else:
            danker_count = len(dankers)
            dank_check_countdown -= 1

    danking = False

    if cancel_dank:
        refresh_dank_countdown = True
        dank_check_countdown = DEFAULT_COUNTDOWN
        cancel_dank = False
        if len(dankers) > 0:
            danker = dankers[0]
            danker_name = danker.display_name
            possess_string = "'" if danker_name.endswith("s") else "'s"
            await channel.send(f"{danker_name}{possess_string} dank cancelled.")
        else:
            await channel.send("Dank cancelled.")
        dankers = []
        return

    if len(dankers) > 1:
        mentions_list = " ".join([danker.mention for danker in dankers])
        if refresh_dank_countdown:
            await channel.send(f"{mentions_list} Dank Check complete. **{len(dankers)}/5** players ready to dank.")
        else:
            danker = dankers[0]
            danking = True
            danker_name = danker.display_name
            dank_check_countdown = DEFAULT_COUNTDOWN
            await channel.send(f"{mentions_list} {danker_name} requested a Dank Check. (expires in {dank_check_countdown} seconds).")
            asyncio.ensure_future(finish_dank(channel))
    elif len(dankers) == 1:
        danker = dankers[0]
        danker_name = danker.display_name
        possess_string = "'" if danker_name.endswith("s") else "'s"
        no_dankers += 1
        await channel.send(f"No candidates found for {danker_name}{possess_string} dank. This server has gone {no_dankers} danks without a dank.")
        await channel.send("https://cdn.discordapp.com/attachments/195236615310934016/952745307509227592/cb3.jpg")
    else:
        no_dankers += 1
        await channel.send(f"No candidates found for the dank. This server has gone {no_dankers} danks without a dank.")
        await channel.send("https://cdn.discordapp.com/attachments/195236615310934016/952745307509227592/cb3.jpg")

    dankers = []
    dank_check_countdown = DEFAULT_COUNTDOWN
    refresh_dank_countdown = True


client.run(os.environ['DANK_TOKEN'])
