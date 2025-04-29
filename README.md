# game bot

quick discord bot for pinging people to play games

## running

1. `pipenv install`
2. `GAME_BOT_TOKEN=token pipenv run python bot.py` 

Optional environment variables:

* `GAME_BOT_STEAM_KEY`: [Steam Web API key](https://steamcommunity.com/dev/apikey), used for getting match details at the end of the match and handling mutual friends
* `GAME_BOT_STEAM_USER`: Used to specify Steam user which will be able to query live match status of friends
* `GAME_BOT_STEAM_PASS`: The password for the Steam user
* `GAME_BOT_OPENDOTA`: [OpenDota API key](https://www.opendota.com/api-keys) to use the premium tier rather the free tier of the OpenDota API, for additional match details

## commands

Just say `game` to schedule a game, and say `game` to join up for a scheduled one. Only supports scheduling one at a time.

When starting it, you can also say some arguments:

* `at <time>`: set a specific date/time to check at. ex: `at 9`, `at 9pm`, `at 9pm PST`, `at 9:30`, etc.
* `in <time>`: set a length of time to check at. ex: `in 1h`, `in 1.5h`, `in 5m`, `in 5 mins`, `in 5 hours, 20 minutes, 30 seconds`, etc.
* `for <game>`: set the name of the game to schedule.

While it's started, anyone can say:

* `cancel`: cancels the scheduled game.
* `now`: skips ahead to finish the schedule now.
* `leave`: leaves a scheduled game you joined.
* `but`: restart the game with new starting arguments

Either when starting it, or while it's started, you can say:

* `if <number>`: set a minimum number of players you'd like to play with. You can say this again to change it. ex: `if 3`, `if 5`, etc.

In any case, anyone can say:

* `status [user]`: gives the status of a game currently running, or of any specified Discord user's game. Currently only supports Dota 2. 
* `register <steam account>`: associates a given Steam user to your Discord account for match handling.
* `option set <key> [value]`: sets an option, or removes it if value not specified (admins only)
* `option get <key>`: gets an option (admins only)
* `mark [in/at] <time> to [in/at] <time>`: marks yourself as available for a scheduled game at a given time

## configuration

### settings

`settings.json` starter:

```json
{
  "games": {
    "dota": {
      "display": "Dota 2"
    }
  }
}
```

`settings.json` explanation:

```json5
{
  // any tz timezone: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  // used for parsing specified dates for "game at"
  "local_timezone": "US/Eastern",
  // the keyword to use for messages, this will be fuzzed as well,
  // so you don't have to worry about typos
  "keyword": "game",
  // an extra message to send when the game fails to find any players
  // perhaps one with a captioned image of megamind ;)
  "failure_message": "",
  // an extra message to send when you win a match
  // perhaps a video or gif of confidence and triumph
  "win_message": "",
  // an extra message to send when you lose a match
  // perhaps a video or gif of extreme agony and frustration
  "loss_message": "",
  // Steam bot will filter auto-accepting friend requests to only users who have this Steam 64 ID added
  "mutual_steam_id": 76999999999999999,
  // the list of available games, required
  // first one is default, others can be specified with "game for"
  "games": {
    "dota": {
      "display": "Dota 2",
      // the minimum number of players required to play the game, defaults to 2
      "min": 2,
      // the maximum number of players that can play together
      // this is used to allow players to specify "game if"
      // so that they are only included if there are enough players
      // defaults to equal to min, not allowing this functionality
      "max": 5,
      "overfill": 1,
      "slots": [
        {
          "name": "Safe Lane",
          "num": 1
        },
        {
          "name": "Mid Lane",
          "num": 1
        },
        {
          "name": "Off Lane",
          "num": 1
        },
        {
          "name": "Soft Support",
          "num": 1
        },
        {
          "name": "Hard Support",
          "num": 1
        },
        {
          "name": "Coach",
          "num": 1,
          "overfill": true
        }
      ]
    },
    "apex": {
      "display": "Apex",
      "max": 3
    },
    "fortnite": {
      "display": "Fortnite",
      "max": 4
    },
  }
}
```

### per-server options

* `channel_id`: forces all bot responses to this channel
