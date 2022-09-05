# game bot

quick discord bot for pinging people to play games

## running

1. `pipenv install`
2. `GAME_BOT_TOKEN=token pipenv run python bot.py` 

## commands

Just say `game` to schedule a game, and say `game` to join up for a scheduled one. Only supports one running at a time.

When starting it, you can also say some arguments:

* `at`: set a specific date/time to check at. ex: `at 9`, `at 9pm`, `at 9pm PST`, `at 9:30`, etc.
* `in`: set a length of time to check at. ex: `in 1h`, `in 1.5h`, `in 5m`, `in 5 mins`, `in 5 hours, 20 minutes, 30 seconds`, etc.
* `for`: set the name of the game to schedule.

While it's started, anyone can say:

* `cancel`: cancels the scheduled game.
* `now`: skips ahead to finish the schedule now.
* `leave`: leaves a scheduled game you joined.

Either when starting it, or while it's started, you can say:

* `if`: set a minimum number of players you'd like to play with. You can say this again to change it. ex: `if 3`, `if 5`, etc.

## configuration

`settings.json` starter:

```json
{
  "games": {
    "apex": {
      "role": 123456789123456789
    }
  }
}
```

`settings.json` explanation:

```json lines
{
  // any tz timezone: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  // used for parsing specified dates for "game at"
  "local_timezone": "US/Eastern",
  // the keyword to use for messages, this will be fuzzed as well, so
  // you don't have to worry about 
  "keyword": "game",
  // an extra message to send when the game fails to find any players
  // perhaps one with a captioned image of megamind ;)
  "failure_message": "",
  // the list of available games, required
  // first one is default, others can be specified with "game for"
  "games": {
    "apex": {
      // the developer ID for the role you want to ping for this game
      "role": 123456789123456789,
      // the minimum number of players required to play the game, defaults to 2
      "min": 2,
      // the maximum number of players that can play together
      // this is used to allow players to specify "game if"
      // so that they are only included if there are enough players
      // defaults to equal to min, not allowing this functionality
      "max": 2,
    },
    "fortnite": {
      role: 123456789123456790,
    },
  }
}
```
