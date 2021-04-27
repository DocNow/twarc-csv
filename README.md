# twarc-csv

This module adds CSV Export for Tweets to `twarc`.

Make sure twarc is installed and configured:

```
pip install --upgrade twarc
twarc2 configure
```

Install this plugin:

```
pip install --upgrade twarc-csv
```

A new `csv` command will be available in twarc. If you have collected some
tweets in a file `tweets.jsonl` you can now convert them to CSV

```
twarc2 search --limit 500 "blacklivesmatter" tweets.jsonl # collect some tweets
twarc2 csv tweets.jsonl tweets.csv # convert to CSV
```

## Extra Command Line Options

`--json-encode-text` / `--no-json-encode-text` Apply json escaping to text fields. Off by default but may break the CSV due to newlines and emoji.

`--json-encode-all` / `--no-json-encode-all` Apply json escaping to all data fields. Off by default.

`--json-encode-lists` / `--no-json-encode-lists` Apply json escaping to lists. On by Default.

`--inline-referenced-tweets` / `--no-inline-referenced-tweets` Include referenced tweets as "rows" as opposed to being "inside" other tweets. On by default.

`--allow-duplicates` / `--no-allow-duplicates` Output all tweets, without filtering duplicates. Off by default.

`--batch-size` How many tweets to process per "chunk" for large files. Default is 5000.

## Issues with Twitter Data in CSV

CSV isn't the best choice for storing twitter data. Always keep the original API responses, and perform feature extraction on json objects.

This export script is intended for convenience, for importing samples of data into other tools, there are many ways to format a CSV of tweets, and this is just one way.

## Contributing

Suggestions, opinions, and pull requests welcome and encouraged. Even if you are just interested in using this plugin, post your use case in the Issues.
