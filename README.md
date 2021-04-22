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

## Issues with Twitter Data in CSV

CSV isn't the best choice for storing twitter data. Always keep the original API responses, and perform feature extraction on json objects.

This export script is intended for convenience, for importing samples of data into other tools, there are many ways to format a CSV of tweets, and this is just one way.

## Contributing

Suggestions, opinions, and pull requests welcome and encouraged. Even if you are just interested in using this plugin, post your use case in the Issues.
