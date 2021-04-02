# twarc-csv

This module adds CSV Export for Tweets to `twarc`. Work in progress.

To install `twarc2` and this plugin:

```
pip install --upgrade https://github.com/DocNow/twarc/archive/v2.zip
pip install twarc-csv
```

A new command will be available in twarc. First, after you've gathered some tweets, flatten the data with:

```
twarc2 flatten gathered_tweets.json gathered_tweets_flattened.json
```

Now you can convert to CSV

```
twarc2 csv gathered_tweets_flattened.json gathered_tweets.csv
```

Currently, this expects 1 tweet json per line, that comes from the flatten command. In a later version it will auto detect what format of json you're giving it, and act appropriately.

## Issues with Twitter Data in CSV

CSV is a poor choice for storing twitter data. Always keep the original API responses, and perform feature extraction on json objects.

This export script is intended for convenience, for importing samples of data into other tools. The work in progress script does not expose any customizations or configuration yet, there are many ways to format a CSV of tweets, and this is just one way. 

## Contributing

Suggestions, opinions, and pull requests welcome and encouraged. Even if you are just interested in using this plugin, post your use case in the Issues.
