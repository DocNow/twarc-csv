# twarc-csv

This module adds CSV Export for Tweets to `twarc`.

Make sure twarc is installed and configured:

```
pip3 install --upgrade twarc
twarc2 configure
```

Install this plugin:

```
pip3 install --upgrade twarc-csv
```

A new `csv` command will be available in twarc. If you have collected some
tweets in a file `tweets.jsonl` you can now convert them to CSV

```
twarc2 search --limit 500 "blacklivesmatter" tweets.jsonl # collect some tweets
twarc2 csv tweets.jsonl tweets.csv # convert to CSV
```

## Extra Command Line Options

Run

```
twarc2 csv --help
```

For a list of options.

```
Options:
  --json-encode-all / --no-json-encode-all
                                  JSON encode / escape all fields. Default: no
  --json-encode-lists / --no-json-encode-lists
                                  JSON encode / escape lists. Default: yes
  --json-encode-text / --no-json-encode-text
                                  JSON encode / escape text fields. Default: no

  --inline-referenced-tweets / --no-inline-referenced-tweets
                                  Output referenced tweets inline as separate
                                  rows. Default: yes

  --inline-pinned-tweets / --no-pinned-tweets
                                  If converting a user dataset, output pinned
                                  tweets inline as separate rows. Default: no

  --allow-duplicates / --no-allow-duplicates
                                  Remove duplicate tweets by ID. Default: yes
  --input-tweet-columns / --no-input-tweet-columns
                                  Use a default list of tweet column names in
                                  the input. Only modify this if you have
                                  processed the json yourself. Default: yes

  --input-users-columns / --no-input-users-columns
                                  Use a default list of user column names in
                                  the input. Only modify this if you have a
                                  dataset of users as opposed to tweets.
                                  Default: no

  --extra-input-columns TEXT      Manually specify extra input columns. Comma
                                  separated string. Default is blank, no extra
                                  input columns

  --output-columns TEXT           Specify what columns to output in the CSV.
                                  Default is all input columns.

  --batch-size INTEGER            How many lines to process per chunk. Default
                                  is 1000

  --show-stats / --no-show-stats  Show stats about the dataset on completion.
                                  Default is show. Always hidden if you're
                                  using stdin / stdout pipes.

  --help                          Show this message and exit.
```

## Issues with Twitter Data in CSV

CSV isn't the best choice for storing twitter data. Always keep the original API responses, and perform feature extraction on json objects.

This export script is intended for convenience, for importing samples of data into other tools, there are many ways to format a CSV of tweets, and this is just one way.

## Contributing

Suggestions, opinions, and pull requests welcome and encouraged. Even if you are just interested in using this plugin, post your use case in the Issues.
