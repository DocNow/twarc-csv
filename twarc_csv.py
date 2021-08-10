import os
import json
import click
import logging
import itertools
import pandas as pd
from tqdm.auto import tqdm
from collections import OrderedDict
from more_itertools import ichunked
from twarc import ensure_flattened

log = logging.getLogger("twarc")

DEFAULT_TWEET_COLUMNS = """id
created_at
text
attachments.media
attachments.media_keys
attachments.poll.duration_minutes
attachments.poll.end_datetime
attachments.poll.id
attachments.poll.options
attachments.poll.voting_status
attachments.poll_ids
author.id
author.created_at
author.username
author.name
author.description
author.entities.description.cashtags
author.entities.description.hashtags
author.entities.description.mentions
author.entities.description.urls
author.entities.url.urls
author.location
author.pinned_tweet_id
author.profile_image_url
author.protected
author.public_metrics.followers_count
author.public_metrics.following_count
author.public_metrics.listed_count
author.public_metrics.tweet_count
author.url
author.verified
author.withheld.scope
author.withheld.copyright
author.withheld.country_codes
author_id
context_annotations
conversation_id
entities.annotations
entities.cashtags
entities.hashtags
entities.mentions
entities.urls
geo.coordinates.coordinates
geo.coordinates.type
geo.country
geo.country_code
geo.full_name
geo.geo.bbox
geo.geo.type
geo.id
geo.name
geo.place_id
geo.place_type
in_reply_to_user.id
in_reply_to_user.created_at
in_reply_to_user.username
in_reply_to_user.name
in_reply_to_user.description
in_reply_to_user.entities.description.cashtags
in_reply_to_user.entities.description.hashtags
in_reply_to_user.entities.description.mentions
in_reply_to_user.entities.description.urls
in_reply_to_user.entities.url.urls
in_reply_to_user.location
in_reply_to_user.pinned_tweet_id
in_reply_to_user.profile_image_url
in_reply_to_user.protected
in_reply_to_user.public_metrics.followers_count
in_reply_to_user.public_metrics.following_count
in_reply_to_user.public_metrics.listed_count
in_reply_to_user.public_metrics.tweet_count
in_reply_to_user.url
in_reply_to_user.verified
in_reply_to_user.withheld.scope
in_reply_to_user.withheld.copyright
in_reply_to_user.withheld.country_codes
in_reply_to_user_id
lang
possibly_sensitive
public_metrics.like_count
public_metrics.quote_count
public_metrics.reply_count
public_metrics.retweet_count
referenced_tweets
reply_settings
source
withheld.scope
withheld.copyright
withheld.country_codes
type
__twarc.retrieved_at
__twarc.url
__twarc.version
""".split(
    "\n"
)

DEFAULT_USERS_COLUMNS = """id
created_at
username
name
description
entities.description.cashtags
entities.description.hashtags
entities.description.mentions
entities.description.urls
entities.url.urls
location
pinned_tweet_id
pinned_tweet
profile_image_url
protected
public_metrics.followers_count
public_metrics.following_count
public_metrics.listed_count
public_metrics.tweet_count
url
verified
withheld.scope
withheld.copyright
withheld.country_codes
__twarc.retrieved_at
__twarc.url
__twarc.version
""".split(
    "\n"
)


class CSVConverter:
    def __init__(
        self,
        infile,
        outfile,
        json_encode_all=False,
        json_encode_lists=True,
        json_encode_text=False,
        inline_referenced_tweets=True,
        allow_duplicates=False,
        input_tweet_columns=True,
        input_users_columns=False,
        extra_input_columns="",
        output_columns="",
        batch_size=100,
    ):
        self.infile = infile
        self.outfile = outfile
        self.json_encode_all = json_encode_all
        self.json_encode_lists = json_encode_lists
        self.json_encode_text = json_encode_text
        self.inline_referenced_tweets = inline_referenced_tweets
        self.allow_duplicates = allow_duplicates
        self.batch_size = batch_size
        self.dataset_ids = set()
        self.std = infile.name == "<stdin>" or outfile.name == "<stdout>"
        self.progress = tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            total=os.stat(infile.name).st_size if not self.std else 1,
            disable=self.std,
        )
        self.columns = list()
        if input_tweet_columns:
            self.columns.extend(
                x for x in DEFAULT_TWEET_COLUMNS if x not in self.columns
            )
        if input_users_columns:
            self.columns.extend(
                x for x in DEFAULT_USERS_COLUMNS if x not in self.columns
            )
        if extra_input_columns:
            self.columns.extend(
                x for x in extra_input_columns.split(",") if x not in self.columns
            )

        self.output_columns = (
            output_columns.split(",") if output_columns else self.columns
        )

        self.counts = {
            "lines": 0,
            "tweets": 0,
            "referenced_tweets": 0,
            "unavailable": 0,
            "non_tweets": 0,
            "parse_errors": 0,
            "duplicates": 0,
            "rows": 0,
            "input_columns": len(self.columns),
            "output_columns": len(self.output_columns),
        }

    def _read_lines(self):
        """
        Generator for reading files line byline from a file. Progress bar is based on file size.
        """
        line = self.infile.readline()
        while line:
            self.counts["lines"] += 1
            if line.strip() != "":
                try:
                    o = json.loads(line)
                    yield o
                except Exception as ex:
                    self.counts["parse_errors"] += 1
                    log.error(f"Error when trying to parse json: '{line}' {ex}")
            if not self.std:
                self.progress.update(self.infile.tell() - self.progress.n)
            line = self.infile.readline()

    def _generate_tweets(self, batch):
        """
        Generate flattened tweets from a batch.
        """
        for item in batch:
            for tweet in ensure_flattened(item):
                yield tweet

    def _inline_referenced_tweets(self, tweet):
        """
        Insert referenced tweets into the main CSV
        """
        if "referenced_tweets" in tweet and self.inline_referenced_tweets:
            for referenced_tweet in tweet["referenced_tweets"]:
                # extract the referenced tweet as a new row
                self.counts["referenced_tweets"] += 1
                # inherit __twarc metadata from parent tweet
                referenced_tweet["__twarc"] = (
                    tweet["__twarc"] if "__twarc" in tweet else None
                )
                # write tweet as new row if referenced tweet exists (has more than the 3 default fields):
                if len(referenced_tweet.keys()) > 3:
                    yield referenced_tweet
                else:
                    self.counts["unavailable"] += 1
            # leave behind the reference, but not the full tweet
            tweet["referenced_tweets"] = [
                {"type": r["type"], "id": r["id"]} for r in tweet["referenced_tweets"]
            ]

        # Deal with pinned tweets for user datasets, `tweet` here is actually a user:
        # remove the tweet from a user dataset, pinned_tweet_id remains:
        tweet.pop("pinned_tweet", None)

        yield tweet

    def _process_tweets(self, tweets):
        """
        Process a single tweet before adding it to the dataframe.
        ToDo: Drop columns and dedupe etc here.
        """
        for tweet in tweets:
            if "id" in tweet:
                tweet_id = tweet["id"]
                self.counts["tweets"] += 1
                if tweet_id in self.dataset_ids:
                    self.counts["duplicates"] += 1

                if self.allow_duplicates:
                    yield tweet
                else:
                    if tweet_id not in self.dataset_ids:
                        yield tweet

                self.dataset_ids.add(tweet_id)
            else:
                self.counts["non_tweets"] += 1

    def _process_dataframe(self, _df):
        # (Optional) json encode all
        if self.json_encode_all:
            _df = _df.applymap(json.dumps, na_action="ignore")
        else:
            # (Optional) text escape for any text fields
            if self.json_encode_text:
                _df = _df.applymap(
                    lambda x: json.dumps(x) if type(x) is str else x,
                    na_action="ignore",
                )
            else:
                # Mandatory newline escape to prevent breaking csv format:
                _df = _df.applymap(
                    lambda x: x.replace("\r", "").replace("\n", r"\n")
                    if type(x) is str
                    else x,
                    na_action="ignore",
                )
            # (Optional) json for lists
            if self.json_encode_lists:
                _df = _df.applymap(
                    lambda x: json.dumps(x) if pd.api.types.is_list_like(x) else x,
                    na_action="ignore",
                )
        return _df

    def _process_batch(self, batch):

        # (Optional) append referenced tweets as new rows
        tweet_batch = itertools.chain.from_iterable(
            self._process_tweets(self._inline_referenced_tweets(tweet))
            for tweet in self._generate_tweets(batch)
        )

        _df = pd.json_normalize(list(tweet_batch), errors="ignore")

        # Check for mismatched columns
        diff = set(_df.columns) - set(self.columns)
        if len(diff) > 0:
            click.echo(
                click.style(
                    f"💔 ERROR: {len(diff)} Unexpected items in data! to fix, add these with:\n--extra-input-columns \"{','.join(diff)}\"\nSkipping entire batch of {len(_df)} tweets!",
                    fg="red",
                ),
                err=True,
            )
            log.error(
                f"CSV Unexpected Data: \"{','.join(diff)}\". Expected {len(self.columns)} columns, got {len(_df.columns)}. Skipping entire batch of {len(_df)} tweets!"
            )
            self.counts["parse_errors"] += len(_df)
            return pd.DataFrame(columns=self.columns)

        return self._process_dataframe(_df.reindex(columns=self.columns))

    def _write_output(self, _df, first_batch):
        """
        Write out the dataframe chunk by chunk

        todo: take parameters from commandline for optional output formats.
        """

        if first_batch:
            mode = "w"
            header = True
        else:
            mode = "a+"
            header = False

        self.counts["rows"] += len(_df)
        _df.to_csv(
            self.outfile,
            mode=mode,
            columns=self.output_columns,
            index=False,
            header=header,
        )  # todo: (Optional) arguments for to_csv

    def process(self):
        """
        Process a file containing JSON into a CSV
        """

        # Flag for writing header & appending to CSV file
        first_batch = True
        for batch in ichunked(self._read_lines(), self.batch_size):
            self._write_output(self._process_batch(batch), first_batch)
            first_batch = False

        self.progress.close()


@click.command()
@click.argument("infile", type=click.File("r", encoding="utf8"), default="-")
@click.argument("outfile", type=click.File("w", encoding="utf8"), default="-")
@click.option(
    "--json-encode-all/--no-json-encode-all",
    default=False,
    help="JSON encode / escape all fields. Default: no",
)
@click.option(
    "--json-encode-lists/--no-json-encode-lists",
    default=True,
    help="JSON encode / escape lists. Default: yes",
)
@click.option(
    "--json-encode-text/--no-json-encode-text",
    default=False,
    help="JSON encode / escape text fields. Default: no",
)
@click.option(
    "--inline-referenced-tweets/--no-inline-referenced-tweets",
    default=True,
    help="Output referenced tweets inline as separate rows. Default: yes",
)
@click.option(
    "--allow-duplicates/--no-allow-duplicates",
    default=False,
    help="Remove duplicate tweets by ID. Default: yes",
)
@click.option(
    "--input-tweet-columns/--no-input-tweet-columns",
    default=True,
    help="Use a default list of tweet column names in the input. Only modify this if you have processed the json yourself. Default: yes",
)
@click.option(
    "--input-users-columns/--no-input-users-columns",
    default=False,
    help="Use a default list of user column names in the input. Only modify this if you have a dataset of users as opposed to tweets. Default: no",
)
@click.option(
    "--extra-input-columns",
    default="",
    help="Manually specify extra input columns. Comma separated string. Default is blank, no extra input columns.",
)
@click.option(
    "--output-columns",
    default="",
    help="Specify what columns to output in the CSV. Default is all input columns.",
)
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="How many lines to process per chunk. Default is 100. Reduce this if output is slow.",
)
@click.option(
    "--show-stats/--no-show-stats",
    default=True,
    help="Show stats about the dataset on completion. Default is show. Always hidden if you're using stdin / stdout pipes.",
)
def csv(
    infile,
    outfile,
    json_encode_all,
    json_encode_lists,
    json_encode_text,
    inline_referenced_tweets,
    allow_duplicates,
    input_tweet_columns,
    input_users_columns,
    extra_input_columns,
    output_columns,
    batch_size,
    show_stats,
):
    """
    Convert tweets to CSV.
    """

    if infile.name == outfile.name:
        click.echo(
            click.style(
                f"💔 Cannot convert files in-place, specify a different output file!",
                fg="red",
            ),
            err=True,
        )
        return

    if (
        not (infile.name == "<stdin>" or outfile.name == "<stdout>")
        and os.stat(infile.name).st_size == 0
    ):
        click.echo(
            click.style(
                f"💔 Input file is empty! Nothing to convert.",
                fg="red",
            ),
            err=True,
        )
        return

    converter = CSVConverter(
        infile,
        outfile,
        json_encode_all,
        json_encode_lists,
        json_encode_text,
        inline_referenced_tweets,
        allow_duplicates,
        input_tweet_columns,
        input_users_columns,
        extra_input_columns,
        output_columns,
        batch_size,
    )
    converter.process()

    if show_stats and outfile.name != "<stdout>":

        errors = (
            click.style(
                f"{converter.counts['parse_errors']} failed to parse. See twarc.log for details.\n",
                fg="red",
            )
            if converter.counts["parse_errors"] > 0
            else ""
        )

        referenced_stats = (
            f"{converter.counts['referenced_tweets']} were referenced tweets, {converter.counts['duplicates']} were referenced multiple times, and {converter.counts['unavailable']} were referenced but not available in the API responses.\n"
            if inline_referenced_tweets
            else ""
        )

        click.echo(
            f"\nℹ️\n"
            + f"Parsed {converter.counts['tweets']} tweets from {converter.counts['lines']} lines in the file, and {converter.counts['non_tweets']} non tweet objects.\n"
            + referenced_stats
            + errors
            + f"Wrote {converter.counts['rows']} rows and output {converter.counts['output_columns']} of {converter.counts['input_columns']} input columns in the CSV.\n",
            err=True,
        )
