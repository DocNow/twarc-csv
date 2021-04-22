import os
import json
import click
import logging
import itertools
import pandas as pd
from tqdm import tqdm
from collections import OrderedDict
from more_itertools import ichunked
from twarc.expansions import flatten

log = logging.getLogger("twarc")


class CSVConverter:
    def __init__(
        self,
        infile,
        outfile,
        json_encode_all,
        json_encode_lists,
        json_encode_text,
        inline_referenced_tweets,
        allow_duplicates,
        batch_size,
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
        self.progress = tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            total=os.stat(self.infile.name).st_size,
        )
        self.counts = {
            "lines": 0,
            "tweets": 0,
            "referenced_tweets": 0,
            "parse_errors": 0,
            "duplicates": 0,
            "rows": 0,
            "columns": 0,
        }

    def _read_lines(self):
        """
        Generator for reading files line byline from a file. Progress bar is based on file size.
        """
        line = self.infile.readline()
        while line:
            self.counts["lines"] = self.counts["lines"] + 1
            if line.strip() is not "":
                try:
                    o = json.loads(line)
                    yield o
                except Exception as ex:
                    self.counts["parse_errors"] = self.counts["parse_errors"] + 1
                    log.error(f"Error when trying to parse json: '{line}' {ex}")
            self.progress.update(self.infile.tell() - self.progress.n)
            line = self.infile.readline()

    def _handle_formats(self, batch):
        """
        Handle different types of json formats, generating 1 tweet at a time

        a batch is a number of lines from a json,
        these can be full pages of requests or individual tweets.
        """
        for item in batch:
            # if it has a "data" key ensure data it is flattened
            if "data" in item:
                # flatten a list of tweets
                if isinstance(item["data"], list):
                    for i in flatten(item)["data"]:
                        yield i
                # flatten a single tweet, eg, from stream
                else:
                    yield flatten(item)["data"]
            else:
                # this assumes the data is flattened
                yield item

    def _inline_referenced_tweets(self, tweet):
        """
        Insert referenced tweets into the main CSV
        """
        if "referenced_tweets" in tweet and self.inline_referenced_tweets:
            for referenced_tweet in tweet["referenced_tweets"]:
                # extract the referenced tweet as a new row
                self.counts["referenced_tweets"] = self.counts["referenced_tweets"] + 1
                yield referenced_tweet
            # leave behind the reference, but not the full tweet
            tweet["referenced_tweets"] = [
                {"type": r["type"], "id": r["id"]} for r in tweet["referenced_tweets"]
            ]
        yield tweet

    def _process_tweets(self, tweets):
        """
        Process a single tweet before adding it to the dataframe.
        ToDo: Drop columns and dedupe etc here.
        """
        for tweet in tweets:
            # Order the fields in the json, because JSON key order isn't guaranteed.
            # Needed so that different batches won't produce different ordered columns
            json_keys = sorted(tweet.keys())
            selected_field_order = list()

            # Opinon: always put in id,created_at,text first, and then the rest
            if "id" in json_keys:
                selected_field_order.append(json_keys.pop(json_keys.index("id")))
            if "created_at" in json_keys:
                selected_field_order.append(
                    json_keys.pop(json_keys.index("created_at"))
                )
            if "text" in json_keys:
                selected_field_order.append(json_keys.pop(json_keys.index("text")))
            selected_field_order.extend(json_keys)

            tweet = OrderedDict((k, tweet[k]) for k in selected_field_order)

            self.counts["tweets"] = self.counts["tweets"] + 1
            if tweet["id"] in self.dataset_ids:
                self.counts["duplicates"] = self.counts["duplicates"] + 1

            if self.allow_duplicates:
                yield tweet
            else:
                if tweet["id"] not in self.dataset_ids:
                    yield tweet

            self.dataset_ids.add(tweet["id"])

    def _process_dataframe(self, _df):
        # (Optional) json encode all
        if self.json_encode_all:
            _df = _df.applymap(json.dumps, na_action="ignore")
        else:
            # (Optional) json for lists
            if self.json_encode_lists:
                _df = _df.applymap(
                    lambda x: json.dumps(x) if pd.api.types.is_list_like(x) else x,
                    na_action="ignore",
                )

            # (Optional) text escape for fields the user can enter
            if self.json_encode_text:
                _df["text"] = _df["text"].apply(json.dumps)
                _df["author.description"] = _df["author.description"].apply(json.dumps)
                _df["author.location"] = _df["author.location"].apply(json.dumps)

        return _df

    def _process_batch(self, batch):

        # todo: (Optional) append referenced tweets as new rows
        tweet_batch = itertools.chain.from_iterable(
            self._process_tweets(self._inline_referenced_tweets(tweet))
            for tweet in self._handle_formats(batch)
        )

        _df = pd.json_normalize([tweet for tweet in tweet_batch])
        _df = self._process_dataframe(_df)

        return _df

    def _write_output(self, _df, first_batch):
        """
        Write out the dataframe chunk by chunk

        todo: take parameters from commandline for optional output formats.
        """

        if first_batch:
            mode = "w"
            header = True
            self.counts["columns"] = len(_df.columns)
        else:
            mode = "a+"
            header = False

        self.counts["rows"] = self.counts["rows"] + len(_df)
        _df.to_csv(
            self.outfile, mode=mode, index=False, header=header
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
@click.argument("infile", type=click.File("r"), default="-")
@click.argument("outfile", type=click.File("w"), default="-")
@click.option("--json-encode-all/--no-json-encode-all", default=False)
@click.option("--json-encode-lists/--no-json-encode-lists", default=True)
@click.option("--json-encode-text/--no-json-encode-text", default=False)
@click.option("--inline-referenced-tweets/--no-inline-referenced-tweets", default=True)
@click.option("--allow-duplicates/--no-allow-duplicates", default=False)
@click.option("--batch-size", type=int, default=5000)
def csv(
    infile,
    outfile,
    json_encode_all,
    json_encode_lists,
    json_encode_text,
    inline_referenced_tweets,
    allow_duplicates,
    batch_size,
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

    converter = CSVConverter(
        infile,
        outfile,
        json_encode_all,
        json_encode_lists,
        json_encode_text,
        inline_referenced_tweets,
        allow_duplicates,
        batch_size,
    )
    converter.process()

    errors = (
        click.style(
            f"{converter.counts['parse_errors']} failed to parse. See twarc.log for details.\n",
            fg="red",
        )
        if converter.counts["parse_errors"] > 0
        else ""
    )

    click.echo(
        f"\nℹ️\n"
        + f"Processed {converter.counts['tweets']} tweets from {converter.counts['lines']} lines. \n"
        + f"{converter.counts['referenced_tweets']} were referenced tweets, {converter.counts['duplicates']} were duplicates.\n"
        + errors
        + f"There are {converter.counts['rows']} rows and {converter.counts['columns']} columns in the CSV.\n",
        err=True,
    )
