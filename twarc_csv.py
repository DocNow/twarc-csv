import json
import click
import pandas as pd

from itertools import chain
from twarc.expansions import flatten


def _inline_referenced_tweets(tweet):
    if "referenced_tweets" in tweet:
        for referenced_tweet in tweet["referenced_tweets"]:
            # extract the referenced tweet as a new row
            yield referenced_tweet

        # leave behind the reference, but not the full tweet
        tweet["referenced_tweets"] = [
            {"type": r["type"], "id": r["id"]} for r in tweet["referenced_tweets"]
        ]

    yield tweet


@click.command()
@click.argument("infile", type=click.File("r"), default="-")
@click.argument("outfile", type=click.File("w"), default="-")
def csv(infile, outfile):
    """
    Convert tweets to CSV.
    """

    json_lines = []
    for line in infile:

        # get a line, and ignore empty lines
        line = line.strip()
        if line == "":
            continue
        obj = json.loads(line)

        # if it has a "data" key ensure data it is flattened
        if "data" in obj:
            # flatten a list of tweets
            if isinstance(obj["data"], list):
                json_lines.extend(flatten(obj)['data'])
            # flatten a single tweet
            else:
                json_lines.append(flatten(obj)['data'])
        else:
            json_lines.append(obj)

    # todo: (Optional) append referenced tweets as new rows
    json_lines = chain.from_iterable(
        _inline_referenced_tweets(tweet) for tweet in json_lines
    )

    df = pd.json_normalize(list(json_lines))

    # todo: (Optional)
    # Drop duplicate columns like geo.place_id and geo.id
    # or select subset of columns here

    # todo: (Optional) json for lists
    df = df.applymap(
        lambda x: json.dumps(x) if pd.api.types.is_list_like(x) else x,
        na_action="ignore",
    )

    # todo: (Optional) json all
    # df = df.applymap(json.dumps, na_action='ignore')

    # todo: (Optional) text escape for fields the user can enter
    df["text"] = df["text"].apply(json.dumps)
    df["author.description"] = df["author.description"].apply(json.dumps)
    df["author.location"] = df["author.location"].apply(json.dumps)

    df.to_csv(outfile, index=False)  # todo: (Optional) arguments for to_csv
