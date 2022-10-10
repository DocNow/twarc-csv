import json
import copy
import click
import itertools
from collections import ChainMap
import logging

import pandas as pd
from twarc import ensure_flattened

log = logging.getLogger("twarc")

DEFAULT_TWEET_COLUMNS = """id
conversation_id
referenced_tweets.replied_to.id
referenced_tweets.retweeted.id
referenced_tweets.quoted.id
edit_history_tweet_ids
edit_controls.edits_remaining
edit_controls.editable_until
edit_controls.is_edit_eligible
author_id
in_reply_to_user_id
retweeted_user_id
quoted_user_id
created_at
text
lang
source
public_metrics.like_count
public_metrics.quote_count
public_metrics.reply_count
public_metrics.retweet_count
reply_settings
possibly_sensitive
withheld.scope
withheld.copyright
withheld.country_codes
entities.annotations
entities.cashtags
entities.hashtags
entities.mentions
entities.urls
context_annotations
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
matching_rules
__twarc.retrieved_at
__twarc.url
__twarc.version
""".split(
    "\n"
)

DEFAULT_USER_COLUMNS = """id
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

DEFAULT_COMPLIANCE_COLUMNS = """id
action
created_at
redacted_at
reason
""".split(
    "\n"
)

DEFAULT_COUNTS_COLUMNS = """start
end
tweet_count
__twarc.retrieved_at
__twarc.url
__twarc.version
""".split(
    "\n"
)

DEFAULT_LISTS_COLUMNS = """
id
owner_id
created_at
name
description
member_count
follower_count
private
__twarc.retrieved_at
__twarc.url
__twarc.version
""".split(
    "\n"
)


class DataFrameConverter:
    """
    Convert a set of JSON Objects into a Pandas DataFrame object.
    You can call this directly on a small set of objects, but memory is quickly consumed for larger datasets.

    This class can accept individual tweets or whole response objects.

    Args:
        objects (iterable): JSON Objects to convert. Can be users, tweets, or other API objects.
        input_data_type (str): data type: `tweets` or `users` or `compliance` or `counts`
    Returns:
        DataFrame: The objects provided as a Pandas DataFrame.
    """

    def __init__(
        self,
        input_data_type="tweets",
        json_encode_all=False,
        json_encode_text=False,
        json_encode_lists=True,
        inline_referenced_tweets=False,
        merge_retweets=True,
        allow_duplicates=False,
        extra_input_columns="",
        output_columns=None,
        dataset_ids=None,
        counts=None,
    ):
        self.json_encode_all = json_encode_all
        self.json_encode_text = json_encode_text
        self.json_encode_lists = json_encode_lists
        self.inline_referenced_tweets = inline_referenced_tweets
        self.merge_retweets = merge_retweets
        self.allow_duplicates = allow_duplicates
        self.input_data_type = input_data_type
        self.columns = list()
        if input_data_type == "tweets":
            self.columns.extend(
                x for x in DEFAULT_TWEET_COLUMNS if x not in self.columns
            )
        if input_data_type == "users":
            self.columns.extend(
                x for x in DEFAULT_USER_COLUMNS if x not in self.columns
            )
        if input_data_type == "compliance":
            self.columns.extend(
                x for x in DEFAULT_COMPLIANCE_COLUMNS if x not in self.columns
            )
        if input_data_type == "counts":
            self.columns.extend(
                x for x in DEFAULT_COUNTS_COLUMNS if x not in self.columns
            )
        if input_data_type == "lists":
            self.columns.extend(
                x for x in DEFAULT_LISTS_COLUMNS if x not in self.columns
            )
        if extra_input_columns:
            self.columns.extend(
                x for x in extra_input_columns.split(",") if x not in self.columns
            )
        self.output_columns = (
            output_columns.split(",") if output_columns else self.columns
        )
        self.dataset_ids = dataset_ids if dataset_ids else set()
        self.counts = (
            counts
            if counts
            else {
                "lines": 0,
                "tweets": 0,
                "referenced_tweets": 0,
                "retweets": 0,
                "quotes": 0,
                "replies": 0,
                "unavailable": 0,
                "non_objects": 0,
                "parse_errors": 0,
                "duplicates": 0,
                "rows": 0,
                "input_columns": len(self.columns),
                "output_columns": len(self.output_columns),
            }
        )

    def _flatten_objects(self, objects):
        """
        Generate flattened tweets from a batch of parsed lines.
        """
        for o in objects:
            for item in ensure_flattened(o):
                yield item

    def _inline_referenced_tweets(self, tweet):
        """
        (Optional) Insert referenced tweets into the main CSV as new rows
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
                    yield self._format_tweet(referenced_tweet)
                else:
                    self.counts["unavailable"] += 1
        yield self._format_tweet(tweet)

    def _format_tweet(self, tweet):
        """
        Make the tweet objects easier to deal with, removing extra info and changing the structure.
        """
        # Make a copy of the original flattened tweet
        tweet = copy.deepcopy(tweet)
        # Deal with pinned tweets for user datasets, `tweet` here is actually a user:
        # remove the tweet from a user dataset, pinned_tweet_id remains:
        tweet.pop("pinned_tweet", None)
        # Remove in_reply_to_user, in_reply_to_user_id remains:
        tweet.pop("in_reply_to_user", None)

        if "referenced_tweets" in tweet:

            # Count Replies:
            replies = [
                t for t in tweet["referenced_tweets"] if t["type"] == "replied_to"
            ]
            reply_tweet = replies[-1] if replies else None
            if "in_reply_to_user_id" in tweet or reply_tweet:
                self.counts["replies"] += 1

            # Extract Retweet only
            rts = [t for t in tweet["referenced_tweets"] if t["type"] == "retweeted"]
            retweeted_tweet = rts[-1] if rts else None
            if retweeted_tweet and "author_id" in retweeted_tweet:
                self.counts["retweets"] += 1
                tweet["retweeted_user_id"] = retweeted_tweet["author_id"]

            # Extract Quoted tweet
            qts = [t for t in tweet["referenced_tweets"] if t["type"] == "quoted"]
            quoted_tweet = qts[-1] if qts else None
            if quoted_tweet and "author_id" in quoted_tweet:
                self.counts["quotes"] += 1
                tweet["quoted_user_id"] = quoted_tweet["author_id"]

            # Process Retweets:
            # If it's a native retweet, replace the "RT @user Text" with the original text, metrics, and entities, but keep the Author.
            if retweeted_tweet and self.merge_retweets:
                # A retweet inherits everything from retweeted tweet.
                tweet["text"] = retweeted_tweet.pop("text", tweet.pop("text", None))
                tweet["entities"] = retweeted_tweet.pop(
                    "entities", tweet.pop("entities", None)
                )
                tweet["attachments"] = retweeted_tweet.pop(
                    "attachments", tweet.pop("attachments", None)
                )
                tweet["context_annotations"] = retweeted_tweet.pop(
                    "context_annotations", tweet.pop("context_annotations", None)
                )
                tweet["public_metrics"] = retweeted_tweet.pop(
                    "public_metrics", tweet.pop("public_metrics", None)
                )

            # reconstruct referenced_tweets object
            referenced_tweets = [
                {r["type"]: {"id": r["id"]}} for r in tweet["referenced_tweets"]
            ]
            # leave behind references, but not the full tweets
            # ChainMap flattens list into properties
            tweet["referenced_tweets"] = dict(ChainMap(*referenced_tweets))
        else:
            tweet["referenced_tweets"] = {}

        # Remove `type` left over from referenced tweets
        tweet.pop("type", None)
        # Remove empty objects
        if "attachments" in tweet and not tweet["attachments"]:
            tweet.pop("attachments", None)
        if "entities" in tweet and not tweet["entities"]:
            tweet.pop("entities", None)
        if "public_metrics" in tweet and not tweet["public_metrics"]:
            tweet.pop("public_metrics", None)
        if "pinned_tweet" in tweet and not tweet["pinned_tweet"]:
            tweet.pop("pinned_tweet", None)

        return tweet

    def _process_tweets(self, tweets):
        """
        Count, deduplicate objects before adding them to the dataframe.
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
            elif self.input_data_type == "counts":
                self.counts["tweets"] += 1
                yield tweet
            else:
                # non tweet objects are usually streaming API errors etc.
                self.counts["non_objects"] += 1

    def _process_dataframe(self, _df):
        """
        Apply additional preprocessing to the DataFrame contents.
        """

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

    def process(self, objects):
        """
        Process the objects into a pandas dataframe.
        """

        tweet_batch = itertools.chain.from_iterable(
            self._process_tweets(self._inline_referenced_tweets(tweet))
            for tweet in self._flatten_objects(objects)
        )
        _df = pd.json_normalize(list(tweet_batch))
        # Check for mismatched columns
        diff = set(_df.columns) - set(self.columns)
        if len(diff) > 0:
            click.echo(
                click.style(
                    f"💔 ERROR: {len(diff)} Unexpected items in data! \n"
                    "Are you sure you specified the correct --input-data-type?\n"
                    "If the object type is correct, add extra columns with:"
                    f"\n--extra-input-columns \"{','.join(diff)}\"\nSkipping entire batch of {len(_df)} tweets!",
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
