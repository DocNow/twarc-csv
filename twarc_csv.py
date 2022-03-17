import os
import click
import dataframe_converter
import csv_writer
import logging

log = logging.getLogger("twarc")

CSVConverter = csv_writer.CSVConverter
DataFrameConverter = dataframe_converter.DataFrameConverter


def _validate_output_columns(context, parameter, value):
    """
    Validate specified output columns
    """

    input_data_type = (
        "input_data_type" in context.params and context.params["input_data_type"]
    )

    if value:
        values = value.split(",")
        valid = {
            "tweets": dataframe_converter.DEFAULT_TWEET_COLUMNS,
            "users": dataframe_converter.DEFAULT_USER_COLUMNS,
            "counts": dataframe_converter.DEFAULT_COUNTS_COLUMNS,
            "compliance": dataframe_converter.DEFAULT_COMPLIANCE_COLUMNS,
            "lists": dataframe_converter.DEFAULT_LISTS_COLUMNS,
        }
        for v in values:
            if v not in valid[input_data_type]:
                raise click.BadOptionUsage(
                    parameter.name,
                    f'"{v}" is not a valid entry for --{parameter.name}. Must be a comma separated string, without spaces, valid entries: {",".join(valid[input_data_type])}',
                )
        return ",".join(values)


@click.command()
@click.argument("infile", type=click.File("r", encoding="utf8"), default="-")
@click.argument("outfile", type=click.File("w", encoding="utf8"), default="-")
@click.option(
    "--input-data-type",
    required=False,
    is_eager=True,
    default="tweets",
    help='Input data type - you can turn "tweets", "users", "counts" or "compliance" or "lists" data into CSV.',
    type=click.Choice(
        ["tweets", "users", "counts", "compliance", "lists"], case_sensitive=False
    ),
)
@click.option(
    "--inline-referenced-tweets/--no-inline-referenced-tweets",
    default=False,
    help="Output referenced tweets inline as separate rows. Default: no.",
)
@click.option(
    "--merge-retweets/--no-merge-retweets",
    default=True,
    help="Merge original tweet metadata into retweets. The Retweet Text, metrics and entities are merged from the original tweet. Default: Yes.",
)
@click.option(
    "--json-encode-all/--no-json-encode-all",
    default=False,
    help="JSON encode / escape all fields. Default: no",
)
@click.option(
    "--json-encode-text/--no-json-encode-text",
    default=False,
    help="Apply JSON encode / escape to text fields. Default: no",
)
@click.option(
    "--json-encode-lists/--no-json-encode-lists",
    default=True,
    help="JSON encode / escape lists. Default: yes",
)
@click.option(
    "--allow-duplicates",
    is_flag=True,
    default=False,
    help="List every tweets as is, including duplicates. Default: No, only unique tweets per row. Retweets are not duplicates.",
)
@click.option(
    "--extra-input-columns",
    default="",
    help="Manually specify extra input columns. Comma separated string. Only modify this if you have processed the json yourself. Default output is all available object columns, no extra input columns.",
)
@click.option(
    "--output-columns",
    default="",
    callback=_validate_output_columns,
    help="Specify what columns to output in the CSV. Default is all input columns.",
)
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="How many lines to process per chunk. Default is 100. Reduce this if output is slow.",
)
@click.option(
    "--hide-stats",
    is_flag=True,
    default=False,
    help="Hide stats about the dataset on completion. Always hidden if you're using stdin / stdout pipes.",
)
@click.option(
    "--hide-progress",
    is_flag=True,
    default=False,
    help="Hide the Progress bar. Always hidden if you're using stdin / stdout pipes.",
)
def csv(
    infile,
    outfile,
    input_data_type,
    json_encode_all,
    json_encode_text,
    json_encode_lists,
    inline_referenced_tweets,
    merge_retweets,
    allow_duplicates,
    extra_input_columns,
    output_columns,
    batch_size,
    hide_stats,
    hide_progress,
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

    converter = DataFrameConverter(
        input_data_type=input_data_type,
        json_encode_all=json_encode_all,
        json_encode_text=json_encode_text,
        json_encode_lists=json_encode_lists,
        inline_referenced_tweets=inline_referenced_tweets,
        merge_retweets=merge_retweets,
        allow_duplicates=allow_duplicates,
        extra_input_columns=extra_input_columns,
        output_columns=output_columns,
    )

    writer = CSVConverter(
        infile=infile,
        outfile=outfile,
        converter=converter,
        output_format="csv",
        batch_size=batch_size,
        hide_progress=hide_progress,
    )
    writer.process()

    if not hide_stats and outfile.name != "<stdout>":

        errors = (
            click.style(
                f"{converter.counts['parse_errors']} failed to parse. See twarc.log for details.\n",
                fg="red",
            )
            if converter.counts["parse_errors"] > 0
            else ""
        )

        dupe_stats = (
            f"{converter.counts['duplicates']} were duplicates. "
            if converter.counts["duplicates"] and not inline_referenced_tweets
            else ""
        )

        referenced_stats = (
            f"{converter.counts['referenced_tweets']} were referenced {input_data_type}:\n"
            f"{converter.counts['retweets']} retweets, {converter.counts['quotes']} quotes, and {converter.counts['replies']} replies.\n"
            f"{converter.counts['duplicates']} were duplicates, and {converter.counts['unavailable']} were referenced but not available in the API responses.\n"
            if inline_referenced_tweets
            else ""
        )

        non_objects = (
            f", and {converter.counts['non_objects']} non {input_data_type} objects"
            if converter.counts["non_objects"]
            else ""
        )

        output_columns = (
            f"{converter.counts['output_columns']} of {converter.counts['input_columns']} input"
            if converter.counts["output_columns"] != converter.counts["input_columns"]
            else converter.counts["output_columns"]
        )

        click.echo(
            f"\nℹ️\n"
            + f"Parsed {converter.counts['tweets']} {input_data_type} objects from {converter.counts['lines']} lines in the input file{non_objects}.\n"
            + dupe_stats
            + referenced_stats
            + errors
            + f"Wrote {converter.counts['rows']} rows and output {output_columns} columns in the CSV.\n",
            err=True,
        )
