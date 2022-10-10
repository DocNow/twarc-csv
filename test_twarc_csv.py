import pandas
import pathlib
import twarc_csv

from click.testing import CliRunner

runner = CliRunner()
test_data = pathlib.Path("test-data")


def _process_file(fname, expected=None, extra=""):
    input_file = test_data / f"{fname}.jsonl"
    output_file = test_data / f"{fname}.csv"
    if output_file.is_file():
        output_file.unlink()

    result = runner.invoke(
        twarc_csv.csv, f"{str(input_file)} {str(output_file)}{extra}"
    )

    assert output_file.is_file()
    df = pandas.read_csv(output_file)
    if expected:
        assert len(df) == expected
    else:
        assert len(df) > 0
    if "counts" not in extra:
        assert type(df["id"]) == pandas.Series
    if output_file.is_file():
        output_file.unlink()


def test_empty():
    input_file = test_data / f"empty.jsonl"
    output_file = test_data / f"empty.csv"
    result = runner.invoke(twarc_csv.csv, [str(input_file), str(output_file)])
    assert not output_file.is_file()


def test_noflat():
    _process_file("noflat")


def test_flat():
    _process_file("flat")


def test_expected_tweets():
    _process_file("two_tweets", 2, extra=" --inline-referenced-tweets")


def test_2sets():
    _process_file("2sets")


def test_brexit():
    _process_file("2sets")


def test_kpop():
    _process_file("kpop")


def test_streaming_output_with_error():
    _process_file("streaming_output_with_error")


def test_withheld():
    _process_file("withheld")


def test_withheld2():
    _process_file("media_policy_violation_on")


def test_withheld3():
    _process_file("media_policy_violation_off")


def test_users():
    _process_file("users", 91, extra=" --input-data-type users")


def test_compliance_users():
    _process_file("users_compliance", 1, extra=" --input-data-type compliance")


def test_compliance_tweets():
    _process_file("tweets_compliance", 2, extra=" --input-data-type compliance")


def test_counts():
    _process_file("counts", 169, extra=" --input-data-type counts")


def test_lists():
    _process_file("lists", 6, extra=" --input-data-type lists")


def test_geo():
    _process_file("geo_tweets", 2, extra=" --input-data-type tweets")


def test_cotweet():
    _process_file("cotweet")


def test_edited():
    _process_file("edited")


def test_edited_after():
    _process_file("edited_after")


def test_edited_before():
    _process_file("edited_before")


def test_quoted_edit():
    _process_file("quoted_edit")
