import pandas
import pathlib
import twarc_csv

from click.testing import CliRunner

runner = CliRunner()
test_data = pathlib.Path("test-data")


def _process_file(fname, expected=None):
    input_file = test_data / f"{fname}.jsonl"
    output_file = test_data / f"{fname}.csv"
    if output_file.is_file():
        output_file.unlink()
    result = runner.invoke(twarc_csv.csv, [str(input_file), str(output_file)])
    assert output_file.is_file()
    df = pandas.read_csv(output_file)
    if expected:
        assert len(df) == expected
    else:
        assert len(df) > 0
    assert type(df["text"]) == pandas.Series


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
    _process_file("two_tweets", 2)


def test_withheld():
    _process_file("withheld")


def test_withheld2():
    _process_file("media_policy_violation_on")


def test_withheld3():
    _process_file("media_policy_violation_off")
