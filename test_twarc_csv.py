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


def test_noflat():
    _process_file("noflat")


def test_flat():
    _process_file("flat")


def test_withheld():
    _process_file("withheld")


def test_expected_tweets():
    _process_file("two_tweets", 2)
