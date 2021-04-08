import pandas 
import pathlib
import twarc_csv

from click.testing import CliRunner

runner = CliRunner()
test_data = pathlib.Path('test-data')

def test_noflat():
    input_file = test_data / "noflat.jsonl"
    output_file = test_data / "noflat.csv"
    if output_file.is_file():
        output_file.unlink()
    result = runner.invoke(twarc_csv.csv, [str(input_file), str(output_file)])
    assert output_file.is_file()
    df = pandas.read_csv(output_file)
    assert len(df) > 0
    assert type(df['text']) == pandas.Series

def test_flat():
    input_file = test_data / "flat.jsonl"
    output_file = test_data / "flat.csv"
    if output_file.is_file():
        output_file.unlink()
    runner.invoke(twarc_csv.csv, [str(input_file), str(output_file)])
    assert output_file.is_file()
    df = pandas.read_csv(output_file)
    assert len(df) > 0
    assert type(df['text']) == pandas.Series
